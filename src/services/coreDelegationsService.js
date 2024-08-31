const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestCoreDelegationsData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchLatest = async (chainToFetch) => {
      const cacheKey = `latestCoreDelegations:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_pool_delegation_${chainToFetch}_mainnet`;
        try {
          result = await troyDBKnex(tableName)
            .where('collateral_type', collateralType)
            .orderBy('ts', 'desc')
            .limit(1);
        } catch (error) {
          console.error(`Error fetching data for ${chainToFetch}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchLatest(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchLatest));
      return CHAINS.reduce((acc, chain) => {
        acc[chain] = results.find(r => r[chain])?.[chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching latest core delegations data: ' + error.message);
  }
};

const getCoreDelegationsData = async (chain, collateralType) => {
  const cacheKey = `coreDelegationsData:${chain}:${collateralType}`;
  let result = await redisService.get(cacheKey);

  if (!result) {
    console.log('Fetching core delegations data from database');
    const tableName = `prod_${chain}_mainnet.fct_core_pool_delegation_${chain}_mainnet`;
    try {
      result = await troyDBKnex(tableName)
        .where('collateral_type', collateralType)
        .orderBy('ts', 'asc')
        .timeout(30000); // 30 seconds timeout
    } catch (error) {
      console.error(`Error fetching data for ${chain}:`, error);
      result = [];
    }
    await redisService.set(cacheKey, result, CACHE_TTL);
  }

  return result;
};

const getCumulativeCoreDelegationsData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchCumulative = async (chainToFetch) => {
      const cacheKey = `cumulativeCoreDelegations:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_pool_delegation_${chainToFetch}_mainnet`;
        try {
          result = await troyDBKnex(tableName)
            .where('collateral_type', collateralType)
            .orderBy('ts', 'asc');
        } catch (error) {
          console.error(`Error fetching data for ${chainToFetch}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchCumulative(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCumulative));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative core delegations data: ' + error.message);
  }
};

const getCoreDelegationsSummaryStats = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cacheKey = `coreDelegationsSummary:${chainToProcess}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing core delegations summary');
        const allData = await getCoreDelegationsData(chainToProcess, collateralType);

        if (allData.length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(allData, 'amount_delegated');
          const reversedSmoothedData = [...smoothedData].reverse();
          const latestData = reversedSmoothedData[0];
          const latestTs = new Date(latestData.ts);

          const getDateFromLatest = (days) => new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);

          const value24h = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(1));
          const value7d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(7));
          const value28d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(28));
          let valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1));

          if (!valueYtd) {
            valueYtd = reversedSmoothedData[reversedSmoothedData.length - 1];
          }

          const delegationsValues = smoothedData.map(item => parseFloat(item.amount_delegated));
          const current = parseFloat(allData[allData.length - 1].amount_delegated);
          const ath = Math.max(...delegationsValues, current);
          const atl = Math.min(...delegationsValues, current);

          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.amount_delegated) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.amount_delegated) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.amount_delegated) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.amount_delegated) : null),
            ath,
            atl,
            ath_percentage: calculatePercentage(current, ath),
            atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
          };
        }

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return result;
    };

    if (chain) {
      const result = await processChainData(chain);
      return { [chain]: result };
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching Core Delegations summary stats: ' + error.message);
  }
};

const getDailyCoreDelegationsData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDaily = async (chainToFetch) => {
      const cacheKey = `dailyCoreDelegations:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_pool_delegation_${chainToFetch}_mainnet`;
        try {
          const queryResult = await troyDBKnex.raw(`
            WITH daily_data AS (
              SELECT
                DATE_TRUNC('day', ts) AS date,
                FIRST_VALUE(SUM(amount_delegated)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_delegations,
                LAST_VALUE(SUM(amount_delegated)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_delegations
              FROM ${tableName}
              WHERE collateral_type = ?
              GROUP BY DATE_TRUNC('day', ts), ts
            )
            SELECT DISTINCT
              date,
              end_of_day_delegations - start_of_day_delegations AS daily_delegations_change
            FROM daily_data
            ORDER BY date;
          `, [collateralType]);

          result = queryResult.rows.map(row => ({
            ts: row.date,
            daily_delegations_change: parseFloat(row.daily_delegations_change)
          }));
        } catch (error) {
          console.error(`Error fetching data for ${chainToFetch}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, result) => ({ ...acc, ...result }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily core delegations data: ' + error.message);
  }
};

module.exports = {
  getLatestCoreDelegationsData,
  getCumulativeCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
};