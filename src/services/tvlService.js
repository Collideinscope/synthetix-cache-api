const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestTVLData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchLatest = async (chainToFetch) => {
      const cacheKey = `latestTVL:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_vault_collateral_${chainToFetch}_mainnet`;
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
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching latest TVL data: ' + error.message);
  }
};

const getCumulativeTVLData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchAll = async (chainToFetch) => {
      const cacheKey = `cumulativeTVL:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_vault_collateral_${chainToFetch}_mainnet`;
        try {
          result = await troyDBKnex(tableName)
            .where({
              pool_id: 1,
              collateral_type: collateralType
            })
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
      return await fetchAll(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchAll));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative TVL data: ' + error.message);
  }
};

const getTVLSummaryStats = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cacheKey = `TVLSummary:${chainToProcess}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToProcess}_mainnet.fct_core_vault_collateral_${chainToProcess}_mainnet`;
        let allData = [];
        try {
          allData = await troyDBKnex(tableName)
            .where({
              pool_id: 1,
              collateral_type: collateralType
            })
            .orderBy('ts', 'asc');
        } catch (error) {
          console.error(`Error fetching data for ${chainToProcess}:`, error);
          return null;  
        }

        if (allData.length === 0) {
          return null;
        }

        try {
          const smoothedData = smoothData(allData, 'collateral_value');
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

          const tvlValues = smoothedData.map(item => parseFloat(item.collateral_value));

          const current = parseFloat(allData[allData.length - 1].collateral_value);
          const ath = Math.max(...tvlValues, current);
          const atl = Math.min(...tvlValues, current);

          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.collateral_value) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.collateral_value) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.collateral_value) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.collateral_value) : null),
            ath,
            atl,
            ath_percentage: calculatePercentage(current, ath),
            atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
          };

          await redisService.set(cacheKey, result, CACHE_TTL);
        } catch (error) {
          console.error(`Error processing data for ${chainToProcess}:`, error);
          return null;
        }
      }

      return result;
    };

    if (chain) {
      const result = await processChainData(chain);
      return result ? { [chain]: result } : {};
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index] || {};
        return acc;
      }, {});
    }
  } catch (error) {
    console.error('Error in getTVLSummaryStats:', error);
    throw new Error('Error fetching TVL summary stats: ' + error.message);
  }
};

const getDailyTVLData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDaily = async (chainToProcess) => {
      const cacheKey = `dailyTVL:${chainToProcess}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToProcess}_mainnet.fct_core_vault_collateral_${chainToProcess}_mainnet`;
        try {
          const queryResult = await troyDBKnex.raw(`
            WITH daily_data AS (
              SELECT
                DATE_TRUNC('day', ts) AS date,
                FIRST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_tvl,
                LAST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_tvl
              FROM ${tableName}
              WHERE pool_id = 1
                AND collateral_type = ?
              GROUP BY DATE_TRUNC('day', ts), ts
            )
            SELECT DISTINCT
              date,
              end_of_day_tvl - start_of_day_tvl AS daily_tvl_change
            FROM daily_data
            ORDER BY date;
          `, [collateralType]);

          result = queryResult.rows.map(row => ({
            ts: row.date,
            daily_tvl_change: parseFloat(row.daily_tvl_change)
          }));
        } catch (error) {
          console.error(`Error fetching data for ${chainToProcess}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToProcess]: result };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily TVL data: ' + error.message);
  }
};

module.exports = {
  getLatestTVLData,
  getCumulativeTVLData,
  getTVLSummaryStats,
  getDailyTVLData,
};