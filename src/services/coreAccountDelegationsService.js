const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getStakerCount = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchCount = async (chainToFetch) => {
      const cacheKey = `stakerCount:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_account_delegation_${chainToFetch}_mainnet`;
        try {
          const queryResult = await troyDBKnex(tableName)
            .where('collateral_type', collateralType)
            .countDistinct('account_id as staker_count')
            .first();
          result = parseInt(queryResult.staker_count);
        } catch (error) {
          console.error(`Error fetching data for ${chainToFetch}:`, error);
          result = 0;
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchCount(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCount));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching staker count: ' + error.message);
  }
};

const getCumulativeUniqueStakers = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchCumulativeData = async (chainToFetch) => {
      const cacheKey = `cumulativeUniqueStakers:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_account_delegation_${chainToFetch}_mainnet`;
        try {
          const queryResult = await troyDBKnex.raw(`
            WITH daily_new_stakers AS (
              SELECT
                date_trunc('day', ts) AS day,
                pool_id,
                collateral_type,
                account_id,
                MIN(date_trunc('day', ts)) OVER (PARTITION BY account_id, pool_id, collateral_type) AS first_staked_day
              FROM
                ${tableName}
              WHERE
                collateral_type = ?
            ),
            daily_cumulative_counts AS (
              SELECT
                day,
                pool_id,
                collateral_type,
                COUNT(DISTINCT CASE WHEN day = first_staked_day THEN account_id END) AS new_stakers,
                SUM(COUNT(DISTINCT CASE WHEN day = first_staked_day THEN account_id END)) 
                  OVER (PARTITION BY pool_id, collateral_type ORDER BY day) AS cumulative_staker_count
              FROM
                daily_new_stakers
              GROUP BY
                day, pool_id, collateral_type
            )
            SELECT
              day AS ts,
              pool_id,
              collateral_type,
              cumulative_staker_count
            FROM
              daily_cumulative_counts
            ORDER BY
              ts, pool_id, collateral_type;
          `, [collateralType]);

          result = queryResult.rows.map(row => ({
            ts: row.ts,
            cumulative_staker_count: parseInt(row.cumulative_staker_count),
            pool_id: row.pool_id,
            collateral_type: row.collateral_type,
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
      return await fetchCumulativeData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCumulativeData));
      return CHAINS.reduce((acc, chain) => {
        acc[chain] = results.find(r => r[chain])?.[chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative unique staker data: ' + error.message);
  }
};

const getUniqueStakersSummaryStats = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cacheKey = `uniqueStakersSummary:${chainToProcess}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const cumulativeData = await getCumulativeUniqueStakers(chainToProcess, collateralType);
        const allData = cumulativeData[chainToProcess] || [];
        if (allData.length === 0) {
          return null;
        }

        const smoothedData = smoothData(allData, 'cumulative_staker_count');
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

        const stakerValues = smoothedData.map(item => parseFloat(item.cumulative_staker_count));

        const current = parseFloat(allData[allData.length - 1].cumulative_staker_count);
        const ath = Math.max(...stakerValues, current);
        const atl = Math.min(...stakerValues, current);

        result = {
          current,
          delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_staker_count) : null),
          delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_staker_count) : null),
          delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_staker_count) : null),
          delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_staker_count) : null),
          ath,
          atl,
          ath_percentage: calculatePercentage(current, ath),
          atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
        };

        await redisService.set(cacheKey, result, CACHE_TTL);
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
    throw new Error('Error fetching Unique Stakers summary stats: ' + error.message);
  }
};

const getDailyNewUniqueStakers = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDailyData = async (chainToFetch) => {
      const cacheKey = `dailyNewUniqueStakers:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_account_delegation_${chainToFetch}_mainnet`;
        try {
          result = await troyDBKnex.raw(`
            WITH first_staking_day AS (
              SELECT
                account_id,
                MIN(DATE_TRUNC('day', ts)) AS first_day
              FROM
                ${tableName}
              WHERE
                collateral_type = ?
              GROUP BY
                account_id
            )
            SELECT
              first_day AS date,
              COUNT(*) AS daily_new_unique_stakers
            FROM
              first_staking_day
            GROUP BY
              first_day
            ORDER BY
              first_day;
          `, [collateralType]);

          result = result.rows.map(row => ({
            ts: row.date,
            daily_new_unique_stakers: parseInt(row.daily_new_unique_stakers),
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
      return await fetchDailyData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(chainToFetch => fetchDailyData(chainToFetch)));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily new unique stakers: ' + error.message);
  }
};

module.exports = {
  getStakerCount,
  getCumulativeUniqueStakers,
  getUniqueStakersSummaryStats,
  getDailyNewUniqueStakers,
};