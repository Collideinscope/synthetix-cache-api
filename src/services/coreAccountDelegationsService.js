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
            WITH daily_stakers AS (
              SELECT DISTINCT
                DATE_TRUNC('day', ts) AS day,
                account_id,
                pool_id,
                collateral_type
              FROM
                ${tableName}
              WHERE
                collateral_type = ?
            ),
            daily_counts AS (
              SELECT
                day,
                pool_id,
                collateral_type,
                COUNT(DISTINCT account_id) AS daily_unique_stakers
              FROM
                daily_stakers
              GROUP BY
                day, pool_id, collateral_type
            ),
            cumulative_counts AS (
              SELECT
                day,
                pool_id,
                collateral_type,
                SUM(daily_unique_stakers) OVER (PARTITION BY pool_id, collateral_type ORDER BY day) AS cumulative_staker_count
              FROM
                daily_counts
            )
            SELECT
              day AS ts,
              pool_id,
              collateral_type,
              cumulative_staker_count
            FROM
              cumulative_counts
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
        console.log('Processing unique stakers summary');
        const data = await getCumulativeUniqueStakers(chainToProcess, collateralType);
        
        if (data[chainToProcess].length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(data[chainToProcess], 'cumulative_staker_count');
          const latestData = smoothedData[smoothedData.length - 1];
          const latestTs = new Date(latestData.ts);
          
          const findValueAtDate = (days) => {
            const targetDate = new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);
            return smoothedData.findLast(item => new Date(item.ts) <= targetDate);
          };
          
          const value24h = findValueAtDate(1);
          const value7d = findValueAtDate(7);
          const value28d = findValueAtDate(28);
          const valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1)) || smoothedData[0];
          
          const current = parseFloat(latestData.cumulative_staker_count);
          const stakerValues = smoothedData.map(item => parseFloat(item.cumulative_staker_count));
          
          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_staker_count) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_staker_count) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_staker_count) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_staker_count) : null),
            ath: Math.max(...stakerValues),
            atl: Math.min(...stakerValues),
          };
          
          result.ath_percentage = calculatePercentage(current, result.ath);
          result.atl_percentage = result.atl === 0 ? 100 : calculatePercentage(current, result.atl);
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
      return Object.fromEntries(CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    throw new Error(`Error fetching unique stakers summary stats: ${error.message}`);
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
            WITH daily_stakers AS (
              SELECT DISTINCT
                DATE_TRUNC('day', ts) AS date,
                account_id
              FROM
                ${tableName}
              WHERE
                collateral_type = ?
            )
            SELECT
              date,
              COUNT(DISTINCT account_id) AS daily_unique_stakers
            FROM
              daily_stakers
            GROUP BY
              date
            ORDER BY
              date;
          `, [collateralType]);

          result = result.rows.map(row => ({
            ts: row.date,
            daily_unique_stakers: parseInt(row.daily_unique_stakers),
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
    throw new Error('Error fetching daily unique stakers: ' + error.message);
  }
};

module.exports = {
  getStakerCount,
  getCumulativeUniqueStakers,
  getUniqueStakersSummaryStats,
  getDailyNewUniqueStakers,
};