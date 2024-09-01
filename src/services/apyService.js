const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestAPYData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const fetchLatest = async (chainToFetch) => {
    const cacheKey = `latestAPY:${chainToFetch}:${collateralType}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_core_apr_${chainToFetch}_mainnet`;
      const data = await trx(tableName)
        .where('collateral_type', collateralType)
        .orderBy('ts', 'desc')
        .first();

      if (data) {
        result = {
          ts: data.ts,
          apy_24h: parseFloat(data.apy_24h),
          apy_7d: parseFloat(data.apy_7d),
          apy_28d: parseFloat(data.apy_28d)
        };
        await redisService.set(cacheKey, result, CACHE_TTL);
      }
    }

    return { [chainToFetch]: result ? [result] : [] };
  };

  if (chain) {
    return await fetchLatest(chain);
  } else {
    const results = await Promise.all(CHAINS.map(fetchLatest));
    return Object.assign({}, ...results);
  }
};

const getAllAPYData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const fetchAll = async (chainToFetch) => {
    const cacheKey = `allAPY:${chainToFetch}:${collateralType}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('Fetching all APY data from database');
      const tableName = `prod_${chainToFetch}_mainnet.fct_core_apr_${chainToFetch}_mainnet`;
      const startDate = new Date('2024-05-01');
      
      const data = await trx(tableName)
        .where('ts', '>=', startDate)
        .where('collateral_type', collateralType)
        .select('ts', 'apy_24h', 'apy_7d', 'apy_28d')
        .orderBy('ts', 'asc');

      result = data.map(row => ({
        ts: row.ts,
        apy_24h: parseFloat(row.apy_24h),
        apy_7d: parseFloat(row.apy_7d),
        apy_28d: parseFloat(row.apy_28d)
      }));

      if (result.length > 0) {
        await redisService.set(cacheKey, result, CACHE_TTL);
      }
    }

    return { [chainToFetch]: result };
  };

  if (chain) {
    return await fetchAll(chain);
  } else {
    const results = await Promise.all(CHAINS.map(fetchAll));
    return Object.assign({}, ...results);
  }
};

const getAPYSummaryStats = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cacheKey = `APYSummary:${chainToProcess}:${collateralType}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing APY summary for', chainToProcess);

        try {
          const data = await getAllAPYData(chainToProcess, collateralType, false, trx);
          const allData = data[chainToProcess];

          if (allData.length === 0) {
            console.log('No data found for', chainToProcess);
            return null;
          }

          const smoothedData = smoothData(allData, 'apy_28d');
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

          const apyValues = smoothedData.map(item => parseFloat(item.apy_28d));
          const current = parseFloat(latestData.apy_28d);
          const ath = Math.max(...apyValues);
          const atl = Math.min(...apyValues);

          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.apy_28d) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.apy_28d) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.apy_28d) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.apy_28d) : null),
            ath,
            atl,
            ath_percentage: calculatePercentage(current, ath),
            atl_percentage: calculatePercentage(current, atl),
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
      return Object.fromEntries(CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    console.error('Error in getAPYSummaryStats:', error);
    throw new Error('Error fetching APY summary stats: ' + error.message);
  }
};

const getDailyAggregatedAPYData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDaily = async (chainToFetch) => {
      const cacheKey = `dailyAPY:${chainToFetch}:${collateralType}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache')
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_apr_${chainToFetch}_mainnet`;
        result = await trx.raw(`
          WITH daily_data AS (
            SELECT
              DATE_TRUNC('day', ts) AS date,
              FIRST_VALUE(apy_28d) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts) AS day_start_apy,
              LAST_VALUE(apy_28d) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts
                RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS day_end_apy
            FROM ${tableName}
            WHERE collateral_type = ?
          )
          SELECT DISTINCT
            date as ts,
            CASE 
              WHEN day_start_apy = 0 OR day_end_apy = 0 THEN NULL
              ELSE (day_end_apy - day_start_apy) / day_start_apy
            END as daily_apy_percentage_delta
          FROM daily_data
          ORDER BY date;
        `, [collateralType]);

        result = result.rows.map(row => ({
          ts: row.ts,
          daily_apy_percentage_delta: row.daily_apy_percentage_delta !== null ? parseFloat(row.daily_apy_percentage_delta) : null
        }));

        if (result.length > 0) {
          await redisService.set(cacheKey, result, CACHE_TTL);
        }
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(chain => fetchDaily(chain)));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily aggregated APY data: ' + error.message);
  }
};

const refreshAllAPYData = async (collateralType) => {
  for (const chain of CHAINS) {
    console.log(`Refreshing APY data for chain: ${chain}`);
    
    // Clear existing cache
    await redisService.del(`latestAPY:${chain}:${collateralType}`);
    await redisService.del(`allAPY:${chain}:${collateralType}`);
    await redisService.del(`APYSummary:${chain}:${collateralType}`);
    await redisService.del(`dailyAPY:${chain}:${collateralType}`);

    // Use a single transaction for all database operations
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        await getLatestAPYData(chain, collateralType, true, trx);
        await getAllAPYData(chain, collateralType, true, trx);
        await getAPYSummaryStats(chain, collateralType, true, trx);
        await getDailyAggregatedAPYData(chain, collateralType, true, trx);
      } catch (error) {
        console.error(`Error refreshing APY data for chain ${chain}:`, error);
        throw error; 
      }
    });

    console.log(`Finished refreshing APY data for chain: ${chain}`);
  }
};

module.exports = {
  getLatestAPYData,
  getAllAPYData,
  getAPYSummaryStats,
  getDailyAggregatedAPYData,
  refreshAllAPYData,
};