const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestPerpStatsData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  const fetchLatest = async (chainToFetch) => {
    const cacheKey = `latestPerpStats:${chainToFetch}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_stats_daily_${chainToFetch}_mainnet`;
      try {
        result = await trx(tableName)
          .orderBy('ts', 'desc')
          .limit(1);
        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching latest perp stats data for ${chainToFetch}:`, error.message);
        result = [];
      }
    }

    return { [chainToFetch]: result };
  };

  try {
    if (chain) {
      return await fetchLatest(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchLatest));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index][chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    console.error('Error in getLatestPerpStatsData:', error);
    return {};
  }
};

const getSummaryStats = async (chain, column, bypassCache = false, trx = troyDBKnex) => {
  const processChainData = async (chainToProcess) => {
    const cacheKey = `perpStatsSummary:${chainToProcess}:${column}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);
    
    if (!result) {
      console.log('Processing perp stats summary');
      const data = await fetchCumulativeData(chainToProcess, column);
      
      if (data.length === 0) {
        result = {};
      } else {
        const smoothedData = smoothData(data, column);
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
        
        const current = parseFloat(latestData[column]);
        const columnValues = smoothedData.map(item => parseFloat(item[column]));
        
        result = {
          current,
          delta_24h: calculateDelta(current, value24h ? parseFloat(value24h[column]) : null),
          delta_7d: calculateDelta(current, value7d ? parseFloat(value7d[column]) : null),
          delta_28d: calculateDelta(current, value28d ? parseFloat(value28d[column]) : null),
          delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd[column]) : null),
          ath: Math.max(...columnValues),
          atl: Math.min(...columnValues),
        };
        
        result.ath_percentage = calculatePercentage(current, result.ath);
        result.atl_percentage = result.atl === 0 ? 100 : calculatePercentage(current, result.atl);
      }
      
      await redisService.set(cacheKey, result, CACHE_TTL);
    }
    
    return result;
  };
  
  try {
    if (chain) {
      const result = await processChainData(chain);
      return { [chain]: result };
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return Object.fromEntries(CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    console.error(`Error in getSummaryStats for ${column}:`, error);
    return {};
  }
};

const getCumulativeVolumeSummaryStats = async (chain, bypassCache = false, trx = troyDBKnex) => {
  return getSummaryStats(chain, 'cumulative_volume', bypassCache, trx);
};

const getCumulativeExchangeFeesSummaryStats = async (chain, bypassCache = false, trx = troyDBKnex) => {
  return getSummaryStats(chain, 'cumulative_exchange_fees', bypassCache, trx);
};

const fetchCumulativeData = async (chain, dataType, bypassCache = false, trx = troyDBKnex) => {
  const cacheKey = `cumulativePerpStats:${chain}:${dataType}`;
  let result = bypassCache ? null : await redisService.get(cacheKey);

  if (!result) {
    console.log('not from cache');
    const tableName = `prod_${chain}_mainnet.fct_perp_stats_daily_${chain}_mainnet`;
    try {
      result = await trx.raw(`
        SELECT 
          ts,
          ${dataType}
        FROM 
          ${tableName}
        ORDER BY 
          ts;
      `);

      result = result.rows.map(row => ({
        ts: row.ts,
        [dataType]: parseFloat(row[dataType]),
      }));

      await redisService.set(cacheKey, result, CACHE_TTL);
    } catch (error) {
      console.error(`Error fetching cumulative data for ${chain}:`, error.message);
      result = [];
    }
  }

  return result;
};

const getCumulativeVolumeData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (chain) {
      const data = await fetchCumulativeData(chain, 'cumulative_volume', bypassCache, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_volume', bypassCache, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getCumulativeVolumeData:', error);
    return {};
  }
};

const getCumulativeExchangeFeesData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (chain) {
      const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees', bypassCache, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees', bypassCache, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getCumulativeExchangeFeesData:', error);
    return {};
  }
};

const fetchDailyData = async (chain, dataType, bypassCache = false, trx = troyDBKnex) => {
  const cacheKey = `dailyPerpStats:${chain}:${dataType}`;
  let result = bypassCache ? null : await redisService.get(cacheKey);

  if (!result) {
    console.log('not from cache');
    const tableName = `prod_${chain}_mainnet.fct_perp_stats_daily_${chain}_mainnet`;
    try {
      result = await trx.raw(`
        WITH daily_data AS (
          SELECT 
            ts AS date,
            ${dataType},
            LAG(${dataType}) OVER (ORDER BY ts) AS prev_${dataType}
          FROM 
            ${tableName}
          ORDER BY 
            ts
        )
        SELECT 
          date,
          COALESCE(${dataType} - prev_${dataType}, ${dataType}) AS daily_${dataType}
        FROM 
          daily_data
        WHERE
          prev_${dataType} IS NOT NULL OR date = (SELECT MIN(date) FROM daily_data)
        ORDER BY 
          date;
      `);

      result = result.rows.map(row => ({
        ts: row.date,
        [`daily_${dataType}`]: parseFloat(row[`daily_${dataType}`]),
      }));

      await redisService.set(cacheKey, result, CACHE_TTL);
    } catch (error) {
      console.error(`Error fetching daily data for ${chain}:`, error.message);
      result = [];
    }
  }

  return result;
};

const getDailyVolumeData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (chain) {
      const data = await fetchDailyData(chain, 'cumulative_volume', bypassCache, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'cumulative_volume', bypassCache, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getDailyVolumeData:', error);
    return {};
  }
};

const getDailyExchangeFeesData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (chain) {
      const data = await fetchDailyData(chain, 'cumulative_exchange_fees', bypassCache, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'cumulative_exchange_fees', bypassCache, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getDailyExchangeFeesData:', error);
    return {};
  }
};

const refreshAllPerpStatsData = async () => {
  console.log('Starting to refresh Perp Stats data for all chains');
  
  for (const chain of CHAINS) {
    console.log(`Refreshing Perp Stats data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);
    
    // Clear existing cache
    await redisService.del(`latestPerpStats:${chain}`);
    await redisService.del(`perpStatsSummary:${chain}:cumulative_volume`);
    await redisService.del(`perpStatsSummary:${chain}:cumulative_exchange_fees`);
    await redisService.del(`cumulativePerpStats:${chain}:cumulative_volume`);
    await redisService.del(`cumulativePerpStats:${chain}:cumulative_exchange_fees`);
    await redisService.del(`dailyPerpStats:${chain}:cumulative_volume`);
    await redisService.del(`dailyPerpStats:${chain}:cumulative_exchange_fees`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getLatestPerpStatsData`);
        await getLatestPerpStatsData(chain, true, trx);
        console.timeEnd(`${chain} getLatestPerpStatsData`);

        console.time(`${chain} getCumulativeVolumeSummaryStats`);
        await getCumulativeVolumeSummaryStats(chain, true, trx);
        console.timeEnd(`${chain} getCumulativeVolumeSummaryStats`);

        console.time(`${chain} getCumulativeExchangeFeesSummaryStats`);
        await getCumulativeExchangeFeesSummaryStats(chain, true, trx);
        console.timeEnd(`${chain} getCumulativeExchangeFeesSummaryStats`);

        console.time(`${chain} getCumulativeVolumeData`);
        await getCumulativeVolumeData(chain, true, trx);
        console.timeEnd(`${chain} getCumulativeVolumeData`);

        console.time(`${chain} getCumulativeExchangeFeesData`);
        await getCumulativeExchangeFeesData(chain, true, trx);
        console.timeEnd(`${chain} getCumulativeExchangeFeesData`);

        console.time(`${chain} getDailyVolumeData`);
        await getDailyVolumeData(chain, true, trx);
        console.timeEnd(`${chain} getDailyVolumeData`);

        console.time(`${chain} getDailyExchangeFeesData`);
        await getDailyExchangeFeesData(chain, true, trx);
        console.timeEnd(`${chain} getDailyExchangeFeesData`);

      } catch (error) {
        console.error(`Error refreshing Perp Stats data for chain ${chain}:`, error);
        // Don't throw the error, just log it and continue with the next chain
      }
    });

    console.timeEnd(`${chain} total refresh time`);
  }

  console.log('Finished refreshing Perp Stats data for all chains');
};

module.exports = {
  getLatestPerpStatsData,
  getCumulativeVolumeSummaryStats,
  getCumulativeExchangeFeesSummaryStats,
  getCumulativeVolumeData,
  getCumulativeExchangeFeesData,
  getDailyVolumeData,
  getDailyExchangeFeesData,
  refreshAllPerpStatsData
};