const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['perp_stats'];

const getSummaryStats = async (chain, column, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getSummaryStats called with chain: ${chain}, column: ${column}, isRefresh: ${isRefresh}`);

  const processChainData = async (chainToProcess) => {
    const summaryCacheKey = `perpStatsSummary:${chainToProcess}:${column}`;
    const summaryTsKey = `${summaryCacheKey}:timestamp`;
    
    const cumulativeDataKey = `cumulativePerpStats:${chainToProcess}:${column}`;
    const cumulativeDataTsKey = `${cumulativeDataKey}:timestamp`;

    let summaryResult = await redisService.get(summaryCacheKey);
    let summaryTimestamp = await redisService.get(summaryTsKey);
    let cumulativeDataTimestamp = await redisService.get(cumulativeDataTsKey);

    console.log(`Summary cache timestamp: ${summaryTimestamp}`);
    console.log(`Cumulative data cache timestamp: ${cumulativeDataTimestamp}`);

    if (isRefresh || !summaryResult || !summaryTimestamp || 
        (cumulativeDataTimestamp && new Date(cumulativeDataTimestamp) > new Date(summaryTimestamp))) {
      console.log('Processing perp stats summary for', chainToProcess);

      const data = await fetchCumulativeData(chainToProcess, column, false, trx);
      
      if (data.length === 0) {
        console.log('No data found for', chainToProcess);
        return null;
      }

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
      
      summaryResult = {
        current,
        delta_24h: calculateDelta(current, value24h ? parseFloat(value24h[column]) : null),
        delta_7d: calculateDelta(current, value7d ? parseFloat(value7d[column]) : null),
        delta_28d: calculateDelta(current, value28d ? parseFloat(value28d[column]) : null),
        delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd[column]) : null),
        ath: Math.max(...columnValues),
        atl: Math.min(...columnValues),
      };
      
      summaryResult.ath_percentage = calculatePercentage(current, summaryResult.ath);
      summaryResult.atl_percentage = summaryResult.atl === 0 ? 100 : calculatePercentage(current, summaryResult.atl);

      console.log('Attempting to cache summary stats in Redis');
      try {
        await redisService.set(summaryCacheKey, summaryResult, CACHE_TTL);
        await redisService.set(summaryTsKey, cumulativeDataTimestamp || new Date().toISOString(), CACHE_TTL);
        console.log('Summary stats successfully cached in Redis');
      } catch (redisError) {
        console.error('Error caching summary stats in Redis:', redisError);
      }
    } else {
      console.log('Using cached summary stats');
    }
    
    return summaryResult;
  };
  
  try {
    if (chain) {
      const result = await processChainData(chain);
      return result ? { [chain]: result } : {};
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(processChainData));
      return Object.fromEntries(SERVICE_CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    console.error(`Error in getSummaryStats for ${column}:`, error);
    throw new Error(`Error fetching summary stats for ${column}: ${error.message}`);
  }
};

const getCumulativeVolumeSummaryStats = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativeVolumeSummaryStats called with chain: ${chain}, isRefresh: ${isRefresh}`);
  return getSummaryStats(chain, 'cumulative_volume', isRefresh, trx);
};

const getCumulativeExchangeFeesSummaryStats = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativeExchangeFeesSummaryStats called with chain: ${chain}, isRefresh: ${isRefresh}`);
  return getSummaryStats(chain, 'cumulative_exchange_fees', isRefresh, trx);
};

const fetchCumulativeData = async (chain, dataType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`fetchCumulativeData called with chain: ${chain}, dataType: ${dataType}, isRefresh: ${isRefresh}`);

  const cacheKey = `cumulativePerpStats:${chain}:${dataType}`;
  const tsKey = `${cacheKey}:timestamp`;
  
  console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
  let result = await redisService.get(cacheKey);
  let cachedTimestamp = await redisService.get(tsKey);

  console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

  if (isRefresh || !result) {
    const tableName = `prod_${chain}_mainnet.fct_perp_stats_daily_${chain}_mainnet`;
    console.log(`Querying database table: ${tableName}`);

    try {
      const latestDbTimestamp = await trx(tableName)
        .max('ts as latest_ts')
        .first();

      console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

      if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
        console.log('Fetching new cumulative data from database');
        const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
        console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

        const queryResult = await trx.raw(`
          SELECT 
            ts,
            ${dataType}
          FROM 
            ${tableName}
          WHERE ts > ?
          ORDER BY 
            ts;
        `, [startDate]);

        const newResult = queryResult.rows.map(row => ({
          ts: row.ts,
          [dataType]: parseFloat(row[dataType]),
        }));

        console.log(`Fetched ${newResult.length} new records from database`);

        if (result && Array.isArray(result)) {
          console.log('Merging existing result with new data');
          const mergedResult = [...result];
          newResult.forEach(newRow => {
            const existingIndex = mergedResult.findIndex(r => {
              return new Date(r.ts).getTime() === newRow.ts.getTime();
            });            
            if (existingIndex !== -1) {
              mergedResult[existingIndex] = newRow;
            } else {
              mergedResult.push(newRow);
            }
          });
          result = mergedResult.sort((a, b) => a.ts - b.ts);
        } else {
          console.log('Setting result to new data');
          result = newResult;
        }

        if (result.length > 0) {
          console.log(`Attempting to cache ${result.length} records in Redis`);
          try {
            await redisService.set(cacheKey, result, CACHE_TTL);
            await redisService.set(tsKey, latestDbTimestamp.latest_ts, CACHE_TTL);
            console.log('Data successfully cached in Redis');
          } catch (redisError) {
            console.error('Error caching data in Redis:', redisError);
          }
        } else {
          console.log('No data to cache in Redis');
        }
      } else {
        console.log('Using cached data, no need to fetch from database');
      }
    } catch (dbError) {
      console.error('Error querying database:', dbError);
      result = [];
    }
  } else {
    console.log('Not refreshing, using cached result');
  }

  console.log(`Returning result: ${result ? result.length + ' records' : 'No data'}`);
  return result || [];
};

const getCumulativeVolumeData = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativeVolumeData called with chain: ${chain}, isRefresh: ${isRefresh}`);
  try {
    if (chain) {
      const data = await fetchCumulativeData(chain, 'cumulative_volume', isRefresh, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_volume', isRefresh, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getCumulativeVolumeData:', error);
    throw new Error('Error fetching cumulative volume data: ' + error.message);
  }
};

const getCumulativeExchangeFeesData = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativeExchangeFeesData called with chain: ${chain}, isRefresh: ${isRefresh}`);
  try {
    if (chain) {
      const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees', isRefresh, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees', isRefresh, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getCumulativeExchangeFeesData:', error);
    throw new Error('Error fetching cumulative exchange fees data: ' + error.message);
  }
};

const fetchDailyData = async (chain, dataType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`fetchDailyData called with chain: ${chain}, dataType: ${dataType}, isRefresh: ${isRefresh}`);

  const cacheKey = `dailyPerpStats:${chain}:${dataType}`;
  const tsKey = `${cacheKey}:timestamp`;
  
  console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
  let result = await redisService.get(cacheKey);
  let cachedTimestamp = await redisService.get(tsKey);

  console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

  if (isRefresh || !result) {
    const tableName = `prod_${chain}_mainnet.fct_perp_stats_daily_${chain}_mainnet`;
    console.log(`Querying database table: ${tableName}`);

    try {
      const latestDbTimestamp = await trx(tableName)
        .max('ts as latest_ts')
        .first();

      console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

      if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
        console.log('Fetching new daily data from database');
        const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
        console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);
        
        const queryResult = await trx.raw(`
          SELECT
            ts,
            ${dataType}
          FROM
            ${tableName}
          WHERE DATE(ts) >= DATE(?)
          ORDER BY
            ts
        `, [startDate]);

        const newResult = queryResult.rows.map(row => ({
          ts: row.ts,
          [`daily_${dataType}`]: parseFloat(row[dataType]),
        }));

        console.log(`Fetched ${newResult.length} new records from database`);

        if (result) {
          console.log('Merging existing result with new data');
          const mergedResult = [...result];
          newResult.forEach(newRow => {
            const existingIndex = mergedResult.findIndex(r => {
              return r.ts === newRow.ts
          });
            if (existingIndex !== -1) {
              mergedResult[existingIndex] = newRow;
            } else {
              mergedResult.push(newRow);
            }
          });
          result = mergedResult.sort((a, b) => new Date(a.ts) - new Date(b.ts));
        } else {
          console.log('Setting result to new data');
          result = newResult;
        }

        if (result.length > 0) {
          console.log(`Attempting to cache ${result.length} records in Redis`);
          try {
            await redisService.set(cacheKey, result, CACHE_TTL);
            await redisService.set(tsKey, latestDbTimestamp.latest_ts, CACHE_TTL);
            console.log('Data successfully cached in Redis');
          } catch (redisError) {
            console.error('Error caching data in Redis:', redisError);
          }
        } else {
          console.log('No data to cache in Redis');
        }
      } else {
        console.log('Using cached data, no need to fetch from database');
      }
    } catch (dbError) {
      console.error('Error querying database:', dbError);
      result = [];
    }
  } else {
    console.log('Not refreshing, using cached result');
  }

  console.log(`Returning result: ${result ? result.length + ' records' : 'No data'}`);
  return result || [];
};

const getDailyVolumeData = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getDailyVolumeData called with chain: ${chain}, isRefresh: ${isRefresh}`);
  try {
    if (chain) {
      const data = await fetchDailyData(chain, 'volume', isRefresh, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'volume', isRefresh, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getDailyVolumeData:', error);
    throw new Error('Error fetching daily volume data: ' + error.message);
  }
};

const getDailyExchangeFeesData = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getDailyExchangeFeesData called with chain: ${chain}, isRefresh: ${isRefresh}`);
  try {
    if (chain) {
      const data = await fetchDailyData(chain, 'exchange_fees', isRefresh, trx);
      return { [chain]: data };
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'exchange_fees', isRefresh, trx);
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    console.error('Error in getDailyExchangeFeesData:', error);
    throw new Error('Error fetching daily exchange fees data: ' + error.message);
  }
};

const refreshAllPerpStatsData = async () => {
  console.log('Starting to refresh Perp Stats data for all chains');
  
  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing Perp Stats data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
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
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing Perp Stats data for chain: ${chain}`);
  }

  console.log('Finished refreshing Perp Stats data for all chains');
};

module.exports = {
  getCumulativeVolumeSummaryStats,
  getCumulativeExchangeFeesSummaryStats,
  getCumulativeVolumeData,
  getCumulativeExchangeFeesData,
  getDailyVolumeData,
  getDailyExchangeFeesData,
  refreshAllPerpStatsData
};