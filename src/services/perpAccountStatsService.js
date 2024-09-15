const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['perp_account_stats'];

const getCumulativeUniqueTraders = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativeUniqueTraders called with chain: ${chain}, isRefresh: ${isRefresh}`);

  const fetchCumulativeData = async (chainToFetch) => {
    const cacheKey = `cumulativeUniqueTraders:${chainToFetch}`;
    const tsKey = `${cacheKey}:timestamp`;

    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_account_stats_hourly_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new cumulative unique traders data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const queryResult = await trx.raw(`
            WITH trader_data AS (
              SELECT DISTINCT
                account_id,
                ts
              FROM
                ${tableName}
              WHERE
                ts > ?
            ),
            unique_traders AS (
              SELECT DISTINCT
                account_id,
                MIN(ts) OVER (PARTITION BY account_id) AS first_trade_ts
              FROM
                trader_data
            ),
            cumulative_counts AS (
              SELECT
                first_trade_ts AS ts,
                COUNT(*) OVER (ORDER BY first_trade_ts) AS cumulative_trader_count,
                ROW_NUMBER() OVER (PARTITION BY first_trade_ts ORDER BY first_trade_ts) AS rn
              FROM
                unique_traders
            )
            SELECT
              ts,
              cumulative_trader_count
            FROM
              cumulative_counts
            WHERE
              rn = 1
            ORDER BY
              ts;
          `, [startDate]);

          const lastCumulativeCount = result && Array.isArray(result) && result.length > 0
            ? result[result.length - 1].cumulative_trader_count
            : 0;

          const newResult = queryResult.rows.map(row => ({
            ts: new Date(row.ts),
            cumulative_trader_count: parseInt(row.cumulative_trader_count) + lastCumulativeCount,
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

    console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
    return { [chainToFetch]: result || [] };
  };

  try {
    if (chain) {
      return await fetchCumulativeData(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchCumulativeData));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getCumulativeUniqueTraders:', error);
    throw new Error('Error fetching cumulative unique traders data: ' + error.message);
  }
};

const getUniqueTradersSummaryStats = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getUniqueTradersSummaryStats called with chain: ${chain}, isRefresh: ${isRefresh}`);

  try {
    const processChainData = async (chainToProcess) => {
      const summaryCacheKey = `uniqueTradersSummary:${chainToProcess}`;
      const summaryTsKey = `${summaryCacheKey}:timestamp`;
      
      const cumulativeDataKey = `cumulativeUniqueTraders:${chainToProcess}`;
      const cumulativeDataTsKey = `${cumulativeDataKey}:timestamp`;

      let summaryResult = await redisService.get(summaryCacheKey);
      let summaryTimestamp = await redisService.get(summaryTsKey);
      let cumulativeDataTimestamp = await redisService.get(cumulativeDataTsKey);

      console.log(`Summary cache timestamp: ${summaryTimestamp}`);
      console.log(`Cumulative data cache timestamp: ${cumulativeDataTimestamp}`);

      if (isRefresh || !summaryResult || !summaryTimestamp || 
          (cumulativeDataTimestamp && new Date(cumulativeDataTimestamp) > new Date(summaryTimestamp))) {
        console.log('Processing unique traders summary for', chainToProcess);

        const cumulativeData = await getCumulativeUniqueTraders(chainToProcess, false, trx);
        const allData = cumulativeData[chainToProcess];

        if (allData.length === 0) {
          console.log('No data found for', chainToProcess);
          return null;
        }

        const smoothedData = smoothData(allData, 'cumulative_trader_count');
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

        const current = parseFloat(latestData.cumulative_trader_count);
        const traderValues = smoothedData.map(item => parseFloat(item.cumulative_trader_count));

        summaryResult = {
          current,
          delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_trader_count) : null),
          delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_trader_count) : null),
          delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_trader_count) : null),
          delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_trader_count) : null),
          ath: Math.max(...traderValues),
          atl: Math.min(...traderValues),
        };

        summaryResult.ath_percentage = calculatePercentage(current, summaryResult.ath);
        summaryResult.atl_percentage = summaryResult.atl === 0 ? 100 : calculatePercentage(current, summaryResult.atl);

        console.log('Attempting to cache summary data in Redis');
        try {
          await redisService.set(summaryCacheKey, summaryResult, CACHE_TTL);
          await redisService.set(summaryTsKey, cumulativeDataTimestamp || new Date().toISOString(), CACHE_TTL);
          console.log('Summary data successfully cached in Redis');
        } catch (redisError) {
          console.error('Error caching summary data in Redis:', redisError);
        }
      } else {
        console.log('Using cached summary data');
      }

      return summaryResult;
    };
    
    if (chain) {
      const result = await processChainData(chain);
      return result ? { [chain]: result } : {};
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(processChainData));
      return Object.fromEntries(SERVICE_CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    console.error('Error in getUniqueTradersSummaryStats:', error);
    throw new Error(`Error fetching unique traders summary stats: ${error.message}`);
  }
};

const getDailyNewUniqueTraders = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getDailyNewUniqueTraders called with chain: ${chain}, isRefresh: ${isRefresh}`);

  const fetchDailyData = async (chainToFetch) => {
    const cacheKey = `dailyNewUniqueTraders:${chainToFetch}`;
    const tsKey = `${cacheKey}:timestamp`;

    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_account_stats_hourly_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new daily unique traders data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const queryResult = await trx.raw(`
            WITH daily_traders AS (
              SELECT DISTINCT
                DATE_TRUNC('day', ts AT TIME ZONE 'UTC') AS date,
                account_id,
                ts
              FROM
                ${tableName}
              WHERE
                DATE(ts AT TIME ZONE 'UTC') >= DATE(?)
            )
            SELECT
              MAX(ts) AS ts,
              COUNT(DISTINCT account_id) AS daily_unique_traders
            FROM
              daily_traders
            GROUP BY
              date
            ORDER BY
              date;
          `, [startDate]);

          const newResult = queryResult.rows.map(row => ({
            ts: new Date(row.ts),
            daily_unique_traders: parseInt(row.daily_unique_traders),
          }));

          console.log(`Fetched ${newResult.length} new records from database`);

          const isSameUTCDay = (date1, date2) => {
            const d1 = new Date(date1);
            const d2 = new Date(date2);
            return d1.getUTCFullYear() === d2.getUTCFullYear() &&
                   d1.getUTCMonth() === d2.getUTCMonth() &&
                   d1.getUTCDate() === d2.getUTCDate();
          };

          if (result && Array.isArray(result)) {
            console.log('Merging existing result with new data');
            const mergedResult = [...result];

            newResult.forEach(newRow => {
              const existingIndex = mergedResult.findIndex(r => isSameUTCDay(r.ts, newRow.ts));

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

    console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
    return { [chainToFetch]: result || [] };
  };

  try {
    if (chain) {
      return await fetchDailyData(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchDailyData));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getDailyNewUniqueTraders:', error);
    throw new Error('Error fetching daily new unique traders data: ' + error.message);
  }
};

const refreshAllPerpAccountStatsData = async () => {
  console.log('Starting to refresh Perp Account Stats data for all chains');
  
  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing Perp Account Stats data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getCumulativeUniqueTraders`);
        await getCumulativeUniqueTraders(chain, true, trx);
        console.timeEnd(`${chain} getCumulativeUniqueTraders`);

        console.time(`${chain} getUniqueTradersSummaryStats`);
        await getUniqueTradersSummaryStats(chain, true, trx);
        console.timeEnd(`${chain} getUniqueTradersSummaryStats`);

        console.time(`${chain} getDailyNewUniqueTraders`);
        await getDailyNewUniqueTraders(chain, true, trx);
        console.timeEnd(`${chain} getDailyNewUniqueTraders`);

      } catch (error) {
        console.error(`Error refreshing Perp Account Stats data for chain ${chain}:`, error);
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing Perp Account Stats data for chain: ${chain}`);
  }

  console.log('Finished refreshing Perp Account Stats data for all chains');
};

module.exports = {
  getCumulativeUniqueTraders,
  getUniqueTradersSummaryStats,
  getDailyNewUniqueTraders,
  refreshAllPerpAccountStatsData
};