const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['perp_market_history'];

const getOpenInterestData = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getOpenInterestData called with chain: ${chain}, isRefresh: ${isRefresh}`);

  const fetchDataForChain = async (chainToFetch) => {
    const cacheKey = `openInterestData:${chainToFetch}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_market_history_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new open interest data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-02-03');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const queryResult = await trx.raw(`
            WITH daily_market_oi AS (
              SELECT
                DATE(ts) AS day,
                market_symbol,
                AVG(size_usd) AS daily_market_oi
              FROM
                ${tableName}
              WHERE DATE(ts) >= DATE(?)
              GROUP BY
                DATE(ts),
                market_symbol
            ),
            daily_oi AS (
              SELECT
                day,
                SUM(daily_market_oi) AS daily_oi
              FROM
                daily_market_oi
              GROUP BY
                day
            )
            SELECT
              day AS ts,
              daily_oi
            FROM
              daily_oi
            ORDER BY
              ts ASC;
          `, [startDate]);

          const newResult = queryResult.rows.map(row => ({
            ts: row.ts,
            daily_oi: parseFloat(row.daily_oi),
          }));

          console.log(`Fetched ${newResult.length} new records from database`);

          if (result) {
            console.log('Merging existing result with new data');
            const mergedResult = [...result];
          
            newResult.forEach(newRow => {
              // Find existing entry based on date day
              const existingIndex = mergedResult.findIndex(r => 
                new Date(r.ts).toISOString().split('T')[0] === new Date(newRow.ts).toISOString().split('T')[0]
              );
          
              if (existingIndex !== -1) {
                // If the day exists, recalculate the average
                const existingRow = mergedResult[existingIndex];
                mergedResult[existingIndex] = {
                  ts: existingRow.ts, 
                  daily_oi: newRow.daily_oi
                };
              } else {
                // If it's a new day, add the new entry
                mergedResult.push(newRow);
              }
            });
          
            // Ensure the data is sorted by date after merging
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

    console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
    return { [chainToFetch]: result || [] };
  };

  try {
    if (chain) {
      return await fetchDataForChain(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchDataForChain));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getOpenInterestData:', error);
    throw new Error('Error fetching open interest data: ' + error.message);
  }
};

const getDailyOpenInterestChangeData = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getDailyOpenInterestChangeData called with chain: ${chain}, isRefresh: ${isRefresh}`);

  const fetchDataForChain = async (chainToFetch) => {
    const cacheKey = `dailyOpenInterestChangeData:${chainToFetch}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_market_history_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new daily open interest change data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-02-03');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const queryResult = await trx.raw(`
            WITH daily_market_oi AS (
              SELECT
                DATE_TRUNC('day', ts) AS day,
                market_symbol,
                AVG(size_usd) AS daily_market_oi
              FROM
                ${tableName}
              WHERE ts > ?
              GROUP BY
                DATE_TRUNC('day', ts),
                market_symbol
            ),
            daily_oi AS (
              SELECT
                day,
                SUM(daily_market_oi) AS daily_oi
              FROM
                daily_market_oi
              GROUP BY
                day
            ),
            daily_oi_change AS (
              SELECT
                day AS ts,
                daily_oi,
                daily_oi - LAG(daily_oi) OVER (ORDER BY day) AS daily_oi_change
              FROM
                daily_oi
            )
            SELECT
              ts,
              daily_oi,
              daily_oi_change
            FROM
              daily_oi_change
            WHERE
              daily_oi_change IS NOT NULL
            ORDER BY
              ts ASC;
          `, [startDate]);

          const newResult = queryResult.rows.map(row => ({
            ts: row.ts,
            daily_oi_change: parseFloat(row.daily_oi_change)
          }));

          console.log(`Fetched ${newResult.length} new records from database`);

          if (result) {
            console.log('Merging existing result with new data');
            const mergedResult = [...result];
          
            newResult.forEach(newRow => {
              // Find existing entry based on date day
              const existingIndex = mergedResult.findIndex(r => 
                new Date(r.ts).toISOString().split('T')[0] === new Date(newRow.ts).toISOString().split('T')[0]
              );
          
              if (existingIndex !== -1) {
                // If the day exists, recalculate the average
                const existingRow = mergedResult[existingIndex];
                mergedResult[existingIndex] = {
                  ts: existingRow.ts, 
                  daily_oi: newRow.daily_oi
                };
              } else {
                // If it's a new day, add the new entry
                mergedResult.push(newRow);
              }
            });
          
            // Ensure the data is sorted by date after merging
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

    console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
    return { [chainToFetch]: result || [] };
  };

  try {
    if (chain) {
      return await fetchDataForChain(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchDataForChain));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getDailyOpenInterestChangeData:', error);
    throw new Error('Error fetching daily open interest change data: ' + error.message);
  }
};

const getOpenInterestSummaryStats = async (chain, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getOpenInterestSummaryStats called with chain: ${chain}, isRefresh: ${isRefresh}`);

  try {
    const processChainData = async (chainToProcess) => {
      const summaryCacheKey = `openInterestSummaryStats:${chainToProcess}`;
      const summaryTsKey = `${summaryCacheKey}:timestamp`;
      
      const openInterestDataKey = `openInterestData:${chainToProcess}`;
      const openInterestDataTsKey = `${openInterestDataKey}:timestamp`;

      let summaryResult = await redisService.get(summaryCacheKey);
      let summaryTimestamp = await redisService.get(summaryTsKey);
      let openInterestDataTimestamp = await redisService.get(openInterestDataTsKey);

      console.log(`Summary cache timestamp: ${summaryTimestamp}`);
      console.log(`Open Interest data cache timestamp: ${openInterestDataTimestamp}`);

      if (isRefresh || !summaryResult || !summaryTimestamp || 
          (openInterestDataTimestamp && new Date(openInterestDataTimestamp) > new Date(summaryTimestamp))) {
        console.log('Processing open interest summary stats for', chainToProcess);

        const data = await getOpenInterestData(chainToProcess, false, trx);
        const chainData = data[chainToProcess];
        
        if (chainData.length === 0) {
          console.log('No data found for', chainToProcess);
          return null;
        }

        const latestData = chainData[chainData.length - 1];
        const latestTs = new Date(latestData.ts);
        
        const findValueAtDate = (days) => {
          const targetDate = new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);
          return chainData.findLast(item => new Date(item.ts) <= targetDate);
        };
        
        const value24h = findValueAtDate(1);
        const value7d = findValueAtDate(7);
        const value28d = findValueAtDate(28);
        const valueYtd = chainData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1)) || chainData[0];
        
        const current = parseFloat(latestData.daily_oi);
        const oiValues = chainData.map(item => parseFloat(item.daily_oi));
        
        summaryResult = {
          current,
          delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.daily_oi) : null),
          delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.daily_oi) : null),
          delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.daily_oi) : null),
          delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.daily_oi) : null),
          ath: Math.max(...oiValues),
          atl: Math.min(...oiValues),
        };
        
        summaryResult.ath_percentage = calculatePercentage(current, summaryResult.ath);
        summaryResult.atl_percentage = summaryResult.atl === 0 ? 100 : calculatePercentage(current, summaryResult.atl);

        console.log('Attempting to cache summary stats in Redis');
        try {
          await redisService.set(summaryCacheKey, summaryResult, CACHE_TTL);
          await redisService.set(summaryTsKey, openInterestDataTimestamp || new Date().toISOString(), CACHE_TTL);
          console.log('Summary stats successfully cached in Redis');
        } catch (redisError) {
          console.error('Error caching summary stats in Redis:', redisError);
        }
      } else {
        console.log('Using cached summary stats');
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
    console.error('Error in getOpenInterestSummaryStats:', error);
    throw new Error(`Error fetching open interest summary stats: ${error.message}`);
  }
};

const refreshAllPerpMarketHistoryData = async () => {
  console.log('Starting to refresh Perp Market History data for all chains');
  
  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing Perp Market History data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getOpenInterestData`);
        await getOpenInterestData(chain, true, trx);
        console.timeEnd(`${chain} getOpenInterestData`);

        console.time(`${chain} getDailyOpenInterestChangeData`);
        await getDailyOpenInterestChangeData(chain, true, trx);
        console.timeEnd(`${chain} getDailyOpenInterestChangeData`);

        console.time(`${chain} getOpenInterestSummaryStats`);
        await getOpenInterestSummaryStats(chain, true, trx);
        console.timeEnd(`${chain} getOpenInterestSummaryStats`);

      } catch (error) {
        console.error(`Error refreshing Perp Market History data for chain ${chain}:`, error);
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing Perp Market History data for chain: ${chain}`);
  }

  console.log('Finished refreshing Perp Market History data for all chains');
};

module.exports = {
  getOpenInterestData,
  getDailyOpenInterestChangeData,
  getOpenInterestSummaryStats,
  refreshAllPerpMarketHistoryData
};