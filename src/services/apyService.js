const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds

const getAllAPYData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  console.log(`getAllAPYData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  const fetchAll = async (chainToFetch) => {
    const cacheKey = `allAPY:${chainToFetch}:${collateralType}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_core_apr_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .where('collateral_type', collateralType)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new APY data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-05-01');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);
          
          const newData = await trx(tableName)
            .where('ts', '>', startDate)
            .where('collateral_type', collateralType)
            .select('ts', 'apy_24h', 'apy_7d', 'apy_28d')
            .orderBy('ts', 'asc');

          console.log(`Fetched ${newData.length} new records from database`);

          const newResult = newData.map(row => ({
            ts: row.ts,
            apy_24h: parseFloat(row.apy_24h),
            apy_7d: parseFloat(row.apy_7d),
            apy_28d: parseFloat(row.apy_28d)
          }));

          if (result) {
            console.log('Parsing and concatenating existing result with new data');
            result = result.concat(newResult);
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
      }
    } else if (result) {
      console.log('Not refreshing, parsing cached result');
      result = result;
    }

    console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
    return { [chainToFetch]: result || [] };
  };

  if (chain) {
    return await fetchAll(chain);
  } else {
    const results = await Promise.all(CHAINS.map(fetchAll));
    return Object.assign({}, ...results);
  }
};

const getAPYSummaryStats = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const summaryCacheKey = `APYSummary:${chainToProcess}:${collateralType}`;
      const summaryTsKey = `${summaryCacheKey}:timestamp`;
      
      const allAPYDataKey = `allAPY:${chainToProcess}:${collateralType}`;
      const allAPYDataTsKey = `${allAPYDataKey}:timestamp`;

      let summaryResult = await redisService.get(summaryCacheKey);
      let summaryTimestamp = await redisService.get(summaryTsKey);
      let allAPYDataTimestamp = await redisService.get(allAPYDataTsKey);

      console.log(`Summary cache timestamp: ${summaryTimestamp}`);
      console.log(`AllAPYData cache timestamp: ${allAPYDataTimestamp}`);

      if (isRefresh || !summaryResult || !summaryTimestamp || 
          (allAPYDataTimestamp && new Date(allAPYDataTimestamp) > new Date(summaryTimestamp))) {
        console.log('Processing APY summary for', chainToProcess);

        try {
          // Use isRefresh: false since we know the data has just been refreshed
          const allDataResult = await getAllAPYData(chainToProcess, collateralType, false, trx);
          const allData = allDataResult[chainToProcess];

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

          summaryResult = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.apy_28d) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.apy_7d) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.apy_28d) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.apy_28d) : null),
            ath,
            atl,
            ath_percentage: calculatePercentage(current, ath),
            atl_percentage: calculatePercentage(current, atl),
          };

          await redisService.set(summaryCacheKey, summaryResult, CACHE_TTL);
          await redisService.set(summaryTsKey, allAPYDataTimestamp || new Date().toISOString(), CACHE_TTL);

          console.log('APY summary updated and cached');
        } catch (error) {
          console.error(`Error processing data for ${chainToProcess}:`, error);
          return null;
        }
      } else {
        console.log('Using cached APY summary data');
      }

      return summaryResult ? summaryResult : null;
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

const getDailyAggregatedAPYData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDaily = async (chainToFetch) => {
      const cacheKey = `dailyAPY:${chainToFetch}:${collateralType}`;
      const tsKey = `${cacheKey}:timestamp`;
      
      let result = await redisService.get(cacheKey);
      let cachedTimestamp = await redisService.get(tsKey);

      if (isRefresh) {
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_apr_${chainToFetch}_mainnet`;
        const latestDbTimestamp = await trx(tableName)
          .where('collateral_type', collateralType)
          .max('ts as latest_ts')
          .first();

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new daily APY data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-05-01');
          console.log(`Fetching daily data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const newData = await trx.raw(`
            WITH daily_data AS (
              SELECT
                DATE_TRUNC('day', ts) AS date,
                FIRST_VALUE(apy_28d) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts) AS day_start_apy,
                LAST_VALUE(apy_28d) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts
                  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS day_end_apy
              FROM ${tableName}
              WHERE collateral_type = ? AND ts > ?
            )
            SELECT DISTINCT
              date as ts,
              CASE 
                WHEN day_start_apy = 0 OR day_end_apy = 0 THEN NULL
                ELSE (day_end_apy - day_start_apy) / day_start_apy
              END as daily_apy_percentage_delta
            FROM daily_data
            ORDER BY date;
          `, [collateralType, startDate]);

          const newResult = newData.rows.map(row => ({
            ts: row.ts,
            daily_apy_percentage_delta: row.daily_apy_percentage_delta !== null ? parseFloat(row.daily_apy_percentage_delta) : null
          }));

          if (result) {
            result = result.concat(newResult);
          } else {
            result = newResult;
          }

          if (result.length > 0) {
            await redisService.set(cacheKey, result, CACHE_TTL);
            await redisService.set(tsKey, latestDbTimestamp.latest_ts, CACHE_TTL);
          }
        }
      } else if (result) {
        // If not refreshing but result exists, parse it
        result = result;
      }
  
      return { [chainToFetch]: result || [] };
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
  console.log('Starting to refresh APY data for all chains');

  for (const chain of CHAINS['apy']) {
    console.log(`Refreshing APY data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a single transaction for all database operations
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getAllAPYData`);
        await getAllAPYData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getAllAPYData`);

        console.time(`${chain} getAPYSummaryStats`);
        await getAPYSummaryStats(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getAPYSummaryStats`);

        console.time(`${chain} getDailyAggregatedAPYData`);
        await getDailyAggregatedAPYData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getDailyAggregatedAPYData`);

      } catch (error) {
        console.error(`Error refreshing APY data for chain ${chain}:`, error);
        throw error; 
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing APY data for chain: ${chain}`);
  }

  console.log('Finished refreshing APY data for all chains');
};

module.exports = {
  getAllAPYData,
  getAPYSummaryStats,
  getDailyAggregatedAPYData,
  refreshAllAPYData,
};