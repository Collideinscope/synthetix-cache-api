const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['core_delegations'];

const getCoreDelegationsData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getCoreDelegationsData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const cacheKey = `coreDelegationsData:${chain}:${collateralType}`;
    const tsKey = `${cacheKey}:timestamp`;

    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chain}_mainnet.fct_core_pool_delegation_${chain}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .where('collateral_type', collateralType)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new core delegations data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-03-25');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const newData = await trx(tableName)
            .where('collateral_type', collateralType)
            .where('ts', '>', startDate)
            .orderBy('ts', 'asc');

          console.log(`Fetched ${newData.length} new records from database`);

          if (result) {
            console.log('Parsing and concatenating existing result with new data');
            result = result.concat(newData);
          } else {
            console.log('Setting result to new data');
            result = newData;
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
  } catch (error) {
    console.error('Error in getCoreDelegationsData:', error);
    throw new Error('Error fetching core delegations data: ' + error.message);
  }
};

const getCumulativeCoreDelegationsData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getCumulativeCoreDelegationsData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const fetchCumulative = async (chainToFetch) => {
      const cacheKey = `cumulativeCoreDelegations:${chainToFetch}:${collateralType}`;
      const tsKey = `${cacheKey}:timestamp`;
      
      console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
      let result = await redisService.get(cacheKey);
      let cachedTimestamp = await redisService.get(tsKey);

      console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

      if (isRefresh || !result) {
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_pool_delegation_${chainToFetch}_mainnet`;
        console.log(`Querying database table: ${tableName}`);

        try {
          const latestDbTimestamp = await trx(tableName)
            .where('collateral_type', collateralType)
            .max('ts as latest_ts')
            .first();

          console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

          if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
            console.log('Fetching new cumulative core delegations data from database');
            const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-03-25');
            console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

            const newData = await trx(tableName)
              .where('collateral_type', collateralType)
              .where('ts', '>', startDate)
              .orderBy('ts', 'asc');

            console.log(`Fetched ${newData.length} new records from database`);

            if (result) {
              console.log('Parsing and concatenating existing result with new data');
              result = result.concat(newData);
            } else {
              console.log('Setting result to new data');
              result = newData;
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

    if (chain) {
      return await fetchCumulative(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchCumulative));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getCumulativeCoreDelegationsData:', error);
    throw new Error('Error fetching cumulative core delegations data: ' + error.message);
  }
};

const getCoreDelegationsSummaryStats = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getCoreDelegationsSummaryStats called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const processChainData = async (chainToProcess) => {
      const summaryCacheKey = `coreDelegationsSummary:${chainToProcess}:${collateralType}`;
      const summaryTsKey = `${summaryCacheKey}:timestamp`;
      
      const delegationsDataKey = `coreDelegationsData:${chainToProcess}:${collateralType}`;
      const delegationsDataTsKey = `${delegationsDataKey}:timestamp`;

      let summaryResult = await redisService.get(summaryCacheKey);
      let summaryTimestamp = await redisService.get(summaryTsKey);
      let delegationsDataTimestamp = await redisService.get(delegationsDataTsKey);

      console.log(`Summary cache timestamp: ${summaryTimestamp}`);
      console.log(`Delegations data cache timestamp: ${delegationsDataTimestamp}`);

      if (isRefresh || !summaryResult || !summaryTimestamp || 
          (delegationsDataTimestamp && new Date(delegationsDataTimestamp) > new Date(summaryTimestamp))) {
        console.log('Processing core delegations summary for', chainToProcess);

        try {
          const allData = await getCoreDelegationsData(chainToProcess, collateralType, false, trx);
          
          if (allData.length === 0) {
            console.log('No data found for', chainToProcess);
            return null;
          }

          const smoothedData = smoothData(allData, 'amount_delegated');
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

          const current = parseFloat(latestData.amount_delegated);
          const delegationsValues = smoothedData.map(item => parseFloat(item.amount_delegated));

          summaryResult = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.amount_delegated) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.amount_delegated) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.amount_delegated) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.amount_delegated) : null),
            ath: Math.max(...delegationsValues),
            atl: Math.min(...delegationsValues),
          };

          summaryResult.ath_percentage = calculatePercentage(current, summaryResult.ath);
          summaryResult.atl_percentage = summaryResult.atl === 0 ? 100 : calculatePercentage(current, summaryResult.atl);

          await redisService.set(summaryCacheKey, summaryResult, CACHE_TTL);
          await redisService.set(summaryTsKey, delegationsDataTimestamp || new Date().toISOString(), CACHE_TTL);

          console.log('Core delegations summary updated and cached');
        } catch (error) {
          console.error(`Error processing data for ${chainToProcess}:`, error);
          return null;
        }
      } else {
        console.log('Using cached core delegations summary data');
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
    console.error('Error in getCoreDelegationsSummaryStats:', error);
    throw new Error(`Error fetching core delegations summary stats: ${error.message}`);
  }
};

const getDailyCoreDelegationsData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getDailyCoreDelegationsData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const fetchDaily = async (chainToFetch) => {
      const cacheKey = `dailyCoreDelegations:${chainToFetch}:${collateralType}`;
      const tsKey = `${cacheKey}:timestamp`;
      
      console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
      let result = await redisService.get(cacheKey);
      let cachedTimestamp = await redisService.get(tsKey);

      console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

      if (isRefresh || !result) {
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_pool_delegation_${chainToFetch}_mainnet`;
        console.log(`Querying database table: ${tableName}`);

        try {
          const latestDbTimestamp = await trx(tableName)
            .where('collateral_type', collateralType)
            .max('ts as latest_ts')
            .first();

          console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

          if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
            console.log('Fetching new daily core delegations data from database');
            const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-03-25');
            console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

            const queryResult = await trx.raw(`
              WITH daily_data AS (
                SELECT
                  DATE_TRUNC('day', ts) AS date,
                  FIRST_VALUE(SUM(amount_delegated)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_delegations,
                  LAST_VALUE(SUM(amount_delegated)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_delegations
                FROM ${tableName}
                WHERE collateral_type = ? AND ts > ?
                GROUP BY DATE_TRUNC('day', ts), ts
              )
              SELECT DISTINCT
                date,
                end_of_day_delegations - start_of_day_delegations AS daily_delegations_change
              FROM daily_data
              ORDER BY date;
            `, [collateralType, startDate]);

            const newResult = queryResult.rows.map(row => ({
              ts: row.date,
              daily_delegations_change: parseFloat(row.daily_delegations_change)
            }));

            console.log(`Fetched ${newResult.length} new records from database`);

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
          result = [];
        }
      } else {
        console.log('Not refreshing, using cached result');
      }

      console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
      return { [chainToFetch]: result || [] };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchDaily));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getDailyCoreDelegationsData:', error);
    throw new Error('Error fetching daily core delegations data: ' + error.message);
  }
};

const refreshAllCoreDelegationsData = async (collateralType) => {
  console.log('Starting to refresh Core Delegations data for all chains');

  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing core delegations data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a single transaction for all database operations
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getCoreDelegationsData`);
        await getCoreDelegationsData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCoreDelegationsData`);

        console.time(`${chain} getCumulativeCoreDelegationsData`);
        await getCumulativeCoreDelegationsData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCumulativeCoreDelegationsData`);

        console.time(`${chain} getCoreDelegationsSummaryStats`);
        await getCoreDelegationsSummaryStats(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCoreDelegationsSummaryStats`);

        console.time(`${chain} getDailyCoreDelegationsData`);
        await getDailyCoreDelegationsData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getDailyCoreDelegationsData`);

      } catch (error) {
        console.error(`Error refreshing core delegations data for chain ${chain}:`, error);
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing core delegations data for chain: ${chain}`);
  }

  console.log('Finished refreshing Core Delegations data for all chains');
};

module.exports = {
  getCoreDelegationsData,
  getCumulativeCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
  refreshAllCoreDelegationsData,
};