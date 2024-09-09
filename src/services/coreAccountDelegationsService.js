const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['core_account_delegations'];

const getStakerCount = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getStakerCount called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const fetchCount = async (chainToFetch) => {
      const cacheKey = `stakerCount:${chainToFetch}:${collateralType}`;
      const tsKey = `${cacheKey}:timestamp`;
      
      console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
      let result = await redisService.get(cacheKey);
      let cachedTimestamp = await redisService.get(tsKey);

      console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

      if (isRefresh || !result) {
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_account_delegation_${chainToFetch}_mainnet`;
        console.log(`Querying database table: ${tableName}`);

        try {
          const latestDbTimestamp = await trx(tableName)
            .where('collateral_type', collateralType)
            .max('ts as latest_ts')
            .first();

          console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

          if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
            console.log('Fetching new staker count data from database');
            
            const queryResult = await trx(tableName)
              .where('collateral_type', collateralType)
              .countDistinct('account_id as staker_count')
              .first();
            
            result = parseInt(queryResult.staker_count);

            console.log(`Fetched new staker count: ${result}`);

            console.log('Attempting to cache new data in Redis');
            try {
              await redisService.set(cacheKey, result, CACHE_TTL);
              await redisService.set(tsKey, latestDbTimestamp.latest_ts, CACHE_TTL);
              console.log('Data successfully cached in Redis');
            } catch (redisError) {
              console.error('Error caching data in Redis:', redisError);
            }
          } else {
            console.log('Using cached data, no need to fetch from database');
          }
        } catch (dbError) {
          console.error('Error querying database:', dbError);
          result = 0;
        }
      } else {
        console.log('Not refreshing, using cached result');
      }

      console.log(`Returning result for ${chainToFetch}: ${result}`);
      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchCount(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchCount));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getStakerCount:', error);
    throw new Error('Error fetching staker count: ' + error.message);
  }
};

const getCumulativeUniqueStakers = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getCumulativeUniqueStakers called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const fetchCumulativeData = async (chainToFetch) => {
      const cacheKey = `cumulativeUniqueStakers:${chainToFetch}:${collateralType}`;
      const tsKey = `${cacheKey}:timestamp`;
      
      console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
      let result = await redisService.get(cacheKey);
      let cachedTimestamp = await redisService.get(tsKey);

      console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

      if (isRefresh || !result) {
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_account_delegation_${chainToFetch}_mainnet`;
        console.log(`Querying database table: ${tableName}`);

        try {
          const latestDbTimestamp = await trx(tableName)
            .where('collateral_type', collateralType)
            .max('ts as latest_ts')
            .first();

          console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

          if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
            console.log('Fetching new cumulative unique stakers data from database');
            const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
            console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

            const queryResult = await trx.raw(`
              WITH daily_stakers AS (
                SELECT DISTINCT
                  DATE_TRUNC('day', ts) AS day,
                  account_id,
                  pool_id,
                  collateral_type
                FROM
                  ${tableName}
                WHERE
                  collateral_type = ? AND ts > ?
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
            `, [collateralType, startDate]);

            const newResult = queryResult.rows.map(row => ({
              ts: row.ts,
              cumulative_staker_count: parseInt(row.cumulative_staker_count),
              pool_id: row.pool_id,
              collateral_type: row.collateral_type,
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
        console.log('Not refreshing, parsing cached result');
        result = result;
      }

      console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
      return { [chainToFetch]: result || [] };
    };

    if (chain) {
      return await fetchCumulativeData(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchCumulativeData));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getCumulativeUniqueStakers:', error);
    throw new Error('Error fetching cumulative unique staker data: ' + error.message);
  }
};

const getUniqueStakersSummaryStats = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getUniqueStakersSummaryStats called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const processChainData = async (chainToProcess) => {
      const summaryCacheKey = `uniqueStakersSummary:${chainToProcess}:${collateralType}`;
      const summaryTsKey = `${summaryCacheKey}:timestamp`;
      
      const cumulativeDataKey = `cumulativeUniqueStakers:${chainToProcess}:${collateralType}`;
      const cumulativeDataTsKey = `${cumulativeDataKey}:timestamp`;

      let summaryResult = await redisService.get(summaryCacheKey);
      let summaryTimestamp = await redisService.get(summaryTsKey);
      let cumulativeDataTimestamp = await redisService.get(cumulativeDataTsKey);

      console.log(`Summary cache timestamp: ${summaryTimestamp}`);
      console.log(`Cumulative data cache timestamp: ${cumulativeDataTimestamp}`);

      if (isRefresh || !summaryResult || !summaryTimestamp || 
          (cumulativeDataTimestamp && new Date(cumulativeDataTimestamp) > new Date(summaryTimestamp))) {
        console.log('Processing unique stakers summary for', chainToProcess);

        try {
          const cumulativeData = await getCumulativeUniqueStakers(chainToProcess, collateralType, false, trx);
          const data = cumulativeData[chainToProcess];

          if (data.length === 0) {
            console.log('No data found for', chainToProcess);
            return null;
          }

          const smoothedData = smoothData(data, 'cumulative_staker_count');
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

          const stakerValues = smoothedData.map(item => parseFloat(item.cumulative_staker_count));
          const current = parseFloat(latestData.cumulative_staker_count);
          const ath = Math.max(...stakerValues);
          const atl = Math.min(...stakerValues);

          summaryResult = {
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

          await redisService.set(summaryCacheKey, summaryResult, CACHE_TTL);
          await redisService.set(summaryTsKey, cumulativeDataTimestamp || new Date().toISOString(), CACHE_TTL);

          console.log('Unique stakers summary updated and cached');
        } catch (error) {
          console.error(`Error processing data for ${chainToProcess}:`, error);
          return null;
        }
      } else {
        console.log('Using cached unique stakers summary data');
      }

      return summaryResult ? summaryResult : null;
    };

    if (chain) {
      const result = await processChainData(chain);
      return result ? { [chain]: result } : {};
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(processChainData));
      return Object.fromEntries(SERVICE_CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    console.error('Error in getUniqueStakersSummaryStats:', error);
    throw new Error('Error fetching unique stakers summary stats: ' + error.message);
  }
};

const getDailyNewUniqueStakers = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    console.log(`getDailyNewUniqueStakers called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

    const fetchDailyData = async (chainToFetch) => {
      const cacheKey = `dailyNewUniqueStakers:${chainToFetch}:${collateralType}`;
      const tsKey = `${cacheKey}:timestamp`;
      
      console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
      let result = await redisService.get(cacheKey);
      let cachedTimestamp = await redisService.get(tsKey);

      console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

      if (isRefresh || !result) {
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_account_delegation_${chainToFetch}_mainnet`;
        console.log(`Querying database table: ${tableName}`);

        try {
          const latestDbTimestamp = await trx(tableName)
            .where('collateral_type', collateralType)
            .max('ts as latest_ts')
            .first();

          console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

          if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
            console.log('Fetching new daily unique stakers data from database');
            const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
            console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

            const queryResult = await trx.raw(`
              WITH daily_stakers AS (
                SELECT DISTINCT
                  DATE_TRUNC('day', ts) AS date,
                  account_id
                FROM
                  ${tableName}
                WHERE
                  collateral_type = ? AND ts > ?
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
            `, [collateralType, startDate]);

            const newResult = queryResult.rows.map(row => ({
              ts: row.date,
              daily_unique_stakers: parseInt(row.daily_unique_stakers),
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
        console.log('Not refreshing, parsing cached result');
        result = result;
      }

      console.log(`Returning result for ${chainToFetch}: ${result ? result.length + ' records' : 'No data'}`);
      return { [chainToFetch]: result || [] };
    };

    if (chain) {
      return await fetchDailyData(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(chainToFetch => fetchDailyData(chainToFetch)));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getDailyNewUniqueStakers:', error);
    throw new Error('Error fetching daily new unique stakers: ' + error.message);
  }
};

const refreshAllCoreAccountDelegationsData = async (collateralType) => {
  console.log('Starting to refresh Core Account Delegations data for all chains');

  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing core account delegations data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a single transaction for all database operations
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getStakerCount`);
        await getStakerCount(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getStakerCount`);

        console.time(`${chain} getCumulativeUniqueStakers`);
        await getCumulativeUniqueStakers(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCumulativeUniqueStakers`);

        console.time(`${chain} getUniqueStakersSummaryStats`);
        await getUniqueStakersSummaryStats(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getUniqueStakersSummaryStats`);

        console.time(`${chain} getDailyNewUniqueStakers`);
        await getDailyNewUniqueStakers(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getDailyNewUniqueStakers`);

      } catch (error) {
        console.error(`Error refreshing core account delegations data for chain ${chain}:`, error);
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing core account delegations data for chain: ${chain}`);
  }

  console.log('Finished refreshing Core Account Delegations data for all chains');
};

module.exports = {
  getStakerCount,
  getCumulativeUniqueStakers,
  getUniqueStakersSummaryStats,
  getDailyNewUniqueStakers,
  refreshAllCoreAccountDelegationsData,
};