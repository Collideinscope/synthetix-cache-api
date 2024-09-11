const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['tvl'];

const getCumulativeTVLData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativeTVLData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const fetchAll = async (chainToFetch) => {
    const cacheKey = `cumulativeTVL:${chainToFetch}:${collateralType}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_core_vault_collateral_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .where('collateral_type', collateralType)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new cumulative TVL data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-03-26');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const newData = await trx(tableName)
            .where('ts', '>=', startDate)
            .where({
              pool_id: 1,
              collateral_type: collateralType
            })
            .orderBy('ts', 'asc');

          console.log(`Fetched ${newData.length} new records from database`);

          if (result) {
            console.log('Merging existing result with new data');
            const mergedResult = [...result];
            newData.forEach(newRow => {
              const existingIndex = mergedResult.findIndex(r => r.ts === newRow.ts);
              if (existingIndex !== -1) {
                // Update existing entry
                mergedResult[existingIndex] = newRow;
              } else {
                // Add new entry
                mergedResult.push(newRow);
              }
            });
            result = mergedResult.sort((a, b) => new Date(a.ts) - new Date(b.ts));
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

  try {
    if (chain) {
      return await fetchAll(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchAll));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getCumulativeTVLData:', error);
    throw new Error('Error fetching cumulative TVL data: ' + error.message);
  }
};

const getTVLSummaryStats = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getTVLSummaryStats called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const processChainData = async (chainToProcess) => {
    const summaryCacheKey = `TVLSummary:${chainToProcess}:${collateralType}`;
    const summaryTsKey = `${summaryCacheKey}:timestamp`;
    
    const cumulativeDataKey = `cumulativeTVL:${chainToProcess}:${collateralType}`;
    const cumulativeDataTsKey = `${cumulativeDataKey}:timestamp`;

    let summaryResult = await redisService.get(summaryCacheKey);
    let summaryTimestamp = await redisService.get(summaryTsKey);
    let cumulativeDataTimestamp = await redisService.get(cumulativeDataTsKey);

    console.log(`Summary cache timestamp: ${summaryTimestamp}`);
    console.log(`Cumulative data cache timestamp: ${cumulativeDataTimestamp}`);

    if (isRefresh || !summaryResult || !summaryTimestamp || 
        (cumulativeDataTimestamp && new Date(cumulativeDataTimestamp) > new Date(summaryTimestamp))) {
      console.log('Processing TVL summary for', chainToProcess);

      const allData = await getCumulativeTVLData(chainToProcess, collateralType, false, trx);
      const chainData = allData[chainToProcess];
      
      if (chainData.length === 0) {
        console.log('No data found for', chainToProcess);
        return null;
      }

      const smoothedData = smoothData(chainData, 'collateral_value');
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
      
      const current = parseFloat(latestData.collateral_value);
      const tvlValues = smoothedData.map(item => parseFloat(item.collateral_value));
      
      summaryResult = {
        current,
        delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.collateral_value) : null),
        delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.collateral_value) : null),
        delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.collateral_value) : null),
        delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.collateral_value) : null),
        ath: Math.max(...tvlValues),
        atl: Math.min(...tvlValues),
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
    console.error('Error in getTVLSummaryStats:', error);
    throw new Error(`Error fetching TVL summary stats: ${error.message}`);
  }
};

const getDailyTVLData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getDailyTVLData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const fetchDaily = async (chainToProcess) => {
    const cacheKey = `dailyTVL:${chainToProcess}:${collateralType}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToProcess}_mainnet.fct_core_vault_collateral_${chainToProcess}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .where('collateral_type', collateralType)
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new daily TVL data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2024-03-26');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const queryResult = await trx.raw(`
            WITH daily_data AS (
              SELECT
                DATE_TRUNC('day', ts) AS date,
                FIRST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_tvl,
                LAST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_tvl
              FROM ${tableName}
              WHERE pool_id = 1
                AND collateral_type = ?
                AND ts > ?
              GROUP BY DATE_TRUNC('day', ts), ts
            )
            SELECT DISTINCT
              date,
              end_of_day_tvl - start_of_day_tvl AS daily_tvl_change
            FROM daily_data
            ORDER BY date;
          `, [collateralType, startDate]);

          const newResult = queryResult.rows.map(row => ({
            ts: row.date,
            daily_tvl_change: parseFloat(row.daily_tvl_change)
          }));

          console.log(`Fetched ${newResult.length} new records from database`);

          if (result) {
            console.log('Merging existing result with new data');
            const mergedResult = [...result];
            newResult.forEach(newRow => {
              const existingIndex = mergedResult.findIndex(r => r.ts === newRow.ts);
              if (existingIndex !== -1) {
                // Update existing entry
                mergedResult[existingIndex] = newRow;
              } else {
                // Add new entry
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

    console.log(`Returning result for ${chainToProcess}: ${result ? result.length + ' records' : 'No data'}`);
    return { [chainToProcess]: result || [] };
  };

  try {
    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchDaily));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getDailyTVLData:', error);
    throw new Error('Error fetching daily TVL data: ' + error.message);
  }
};

const refreshAllTVLData = async (collateralType) => {
  console.log('Starting to refresh TVL data for all chains');
  
  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing TVL data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getCumulativeTVLData`);
        await getCumulativeTVLData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCumulativeTVLData`);

        console.time(`${chain} getTVLSummaryStats`);
        await getTVLSummaryStats(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getTVLSummaryStats`);

        console.time(`${chain} getDailyTVLData`);
        await getDailyTVLData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getDailyTVLData`);

      } catch (error) {
        console.error(`Error refreshing TVL data for chain ${chain}:`, error);
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing TVL data for chain: ${chain}`);
  }

  console.log('Finished refreshing TVL data for all chains');
};

module.exports = {
  getCumulativeTVLData,
  getTVLSummaryStats,
  getDailyTVLData,
  refreshAllTVLData
};