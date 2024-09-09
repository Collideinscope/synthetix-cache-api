const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');
const { calculateDelta, calculatePercentage, smoothData } = require('../helpers');

const CACHE_TTL = 60 * 60 * 24 * 365; // 1 year in seconds
const SERVICE_CHAINS = CHAINS['pool_rewards'];

const getCumulativePoolRewardsData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getCumulativePoolRewardsData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const fetchCumulative = async (chainToFetch) => {
    const cacheKey = `cumulativePoolRewards:${chainToFetch}:${collateralType}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_pool_rewards_hourly_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .where({ collateral_type: collateralType })
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new cumulative pool rewards data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const newData = await trx(tableName)
            .select('ts', 'pool_id', 'collateral_type', 'rewards_usd')
            .where({ collateral_type: collateralType })
            .where('ts', '>', startDate)
            .orderBy('ts', 'asc');

          console.log(`Fetched ${newData.length} new records from database`);

          let cumulativeRewards = result && result.length > 0 ? result[result.length - 1].cumulative_rewards_usd : 0;
          const processedNewData = newData.map(row => {
            cumulativeRewards += parseFloat(row.rewards_usd);
            return {
              ...row,
              cumulative_rewards_usd: parseFloat(cumulativeRewards.toFixed(2))
            };
          });

          if (result) {
            console.log('Parsing and concatenating existing result with new data');
            result = result.concat(processedNewData);
          } else {
            console.log('Setting result to new data');
            result = processedNewData;
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
      return await fetchCumulative(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchCumulative));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getCumulativePoolRewardsData:', error);
    throw new Error('Error fetching cumulative pool rewards data: ' + error.message);
  }
};

const getPoolRewardsSummaryStats = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getPoolRewardsSummaryStats called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const processChainData = async (chainToProcess) => {
    const summaryCacheKey = `poolRewardsSummary:${chainToProcess}:${collateralType}`;
    const summaryTsKey = `${summaryCacheKey}:timestamp`;
    
    const cumulativeDataKey = `cumulativePoolRewards:${chainToProcess}:${collateralType}`;
    const cumulativeDataTsKey = `${cumulativeDataKey}:timestamp`;

    let summaryResult = await redisService.get(summaryCacheKey);
    let summaryTimestamp = await redisService.get(summaryTsKey);
    let cumulativeDataTimestamp = await redisService.get(cumulativeDataTsKey);

    console.log(`Summary cache timestamp: ${summaryTimestamp}`);
    console.log(`Cumulative data cache timestamp: ${cumulativeDataTimestamp}`);

    if (isRefresh || !summaryResult || !summaryTimestamp || 
        (cumulativeDataTimestamp && new Date(cumulativeDataTimestamp) > new Date(summaryTimestamp))) {
      console.log('Processing pool rewards summary for', chainToProcess);

      const allData = await getCumulativePoolRewardsData(chainToProcess, collateralType, false, trx);
      const chainData = allData[chainToProcess];
      
      if (chainData.length === 0) {
        console.log('No data found for', chainToProcess);
        return null;
      }

      const smoothedData = smoothData(chainData, 'cumulative_rewards_usd');
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
      
      const current = parseFloat(latestData.cumulative_rewards_usd);
      const rewardsValues = smoothedData.map(item => parseFloat(item.cumulative_rewards_usd));
      
      summaryResult = {
        current,
        delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_rewards_usd) : null),
        delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_rewards_usd) : null),
        delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_rewards_usd) : null),
        delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_rewards_usd) : null),
        ath: Math.max(...rewardsValues),
        atl: Math.min(...rewardsValues),
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
    console.error('Error in getPoolRewardsSummaryStats:', error);
    throw new Error(`Error fetching pool rewards summary stats: ${error.message}`);
  }
};

const getDailyPoolRewardsData = async (chain, collateralType, isRefresh = false, trx = troyDBKnex) => {
  console.log(`getDailyPoolRewardsData called with chain: ${chain}, collateralType: ${collateralType}, isRefresh: ${isRefresh}`);

  if (!collateralType) {
    throw new Error('collateralType is required');
  }

  const fetchDaily = async (chainToFetch) => {
    const cacheKey = `dailyPoolRewards:${chainToFetch}:${collateralType}`;
    const tsKey = `${cacheKey}:timestamp`;
    
    console.log(`Attempting to get data from Redis for key: ${cacheKey}`);
    let result = await redisService.get(cacheKey);
    let cachedTimestamp = await redisService.get(tsKey);

    console.log(`Redis result: ${result ? 'Data found' : 'No data'}, Timestamp: ${cachedTimestamp}`);

    if (isRefresh || !result) {
      const tableName = `prod_${chainToFetch}_mainnet.fct_pool_rewards_hourly_${chainToFetch}_mainnet`;
      console.log(`Querying database table: ${tableName}`);

      try {
        const latestDbTimestamp = await trx(tableName)
          .where({ collateral_type: collateralType })
          .max('ts as latest_ts')
          .first();

        console.log(`Latest DB timestamp: ${JSON.stringify(latestDbTimestamp)}`);

        if (!result || !cachedTimestamp || new Date(latestDbTimestamp.latest_ts) > new Date(cachedTimestamp)) {
          console.log('Fetching new daily pool rewards data from database');
          const startDate = cachedTimestamp ? new Date(cachedTimestamp) : new Date('2023-01-01');
          console.log(`Fetching data from ${startDate.toISOString()} to ${latestDbTimestamp.latest_ts}`);

          const queryResult = await trx.raw(`
            WITH daily_data AS (
              SELECT
                DATE_TRUNC('day', ts) AS date,
                SUM(rewards_usd) AS daily_rewards
              FROM ${tableName}
              WHERE collateral_type = ? AND ts > ?
              GROUP BY DATE_TRUNC('day', ts)
              ORDER BY DATE_TRUNC('day', ts)
            )
            SELECT
              date,
              daily_rewards
            FROM daily_data
            ORDER BY date;
          `, [collateralType, startDate]);

          const newResult = queryResult.rows.map(row => ({
            ts: row.date,
            daily_rewards: parseFloat(row.daily_rewards)
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

  try {
    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(SERVICE_CHAINS.map(fetchDaily));
      return Object.assign({}, ...results);
    }
  } catch (error) {
    console.error('Error in getDailyPoolRewardsData:', error);
    throw new Error('Error fetching daily pool rewards data: ' + error.message);
  }
};

const refreshAllPoolRewardsData = async (collateralType) => {
  console.log('Starting to refresh Pool Rewards data for all chains');
  
  for (const chain of SERVICE_CHAINS) {
    console.log(`Refreshing Pool Rewards data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getCumulativePoolRewardsData`);
        await getCumulativePoolRewardsData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCumulativePoolRewardsData`);

        console.time(`${chain} getPoolRewardsSummaryStats`);
        await getPoolRewardsSummaryStats(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getPoolRewardsSummaryStats`);

        console.time(`${chain} getDailyPoolRewardsData`);
        await getDailyPoolRewardsData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getDailyPoolRewardsData`);

      } catch (error) {
        console.error(`Error refreshing Pool Rewards data for chain ${chain}:`, error);
        throw error; // This will cause the transaction to rollback
      }
    });

    console.timeEnd(`${chain} total refresh time`);
    console.log(`Finished refreshing Pool Rewards data for chain: ${chain}`);
  }

  console.log('Finished refreshing Pool Rewards data for all chains');
};

module.exports = {
  getCumulativePoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
  refreshAllPoolRewardsData,
};