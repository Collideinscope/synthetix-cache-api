const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestPoolRewardsData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  const fetchLatest = async (chainToFetch) => {
    const cacheKey = `latestPoolRewards:${chainToFetch}:${collateralType}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_pool_rewards_hourly_${chainToFetch}_mainnet`;
      try {
        result = await trx(tableName)
          .where({ collateral_type: collateralType })
          .orderBy('ts', 'desc')
          .limit(1);
        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching latest pool rewards data for ${chainToFetch}:`, error.message);
        result = [];
      }
    }

    return { [chainToFetch]: result };
  };

  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

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
    console.error('Error in getLatestPoolRewardsData:', error);
    return {};
  }
};

const getCumulativePoolRewardsData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  const fetchCumulative = async (chainToFetch) => {
    const cacheKey = `cumulativePoolRewards:${chainToFetch}:${collateralType}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_pool_rewards_hourly_${chainToFetch}_mainnet`;
      try {
        result = await trx(tableName)
          .select('ts', 'pool_id', 'collateral_type', 'rewards_usd')
          .where({ collateral_type: collateralType })
          .orderBy('ts', 'asc');

        let cumulativeRewards = 0;
        result = result.map(row => {
          cumulativeRewards += parseFloat(row.rewards_usd);
          return {
            ...row,
            cumulative_rewards_usd: parseFloat(cumulativeRewards.toFixed(2))
          };
        });

        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching cumulative pool rewards data for ${chainToFetch}:`, error.message);
        result = [];
      }
    }

    return { [chainToFetch]: result };
  };

  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    if (chain) {
      return await fetchCumulative(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCumulative));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index][chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    console.error('Error in getCumulativePoolRewardsData:', error);
    return {};
  }
};

const getPoolRewardsSummaryStats = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  const processChainData = async (chainToProcess) => {
    const cacheKey = `poolRewardsSummary:${chainToProcess}:${collateralType}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('Processing pool rewards summary');
      const allData = await getCumulativePoolRewardsData(chainToProcess, collateralType, bypassCache = false, trx = troyDBKnex);
      const chainData = allData[chainToProcess];
      
      if (chainData.length === 0) {
        result = {};
      } else {
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
        
        result = {
          current,
          delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_rewards_usd) : null),
          delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_rewards_usd) : null),
          delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_rewards_usd) : null),
          delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_rewards_usd) : null),
          ath: Math.max(...rewardsValues),
          atl: Math.min(...rewardsValues),
        };
        
        result.ath_percentage = calculatePercentage(current, result.ath);
        result.atl_percentage = result.atl === 0 ? 100 : calculatePercentage(current, result.atl);
      }
      
      await redisService.set(cacheKey, result, CACHE_TTL);
    }
    
    return result;
  };
  
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    if (chain) {
      const result = await processChainData(chain);
      return { [chain]: result };
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return Object.fromEntries(CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    console.error('Error in getPoolRewardsSummaryStats:', error);
    return {};
  }
};

const getDailyPoolRewardsData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  const fetchDaily = async (chainToFetch) => {
    const cacheKey = `dailyPoolRewards:${chainToFetch}:${collateralType}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_pool_rewards_hourly_${chainToFetch}_mainnet`;
      try {
        result = await trx.raw(`
          WITH daily_data AS (
            SELECT
              DATE_TRUNC('day', ts) AS date,
              SUM(rewards_usd) AS daily_rewards
            FROM ${tableName}
            WHERE collateral_type = ?
            GROUP BY DATE_TRUNC('day', ts)
            ORDER BY DATE_TRUNC('day', ts)
          )
          SELECT
            date,
            daily_rewards
          FROM daily_data
          ORDER BY date;
        `, [collateralType]);

        result = result.rows.map(row => ({
          ts: row.date,
          daily_rewards: parseFloat(row.daily_rewards)
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching daily pool rewards data for ${chainToFetch}:`, error.message);
        result = [];
      }
    }

    return { [chainToFetch]: result };
  };

  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, result) => ({ ...acc, ...result }), {});
    }
  } catch (error) {
    console.error('Error in getDailyPoolRewardsData:', error);
    return {};
  }
};

const refreshAllPoolRewardsData = async (collateralType) => {
  console.log('Starting to refresh Pool Rewards data for all chains');
  
  for (const chain of CHAINS) {
    console.log(`Refreshing Pool Rewards data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);
    
    // Clear existing cache
    await redisService.del(`latestPoolRewards:${chain}:${collateralType}`);
    await redisService.del(`cumulativePoolRewards:${chain}:${collateralType}`);
    await redisService.del(`poolRewardsSummary:${chain}:${collateralType}`);
    await redisService.del(`dailyPoolRewards:${chain}:${collateralType}`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getLatestPoolRewardsData`);
        await getLatestPoolRewardsData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getLatestPoolRewardsData`);

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
        // Don't throw the error, just log it and continue with the next chain
      }
    });

    console.timeEnd(`${chain} total refresh time`);
  }

  console.log('Finished refreshing Pool Rewards data for all chains');
};

module.exports = {
  getLatestPoolRewardsData,
  getCumulativePoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
  refreshAllPoolRewardsData,
};