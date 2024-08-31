const { knex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestPoolRewardsData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchLatest = async (chainToFetch) => {
      const cacheKey = `latestPoolRewards:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex('pool_rewards')
          .where({ chain: chainToFetch, collateral_type: collateralType })
          .orderBy('ts', 'desc')
          .limit(1);
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

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
    throw new Error('Error fetching latest pool rewards data: ' + error.message);
  }
};

const getCumulativePoolRewardsData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchCumulative = async (chainToFetch) => {
      const cacheKey = `cumulativePoolRewards:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex('pool_rewards')
          .select('ts', 'chain', 'pool_id', 'collateral_type', 'rewards_usd')
          .where({ chain: chainToFetch, collateral_type: collateralType })
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
      }

      return { [chainToFetch]: result };
    };

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
    throw new Error('Error fetching cumulative pool rewards data: ' + error.message);
  }
};

const getPoolRewardsSummaryStats = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cacheKey = `poolRewardsSummary:${chainToProcess}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing pool rewards summary');
        const allData = await getCumulativePoolRewardsData(chainToProcess, collateralType);
        const chainData = allData[chainToProcess];

        if (chainData.length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(chainData, 'cumulative_rewards_usd');
          const reversedSmoothedData = [...smoothedData].reverse();

          const latestData = reversedSmoothedData[0];
          const latestTs = new Date(latestData.ts);

          const getDateFromLatest = (days) => new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);

          const value24h = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(1));
          const value7d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(7));
          const value28d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(28));
          let valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1));

          if (!valueYtd) {
            valueYtd = reversedSmoothedData[reversedSmoothedData.length - 1];
          }

          const rewardsValues = smoothedData.map(item => parseFloat(item.cumulative_rewards_usd));
          const current = parseFloat(chainData[chainData.length - 1].cumulative_rewards_usd);

          const ath = Math.max(...rewardsValues, current);
          const atl = Math.min(...rewardsValues, current);

          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_rewards_usd) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_rewards_usd) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_rewards_usd) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_rewards_usd) : null),
            ath,
            atl,
            ath_percentage: calculatePercentage(current, ath),
            atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
          };
        }

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return result;
    };

    if (chain) {
      const result = await processChainData(chain);
      return { [chain]: result };
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index] || {};
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching Pool Rewards summary stats: ' + error.message);
  }
};

const getDailyPoolRewardsData = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDaily = async (chainToFetch) => {
      const cacheKey = `dailyPoolRewards:${chainToFetch}:${collateralType}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex.raw(`
          WITH daily_data AS (
            SELECT
              DATE_TRUNC('day', ts) AS date,
              SUM(rewards_usd) AS daily_rewards
            FROM pool_rewards
            WHERE chain = ? AND collateral_type = ?
            GROUP BY DATE_TRUNC('day', ts)
            ORDER BY DATE_TRUNC('day', ts)
          )
          SELECT
            date,
            daily_rewards
          FROM daily_data
          ORDER BY date;
        `, [chainToFetch, collateralType]);

        result = result.rows.map(row => ({
          ts: row.date,
          daily_rewards: parseFloat(row.daily_rewards)
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, result) => ({ ...acc, ...result }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily pool rewards data: ' + error.message);
  }
};

module.exports = {
  getLatestPoolRewardsData,
  getCumulativePoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
};