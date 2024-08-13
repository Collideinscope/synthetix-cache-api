const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const getLatestPoolRewardsData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specific chain
      const result = await knex('pool_rewards')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    }

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('pool_rewards')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean); // Filter out any undefined results
  } catch (error) {
    throw new Error('Error fetching latest pool rewards data: ' + error.message);
  }
};

const getAllPoolRewardsData = async (chain) => {
  try {
    let query = knex('pool_rewards')
      .select('ts', 'chain', 'pool_id', 'collateral_type', 'rewards_usd')
      .orderBy('ts', 'asc');
    
    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    const calculateCumulativeRewards = (data) => {
      let cumulativeRewards = 0;

      return data.map(row => {
        cumulativeRewards += parseFloat(row.rewards_usd);

        return {
          ...row,
          cumulative_rewards_usd: parseFloat(cumulativeRewards.toFixed(2)) 
        };
      });
    };

    if (chain && CHAINS.includes(chain)) {
      const processedResult = calculateCumulativeRewards(result);

      return processedResult;
    } else {
      const independentResults = {};

      for (const ch of CHAINS) {
        const chainData = result.filter(row => row.chain === ch);
        independentResults[ch] = calculateCumulativeRewards(chainData);
      }

      const combinedResult = calculateCumulativeRewards(result);

      return {
        ...independentResults,
        combined: combinedResult
      };
    }
  } catch (error) {
    throw new Error('Error fetching all pool rewards data: ' + error.message);
  }
};

const getPoolRewardsSummaryStats = async (chain) => {
  try {
    const baseQuery = () => knex('pool_rewards')
      .select('ts', 'chain', 'pool_id', 'collateral_type', 'rewards_usd')
      .where('chain', chain)
      .orderBy('ts', 'asc');

    const allData = await baseQuery();
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    // Calculate cumulative rewards
    let cumulativeRewards = 0;
    const processedResult = allData.map(row => {
      cumulativeRewards += parseFloat(row.rewards_usd);
      return {
        ...row,
        cumulative_rewards_usd: parseFloat(cumulativeRewards.toFixed(2))
      };
    });

    const smoothedData = smoothData([...processedResult], 'cumulative_rewards_usd');  
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
    const standardDeviation = calculateStandardDeviation(rewardsValues);

    const current = parseFloat(processedResult[processedResult.length - 1].cumulative_rewards_usd);

    const ath = Math.max(...rewardsValues, current);
    const atl = Math.min(...rewardsValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_rewards_usd) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_rewards_usd) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_rewards_usd) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_rewards_usd) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching Pool Rewards summary stats: ' + error.message);
  }
};

// Initial seed
const fetchAndInsertAllPoolRewardsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_pool_rewards_hourly_${chain}_mainnet`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, rewards_usd
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('pool_rewards')
      .insert(dataWithChainAdded)
      .onConflict(['ts', 'chain', 'pool_id', 'collateral_type'])
      .merge({
        rewards_usd: knex.raw('GREATEST(pool_rewards.rewards_usd, excluded.rewards_usd)'),
      });

    console.log(`Pool rewards data seeded successfully for ${chain}.`);
  } catch (error) {
    console.error('Error seeding pool rewards data:', error);
  }
};

const fetchAndUpdateLatestPoolRewardsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_pool_rewards_hourly_${chain}_mainnet`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('pool_rewards')
      .where('chain', chain)
      .orderBy('ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.ts;

    // Fetch new data starting from the last timestamp
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, rewards_usd
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new pool rewards data to update for ${chain} chain.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('pool_rewards')
      .insert(dataWithChainAdded)
      .onConflict(['ts', 'chain', 'pool_id', 'collateral_type'])
      .merge({
        rewards_usd: knex.raw('GREATEST(pool_rewards.rewards_usd, excluded.rewards_usd)'),
      });

    console.log(`Pool rewards data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating pool rewards data for ${chain} chain: ` + error.message);
  }
};

const fetchDailyPoolRewardsData = async (chain) => {
  const result = await knex.raw(`
    WITH daily_data AS (
      SELECT
        DATE_TRUNC('day', ts) AS date,
        SUM(rewards_usd) AS daily_rewards
      FROM pool_rewards
      WHERE chain = ?
      GROUP BY DATE_TRUNC('day', ts)
      ORDER BY DATE_TRUNC('day', ts)
    )
    SELECT
      date,
      daily_rewards
    FROM daily_data
    ORDER BY date;
  `, [chain]);

  return result.rows.map(row => ({
    ts: row.date,
    daily_rewards: parseFloat(row.daily_rewards)
  }));
};

const getDailyPoolRewardsData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const data = await fetchDailyPoolRewardsData(chain);
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchDailyPoolRewardsData(chain);
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching daily pool rewards data: ' + error.message);
  }
};

const getDailyPoolRewardsSummaryStats = async (chain) => {
  try {
    const data = await getDailyPoolRewardsData(chain);
    const dailyValues = data[chain].map(item => item.daily_rewards);

    if (dailyValues.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(data[chain], 'daily_rewards');
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

    const standardDeviation = calculateStandardDeviation(dailyValues);

    const current = latestData.daily_rewards;
    const ath = Math.max(...dailyValues);
    const atl = Math.min(...dailyValues);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? value24h.daily_rewards : null),
      delta_7d: calculateDelta(current, value7d ? value7d.daily_rewards : null),
      delta_28d: calculateDelta(current, value28d ? value28d.daily_rewards : null),
      delta_ytd: calculateDelta(current, valueYtd ? valueYtd.daily_rewards : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching daily pool rewards summary stats: ' + error.message);
  }
};

module.exports = {
  getLatestPoolRewardsData,
  getAllPoolRewardsData,
  fetchAndInsertAllPoolRewardsData,
  fetchAndUpdateLatestPoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
  getDailyPoolRewardsSummaryStats,
};
