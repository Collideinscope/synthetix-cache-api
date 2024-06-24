const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

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
    let query = knex('pool_rewards').orderBy('ts', 'desc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all pool rewards data: ' + error.message);
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
    const tableName = `${chain}_mainnet.fct_pool_rewards_hourly`;

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
    const tableName = `${chain}_mainnet.fct_pool_rewards_hourly`;

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

module.exports = {
  getLatestPoolRewardsData,
  getAllPoolRewardsData,
  fetchAndInsertAllPoolRewardsData,
  fetchAndUpdateLatestPoolRewardsData,
};
