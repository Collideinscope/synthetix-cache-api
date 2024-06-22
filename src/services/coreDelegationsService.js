const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getLatestCoreDelegationsData = async (chain) => {
  try {
    let query = knex('delegations').orderBy('ts', 'desc').limit(1);

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching latest delegations data: ' + error.message);
  }
};

const getAllCoreDelegationsData = async (chain) => {
  try {
    let query = knex('delegations').orderBy('ts', 'desc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all delegations data: ' + error.message);
  }
};

// Initial seed
const fetchAndInsertAllCoreDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_pool_delegation`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('delegations').insert(dataWithChainAdded);

    console.log(`Delegations data seeded successfully for ${chain}.`);
  } catch (error) {
    console.error('Error seeding delegations data:', error);
  }
};

const fetchAndUpdateLatestCoreDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_pool_delegation`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('delegations').where('chain', chain).max('ts as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new delegations data to update for ${chain}.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('delegations').insert(dataWithChainAdded);

    console.log(`Delegations data updated successfully for ${chain}.`);
  } catch (error) {
    console.error(`Error updating delegations data for ${chain}:`, error);
  }
};

module.exports = {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData,
  fetchAndInsertAllCoreDelegationsData,
  fetchAndUpdateLatestCoreDelegationsData,
};
