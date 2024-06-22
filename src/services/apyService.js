const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getLatestAPYData = async (chain) => {
  try {
    let query = knex('apy').orderBy('ts', 'desc').limit(1);

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching latest APY data: ' + error.message);
  }
};

const getAllAPYData = async (chain) => {
  try {
    let query = knex('apy').orderBy('ts', 'desc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all APY data: ' + error.message);
  }
};

const fetchAndInsertAllAPYData = async (chain) => {
  try {
    const tableName = `${chain}_mainnet.fct_core_apr`;

    // Fetch initial 
    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = chain;
      return row;
    });
    
    await knex('apy').insert(dataWithChainAdded);

    console.log(`APY data seeded successfully for ${chain}.`);
  } catch (error) {
    console.error('Error seeding APY data:', error);
  }
};

const fetchAndUpdateLatestAPYData = async (chain) => {
  try {
    const tableName = `${chain}_mainnet.fct_core_apr`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('apy').where('chain', chain).max('ts as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new data to update for ${chain}.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('apy').insert(dataWithChainAdded);

    console.log(`APY data updated successfully for ${chain}.`);  
  } catch (error) {
    console.error('Error updating APY data:', error);
  }
};

module.exports = {
  getLatestAPYData,
  getAllAPYData,
  fetchAndInsertAllAPYData,
  fetchAndUpdateLatestAPYData,
};
