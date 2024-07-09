const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getLatestAPYData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specified chain
      const result = await knex('apy')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    } 

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('apy')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean); // Filter out any undefined results
  } catch (error) {
    throw new Error('Error fetching latest APY data: ' + error.message);
  }
};

const getAllAPYData = async (chain) => {
  try {
    let query = knex('apy').orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all APY data: ' + error.message);
  }
};

// initial seed
const fetchAndInsertAllAPYData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  };

  try {
    const tableName = `prod_${chain}_mainnet.fct_core_apr_${chain}_mainnet`;

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
    
    // Insert and handle conflicts
    await knex('apy')
      .insert(dataWithChainAdded)
      .onConflict(['ts', 'pool_id', 'collateral_type', 'chain'])
      .merge({
        collateral_value: knex.raw('excluded.collateral_value'),
        apy_24h: knex.raw('excluded.apy_24h'),
        apy_7d: knex.raw('excluded.apy_7d'),
        apy_28d: knex.raw('excluded.apy_28d'),
        ts: knex.raw('excluded.ts'), 
      });

    console.log(`APY data seeded successfully for ${chain}.`);
  } catch (error) {
    console.error('Error seeding APY data:', error);
  }
};

const fetchAndUpdateLatestAPYData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  };

  try {
    const tableName = `prod_${chain}_mainnet.fct_core_apr_${chain}_mainnet`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('apy').where('chain', chain).max('ts as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new APY data to update for ${chain}.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    // Insert and handle conflicts
    await knex('apy')
      .insert(dataWithChainAdded)
      .onConflict(['ts', 'pool_id', 'collateral_type', 'chain'])
      .merge({
        collateral_value: knex.raw('excluded.collateral_value'),
        apy_24h: knex.raw('excluded.apy_24h'),
        apy_7d: knex.raw('excluded.apy_7d'),
        apy_28d: knex.raw('excluded.apy_28d'),
        ts: knex.raw('excluded.ts'), // Keep the newer value
      });

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
