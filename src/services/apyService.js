const { knex, troyDBKnex } = require('../config/db');

const fetchAndInsertAllAPYData = async () => {
  try {
    // Fetch initial 
    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM base_mainnet.fct_core_apr
      ORDER BY ts DESC;
    `);

    // add 'base' as chain
    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = 'base';

      return row;
    });
    
    await knex('apy').insert(dataWithChainAdded);

    console.log('APY data seeded successfully.');
  } catch (error) {
    console.error('Error seeding APY data:', error);
  }
}

const fetchAndUpdateLatestAPYData = async () => {
  try {
    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('apy').max('ts as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM base_mainnet.fct_core_apr
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log('No new data to update.');
      return;
    }

    // add 'base' as chain
    const dataWithChainAdded = newRows.rows.map(row => {
      row.chain = 'base';

      return row;
    });

    await knex('apy').insert(dataWithChainAdded);

    console.log('APY data updated successfully.');
  } catch (error) {
    console.error('Error updating APY data:', error);
  }
};

module.exports = {
  fetchAndInsertAllAPYData,
  fetchAndUpdateLatestAPYData,
};
