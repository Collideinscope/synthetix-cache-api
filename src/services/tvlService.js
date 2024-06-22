const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getLatestTVLData = async (chain) => {
  try {
    let query = knex('tvl').orderBy('ts', 'desc').limit(1);

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching latest TVL data: ' + error.message);
  }
};

const getAllTVLData = async (chain) => {
  try {
    let query = knex('tvl').orderBy('ts', 'desc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all TVL data: ' + error.message);
  }
};

// initial seed
const fetchAndInsertAllTVLData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  };

  try {
    const tableName = `${chain}_mainnet.core_vault_collateral`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    // Aggregate the data to keep only the highest value per hour
    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      const hourKey = row.ts.toISOString().slice(0, 13); // Format as 'YYYY-MM-DDTHH'

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: new Date(hourKey + ':00:00Z') // Set the timestamp to the start of the hour
        };
      }
    }, {})

    const dataToInsert = Object.values(rowsAggregatedByHour);

    await knex('tvl').insert(dataToInsert);

    console.log(`TVL data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding TVL data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestTVLData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.core_vault_collateral`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('tvl').where('chain', chain).max('block_timestamp as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new TVL data to update for ${chain} chain.`);
      return;
    }

    // Process the data to keep only the highest value per hour
    const rowsAggregatedByHour = {};

    for (const row of newRows.rows) {
      const hourKey = row.ts.toISOString().slice(0, 13); // Format as 'YYYY-MM-DDTHH'
      const startOfHour = new Date(hourKey + ':00:00Z');
      
      // Check the existing entry for current hour in the cache
      const existingEntry = await knex('tvl')
        .where('chain', chain)
        .andWhere('ts', startOfHour)
        .andWhere('pool_id', row.pool_id)
        .andWhere('collateral_type', row.collateral_type)
        .first();
      
      if (!rowsAggregatedByHour[hourKey]) {
        rowsAggregatedByHour[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: startOfHour // Set the timestamp to the start of the hour
        };
      }

      // Update the entry if the current row amount is greater than the existing one
      if (!existingEntry || row.amount > existingEntry.amount
          || !rowsAggregatedByHour[hourKey] || row.amount > rowsAggregatedByHour[hourKey]
      ) {
        rowsAggregatedByHour[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: startOfHour // Set the timestamp to the start of the hour
        };
      }
    }

    const dataToInsert = Object.values(rowsAggregatedByHour);

    // Insert or update the data in the cache
    for (const data of dataToInsert) {
      await knex('tvl')
        .insert(data)
        .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
        .merge();
    }

    console.log(`TVL data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating TVL data for ${chain} chain:`, error);
  }
};

module.exports = {
  getLatestTVLData,
  getAllTVLData,
  fetchAndInsertAllTVLData,
  fetchAndUpdateLatestTVLData,
};
