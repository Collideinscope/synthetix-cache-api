const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getLatestCoreDelegationsData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specific chain
      const result = await knex('core_delegations')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    }

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('core_delegations')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean); // Filter out any undefined results
  } catch (error) {
    throw new Error('Error fetching latest core delegations data: ' + error.message);
  }
};

const getAllCoreDelegationsData = async (chain) => {
  try {
    let query = knex('core_delegations').orderBy('ts', 'desc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all core delegations data: ' + error.message);
  }
};

// Initial seed
const fetchAndInsertAllCoreDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_pool_delegation`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    // Aggregate the data to keep only the highest value per hour
    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      // Soft constraints: chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}`;

      if (!acc[hourKey]) {
        console.log(`Adding new entry for key: ${hourKey} with row:`, row);
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // Chain added
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z') // Set the timestamp to the start of the hour
        };
      } else if (row.amount_delegated > acc[hourKey].amount_delegated) {
        console.log(`Updating entry for key: ${hourKey} with row:`, row);
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // Chain added
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z') // Set the timestamp to the start of the hour
        };
      } else {
        console.log(`Skipping entry for key: ${hourKey} with row:`, row);
      }

      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    await knex('core_delegations').insert(dataToInsert);

    console.log(`Core delegations data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding core delegations data for ${chain} chain:`, error);
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
    const lastTimestampResult = await knex('core_delegations').where('chain', chain).max('block_ts as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new core delegations data to update for ${chain}.`);
      return;
    }

    // Process the data to keep only the highest value per hour
    const rowsAggregatedByHour = {};

    for (const row of newRows.rows) {
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}`;
      
      // Check the existing entry for current hour in the cache
      // soft contraints chain, ts, pool_id, collateral_type
      const existingEntry = await knex('core_delegations')
        .where('chain', chain)
        .andWhere('ts', startOfHour)
        .andWhere('pool_id', row.pool_id)
        .andWhere('collateral_type', row.collateral_type)
        .first();

      // if there's an existing entry
      if (existingEntry) {
        // Update value right away if current row value is greater
        if (row.amount_delegated > existingEntry.amount_delegated) {
          await knex('core_delegations')
            .where({ id: existingEntry.id })
            .update({
              amount_delegated: row.amount_delegated,
              block_ts: row.ts,
            });
        }
        // Considered value against existing entry, move on to next row
        continue;
      }

      // Update the entry if needed
      if (!rowsAggregatedByHour[hourKey] || row.amount_delegated > rowsAggregatedByHour[hourKey].amount_delegated) {
        rowsAggregatedByHour[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: startOfHour // Set the timestamp to the start of the hour
        };
      }
    }

    const dataToInsert = Object.values(rowsAggregatedByHour);

    if (dataToInsert.length > 0) {
      await knex('core_delegations').insert(dataToInsert);
    }

    console.log(`Core delegations data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating core delegations data for ${chain} chain:`, error);
  }
};

module.exports = {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData,
  fetchAndInsertAllCoreDelegationsData,
  fetchAndUpdateLatestCoreDelegationsData,
};
