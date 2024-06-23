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

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_pool_delegation`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    // Aggregate and transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`;

      if (!acc[hourKey] || row.amount_delegated > acc[hourKey].amount_delegated) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z') // Set the timestamp to the start of the hour
        };
      }

      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    await knex('core_delegations')
      .insert(dataToInsert)
      .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
      .merge({
        amount_delegated: knex.raw('GREATEST(core_delegations.amount_delegated, excluded.amount_delegated)'),
        block_ts: knex.raw('CASE WHEN core_delegations.amount_delegated <= excluded.amount_delegated THEN excluded.block_ts ELSE core_delegations.block_ts END'),
      });

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

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_pool_delegation`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('core_delegations')
      .where('chain', chain)
      .orderBy('block_ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.block_ts;

    // Fetch new data starting from the last timestamp
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new core delegations data to update for ${chain} chain.`);
      return;
    }

    // Transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = newRows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`;

      if (!acc[hourKey] || row.amount_delegated > acc[hourKey].amount_delegated) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z') // Set the timestamp to the start of the hour
        };
      }

      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    if (dataToInsert.length > 0) {
      await knex('core_delegations')
        .insert(dataToInsert)
        .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
        .merge({
          amount_delegated: knex.raw('GREATEST(core_delegations.amount_delegated, excluded.amount_delegated)'),
          block_ts: knex.raw('CASE WHEN core_delegations.amount_delegated <= excluded.amount_delegated THEN excluded.block_ts ELSE core_delegations.block_ts END'),
        });
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
