const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getLatestTVLData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specific chain
      const result = await knex('tvl')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    }

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('tvl')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean); // Filter out any undefined results
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

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.core_vault_collateral`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    // Aggregate and transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`; // Format as 'YYYY-MM-DDTHH_poolId_collateralType_chain'

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
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

    await knex('tvl')
      .insert(dataToInsert)
      .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
      .merge({
        amount: knex.raw('GREATEST(tvl.amount, excluded.amount)'),
        block_ts: knex.raw('CASE WHEN tvl.amount <= excluded.amount THEN excluded.block_ts ELSE tvl.block_ts END'),
      });

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

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.core_vault_collateral`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('tvl')
      .where('chain', chain)
      .orderBy('block_ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.block_ts;

    // Fetch new data starting from the last timestamp
    const newRows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new TVL data to update for ${chain} chain.`);
      return;
    }

    // Transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = newRows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`; // Format as 'YYYY-MM-DDTHH_poolId_collateralType_chain'

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
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
      await knex('tvl')
        .insert(dataToInsert)
        .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
        .merge({
          amount: knex.raw('GREATEST(tvl.amount, excluded.amount)'),
          block_ts: knex.raw('CASE WHEN tvl.amount <= excluded.amount THEN excluded.block_ts ELSE tvl.block_ts END'),
        });
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
