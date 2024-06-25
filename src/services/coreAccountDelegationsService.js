const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const getStakerCount = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the unique staker count for the specific chain
      const result = await knex('core_account_delegations')
        .where('chain', chain)
        .countDistinct('account_id as staker_count');

      return { [chain]: result[0].staker_count };
    }

    // Fetch the unique staker count for each chain
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('core_account_delegations')
          .where('chain', chain)
          .countDistinct('account_id as staker_count');

        return { [chain]: result[0].staker_count };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching staker count: ' + error.message);
  }
};

const getLatestCoreAccountDelegationsDataOrderedByAccount = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for each account 
      const result = await knex('core_account_delegations')
        .where('chain', chain)
        .distinctOn('account_id')
        .orderBy('account_id')
        .orderBy('ts', 'desc');

      return result;
    }

    // Fetch the latest value for each account in each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('core_account_delegations')
          .where('chain', chain)
          .distinctOn('account_id')
          .orderBy('account_id')
          .orderBy('ts', 'desc');

        return result;
      })
    );

    return results.flat().filter(Boolean); 
  } catch (error) {
    throw new Error('Error fetching latest core account delegations data: ' + error.message);
  }
};

const getCoreAccountDelegationsDataByAccount = async (accountId) => {
  try {
    let query = knex('core_account_delegations')
      .where('account_id', accountId)
      .orderBy('ts', 'desc');

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching core account delegations data by account: ' + error.message);
  }
};

const getAllCoreAccountDelegationsData = async (chain) => {
  try {
    let query = knex('core_account_delegations').orderBy('ts', 'desc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all core account delegations data: ' + error.message);
  }
};

// Initial seed
const fetchAndInsertAllCoreAccountDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_account_delegation`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, account_id, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('core_account_delegations')
      .insert(dataWithChainAdded)
      .onConflict(['chain', 'account_id', 'pool_id', 'collateral_type'])
      .merge({
        ts: knex.raw('EXCLUDED.ts'),
        amount_delegated: knex.raw('EXCLUDED.amount_delegated')
      });

    console.log(`Core account delegations data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding core account delegations data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestCoreAccountDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.fct_core_account_delegation`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('core_account_delegations')
      .where('chain', chain)
      .orderBy('ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.ts;

    // Fetch new data starting from the last timestamp
    const newRows = await troyDBKnex.raw(`
      SELECT ts, account_id, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new core account delegations data to update for ${chain} chain.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => ({
      ...row,
      chain
    }));

    await knex('core_account_delegations')
      .insert(dataWithChainAdded)
      .onConflict(['chain', 'account_id', 'pool_id', 'collateral_type'])
      .merge({
        ts: knex.raw('EXCLUDED.ts'),
        amount_delegated: knex.raw('EXCLUDED.amount_delegated')
      });
      
    console.log(`Core account delegations data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating core account delegations data for ${chain} chain:`, error);
  }
};

module.exports = {
  getStakerCount,
  getLatestCoreAccountDelegationsDataOrderedByAccount,
  getCoreAccountDelegationsDataByAccount,
  getAllCoreAccountDelegationsData,
  fetchAndInsertAllCoreAccountDelegationsData,
  fetchAndUpdateLatestCoreAccountDelegationsData,
};
