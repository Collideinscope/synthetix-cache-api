const { sourcePool, cachePool } = require('./db');

const updateTVLData = async () => {
  try {
    // Get updated figure from external
    const result = await sourcePool.query(`
      SELECT ts, pool_id, collateral_type, amount, collateral_value
      FROM base_mainnet.core_vault_collateral
      ORDER BY ts DESC
      LIMIT 1;
    `);

    const row = result.rows[0];

    // Insert to cache db
    await cachePool.query(`
      INSERT INTO tvl (ts, pool_id, collateral_type, amount, collateral_value)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (ts) DO UPDATE
      SET amount = EXCLUDED.amount, collateral_value = EXCLUDED.collateral_value;    
    `, [row.ts, row.pool_id, row.collateral_type, row.amount, row.collateral_value])
    ;

    console.log('TVL data fetched and inserted/updated successfully.');
  } catch (error) {
    console.error('Error updating TVL data:', error);
  }
};

module.exports = {
  updateTVLData,
};
