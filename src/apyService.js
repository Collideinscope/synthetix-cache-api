const { sourcePool, cachePool } = require('./db');

const updateAPYData = async () => {
  try {
    // Get updated figure from external
    const result = await sourcePool.query(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d      
      FROM base_mainnet.fct_core_apr
      ORDER BY ts DESC
      LIMIT 1;
      `);

    const row = result.rows[0];

    // update cache db
    await cachePool.query(`
      INSERT INTO apy (ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (pool_id, collateral_type) DO UPDATE
      SET ts = EXCLUDED.ts, collateral_value = EXCLUDED.collateral_value, apy_24h = EXCLUDED.apy_24h, apy_7d = EXCLUDED.apy_7d, apy_28d = EXCLUDED.apy_28d;
      `, [row.ts, row.pool_id, row.collateral_type, row.collateral_value, row.apy_24h, row.apy_7d, row.apy_28d]);

    console.log('APY data fetched and inserted/updated successfully.');
  } catch (error) {
    console.error('Error updating APY data:', error);
  }
};

module.exports = {
  updateAPYData,
};
