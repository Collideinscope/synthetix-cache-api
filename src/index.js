require('dotenv').config();

const express = require('express');
const pool = require('./db');
const app = express();

const PORT = process.env.PORT || 3000;

app.get('', async (req, res) => {
  return res.send('synthetix cache api');
});

app.get('/api/apy', async (req, res) => {
  try {

  } catch (error) {
    console.error(error);

    return res.status(500).send('Server error');
  }
});

app.get('/tvl', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT ts, pool_id, collateral_type, amount, collateral_value
      FROM base_mainnet.core_vault_collateral
      ORDER BY ts DESC
      LIMIT 1;
    `);

    return res.json(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).send('Server error');
  }
});


app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});