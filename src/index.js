require('dotenv').config();

const express = require('express');
const pool = require('./db');
const app = express();

const PORT = process.env.PORT || 3000;

app.get('/api/apy', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM your_apy_view;');

    return res.json(result.rows);
  } catch (error) {
    console.error(error);

    return res.status(500).send('Server error');
  }
});

app.get('/api/tvl', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM your_tvl_view;');

    res.json(result.rows);
  } catch (error) {
    console.error(error);
    
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});