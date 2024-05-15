require('dotenv').config();

const cors = require('cors');
const { CLIENT_URL } = require('./config');
const express = require('express');
const { cachePool } = require('./db');
const app = express();
const PORT = process.env.PORT || 3001;

const corsOptions = {
  origin: CLIENT_URL,
  optionsSuccessStatus: 200
};

app.use(cors(corsOptions));

app.get('', async (req, res) => {
  return res.send('synthetix cache api');
});

app.get('/apy', async (req, res) => {
  try {
    return res.send('apy');
  } catch (error) {
    console.error(error);

    return res.status(500).send('Server error');
  }
});

app.get('/tvl', async (req, res) => {
  try {
    // query cache db for updated value
    const tvl = await cachePool.query('SELECT * FROM tvl LIMIT 1');

    return res.json(tvl.rows[0]);  
  } catch (error) {
    console.error(error);
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});