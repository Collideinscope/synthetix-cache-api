require('dotenv').config();

const cors = require('cors');
const express = require('express');
const { knex } = require('./db');
const { TIMEFRAMES, calculateAPY } = require('./helpers');
const { CLIENT_URL } = require('./config');

const app = express();

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
    const result = await knex('apy_timeseries').orderBy('ts', 'desc').limit(1);
    const apyData = result[0];
    
    if (!apyData) {
      return res.status(404).send('APY data not found');
    }

    const apyPeriods = {
      apy_24h: parseFloat(apyData.apy_24h),
      apy_7d: parseFloat(apyData.apy_7d),
      apy_28d: parseFloat(apyData.apy_28d),
    };

    const apyValues = Object.entries(apyPeriods).reduce((acc, [period, apy]) => {
      acc[period] = TIMEFRAMES.reduce((result, timeframe) => {
        result[timeframe] = calculateAPY(apy, timeframe);
        return result;
      }, {});

      return acc;
    }, {});

    const apyRes = {
      ts: apyData.ts,
      pool_id: apyData.pool_id,
      collateral_type: apyData.collateral_type,
      collateral_value: apyData.collateral_value,
      apys: apyValues,
    };

    return res.json(apyRes);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/tvl', async (req, res) => {
  try {
    const tvl = await knex('tvl').orderBy('ts', 'desc').limit(1);
    return res.json(tvl[0]);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = app;
