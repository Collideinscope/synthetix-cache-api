const express = require('express');
const { getLatestAPYData, getAllAPYData } = require('./services/apyService');
const { modifyAPYDataWithTimeframes } = require('./transformers/index');

const app = express();

app.get('/', async (req, res) => {
  try {
    return res.send('Synthetix V3 Cache API'); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/apy/latest', async (req, res) => {
  try {
    const result = await getLatestAPYData();

    if (!result.length) {
      return res.status(404).send('APY data not found');
    }

    const transformedData = modifyAPYDataWithTimeframes(result);

    // return the only value in the arrray (latest)
    return res.json(transformedData[0]); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/apy/all', async (req, res) => {
  try {
    const result = await getAllAPYData();

    if (!result.length) {
      return res.status(404).send('APY data not found');
    }

    const transformedData = modifyAPYDataWithTimeframes(result);

    return res.json(transformedData);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = app;
