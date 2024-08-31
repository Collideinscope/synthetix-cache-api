const express = require('express');
const router = express.Router();
const {
  getLatestAPYData,
  getAllAPYData,
  getAPYSummaryStats,
  getDailyAggregatedAPYData,
} = require('../services/apyService');
const { modifyAPYDataWithTimeframes } = require('../transformers');
const { CHAINS } = require('../helpers');

const validateParameters = (chain, collateralType) => {
  if (!collateralType) {
    throw new Error("collateralType is required");
  }
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

const transformDataByChain = (data) => {
  return Object.entries(data).reduce((acc, [chain, chainData]) => {
    acc[chain] = modifyAPYDataWithTimeframes(chainData);
    return acc;
  }, {});
};

router.get('/latest', async (req, res) => {
  try {
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
    const result = await getLatestAPYData(chain, collateralType);
    if (Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'APY data not found' });
    }
console.log(result)
    const transformedData = transformDataByChain(result);
    return res.json(transformedData);
  } catch (error) {
    console.error('Error in /apy/latest route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/all', async (req, res) => {
  try {
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
    const result = await getAllAPYData(chain, collateralType);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).json({ error: 'APY data not found for the specified chain' });
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'APY data not found' });
    }
    const transformedData = transformDataByChain(result);
    res.json(transformedData);
  } catch (error) {
    console.error('Error in /apy/all route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/summary', async (req, res) => {
  try {
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
    const stats = await getAPYSummaryStats(chain, collateralType);
    if (Object.keys(stats).length === 0) {
      return res.status(404).json({ error: 'APY summary stats not found' });
    }
    res.json(stats);
  } catch (error) {
    console.error('Error in /apy/summary route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/daily', async (req, res) => {
  try {
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
    const data = await getDailyAggregatedAPYData(chain, collateralType);
    if (chain && (!data[chain] || data[chain].length === 0)) {
      return res.status(404).json({ error: 'Daily aggregated APY data not found for the specified chain' });
    }
    if (!chain && Object.values(data).every(chainData => chainData.length === 0)) {
      return res.status(404).json({ error: 'Daily aggregated APY data not found' });
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /apy/daily route:', error);
    return res.status(400).json({ error: error.message });
  }
});

module.exports = router;