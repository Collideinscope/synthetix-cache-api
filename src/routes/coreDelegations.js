const express = require('express');
const router = express.Router();
const {
  getCumulativeCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
} = require('../services/coreDelegationsService');
const { CHAINS } = require('../helpers');

const validateParameters = (chain, collateralType) => {
  if (!collateralType) {
    throw new Error("collateralType is required");
  }
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/cumulative', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const result = await getCumulativeCoreDelegationsData(chain, collateralType);
    if (Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'Core delegations data not found' });
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /core-delegations/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/summary', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const stats = await getCoreDelegationsSummaryStats(chain, collateralType);
    if (Object.values(stats).every(data => Object.keys(data).length === 0)) {
      return res.status(404).json({ error: 'Core delegations summary stats not found' });
    }
    res.json(stats);
  } catch (error) {
    console.error('Error in /core-delegations/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/daily', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const data = await getDailyCoreDelegationsData(chain, collateralType);
    if (Object.values(data).every(chainData => chainData.length === 0)) {
      return res.status(404).json({ error: 'Daily core delegations data not found' });
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /core-delegations/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

module.exports = router;