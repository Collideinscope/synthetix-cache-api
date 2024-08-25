const express = require('express');
const router = express.Router();
const {
  getLatestCoreDelegationsData,
  getCumulativeCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
} = require('../services/coreDelegationsService');
const { CHAINS } = require('../helpers');

const validateOptionalChain = (chain) => {
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/latest', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const result = await getLatestCoreDelegationsData(chain);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('Core delegations data not found for the specified chain');
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('Core delegations data not found');
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /core-delegations/latest route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/cumulative', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const result = await getCumulativeCoreDelegationsData(chain);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('Core delegations data not found for the specified chain');
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('Core delegations data not found');
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /core-delegations/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getCoreDelegationsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /core-delegations/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/daily', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getDailyCoreDelegationsData(chain);
    if (chain && (!data[chain] || data[chain].length === 0)) {
      return res.status(404).send('Daily core delegations data not found for the specified chain');
    }
    if (!chain && Object.values(data).every(chainData => chainData.length === 0)) {
      return res.status(404).send('Daily core delegations data not found');
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /core-delegations/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/daily/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getDailyCoreDelegationsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /core-delegations/daily/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

module.exports = router;