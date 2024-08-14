const express = require('express');
const router = express.Router();
const {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
  getDailyCoreDelegationsSummaryStats,
} = require('../services/coreDelegationsService');

const { CHAINS } = require('../helpers')

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestCoreDelegationsData(chain);
    if (chain) {
      if (!result[chain] || result[chain].length === 0) {
        return res.status(404).send('Core delegations data not found for the specified chain');
      }
    } else {
      if (Object.values(result).every(data => data.length === 0)) {
        return res.status(404).send('Core delegations data not found');
      }
    }
    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllCoreDelegationsData(chain);
    if (chain) {
      if (!result[chain] || result[chain].length === 0) {
        return res.status(404).send('Core delegations data not found for the specified chain');
      }
    } else {
      if (Object.values(result).every(data => data.length === 0)) {
        return res.status(404).send('Core delegations data not found');
      }
    }
    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/summary/:chain', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const stats = await getCoreDelegationsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /core-delegations/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily/:chain', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const data = await getDailyCoreDelegationsData(chain);
    if (!data[chain] || data[chain].length === 0) {
      return res.status(404).send('Daily core delegations data not found for the specified chain');
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /core-delegations/daily route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily/summary/:chain', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const stats = await getDailyCoreDelegationsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /core-delegations/daily/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
