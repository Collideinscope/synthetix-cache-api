const express = require('express');
const router = express.Router();
const { 
  getLatestPoolRewardsData, 
  getAllPoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
  getDailyPoolRewardsSummaryStats,
} = require('../services/poolRewardsService');

const { CHAINS } = require('../helpers')

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestPoolRewardsData(chain);
    if (chain) {
      if (!result[chain] || result[chain].length === 0) {
        return res.status(404).send('Pool rewards data not found for the specified chain');
      }
    } else {
      if (Object.values(result).every(data => data.length === 0)) {
        return res.status(404).send('Pool rewards data not found');
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
    const result = await getAllPoolRewardsData(chain);
    if (chain) {
      if (!result[chain] || result[chain].length === 0) {
        return res.status(404).send('Pool rewards data not found for the specified chain');
      }
    } else {
      if (Object.values(result).every(data => data.length === 0)) {
        return res.status(404).send('Pool rewards data not found');
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
    const stats = await getPoolRewardsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /pool-rewards/summary route:', error);
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
    const data = await getDailyPoolRewardsData(chain);
    if (!data[chain] || data[chain].length === 0) {
      return res.status(404).send('Daily pool rewards data not found for the specified chain');
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /pool-rewards/daily route:', error);
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
    const stats = await getDailyPoolRewardsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /pool-rewards/daily/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
