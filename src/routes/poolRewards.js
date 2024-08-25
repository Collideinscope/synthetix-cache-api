const express = require('express');
const router = express.Router();
const {
  getLatestPoolRewardsData,
  getCumulativePoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
} = require('../services/poolRewardsService');
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
    const result = await getLatestPoolRewardsData(chain);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('Pool rewards data not found for the specified chain');
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('Pool rewards data not found');
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /pool-rewards/latest route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/cumulative', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const result = await getCumulativePoolRewardsData(chain);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('Pool rewards data not found for the specified chain');
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('Pool rewards data not found');
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /pool-rewards/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getPoolRewardsSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /pool-rewards/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/daily', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getDailyPoolRewardsData(chain);
    if (chain && (!data[chain] || data[chain].length === 0)) {
      return res.status(404).send('Daily pool rewards data not found for the specified chain');
    }
    if (!chain && Object.values(data).every(chainData => chainData.length === 0)) {
      return res.status(404).send('Daily pool rewards data not found');
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /pool-rewards/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

module.exports = router;