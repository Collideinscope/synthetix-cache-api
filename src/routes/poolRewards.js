const express = require('express');
const router = express.Router();
const {
  getLatestPoolRewardsData,
  getCumulativePoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
} = require('../services/poolRewardsService');
const { CHAINS } = require('../helpers');

const validateParameters = (chain, collateralType) => {
  if (!collateralType) {
    throw new Error("collateralType is required");
  }
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/latest', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const result = await getLatestPoolRewardsData(chain, collateralType);
    if (Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'Pool rewards data not found' });
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /pool-rewards/latest route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/cumulative', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const result = await getCumulativePoolRewardsData(chain, collateralType);
    if (Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'Pool rewards data not found' });
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /pool-rewards/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/summary', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const stats = await getPoolRewardsSummaryStats(chain, collateralType);
    if (Object.values(stats).every(data => Object.keys(data).length === 0)) {
      return res.status(404).json({ error: 'Pool rewards summary stats not found' });
    }
    res.json(stats);
  } catch (error) {
    console.error('Error in /pool-rewards/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/daily', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const data = await getDailyPoolRewardsData(chain, collateralType);
    if (Object.values(data).every(chainData => chainData.length === 0)) {
      return res.status(404).json({ error: 'Daily pool rewards data not found' });
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /pool-rewards/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

module.exports = router;