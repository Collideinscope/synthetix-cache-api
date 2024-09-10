const express = require('express');
const router = express.Router();
const {
  getCumulativePoolRewardsData,
  getPoolRewardsSummaryStats,
  getDailyPoolRewardsData,
} = require('../services/poolRewardsService');
const { CHAINS } = require('../helpers');

const SERVICE_CHAINS = CHAINS['pool_rewards'];

const validateParameters = (chain, collateralType) => {
  if (!collateralType) {
    throw new Error("collateralType is required");
  }
  if (chain && !SERVICE_CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/cumulative', async (req, res) => {
  try {
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
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
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
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
    let { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    chain = chain?.toLowerCase();
    collateralType = collateralType?.toLowerCase();
    
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