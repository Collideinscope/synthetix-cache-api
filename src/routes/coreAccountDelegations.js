const express = require('express');
const router = express.Router();
const {
  getStakerCount,
  getCumulativeUniqueStakers,
  getCoreAccountDelegationsDataByAccount,
  getUniqueStakersSummaryStats,
  getDailyNewUniqueStakers,
} = require('../services/coreAccountDelegationsService');
const { CHAINS } = require('../helpers');

const validateParameters = (chain, collateralType) => {
  if (!collateralType) {
    throw new Error("collateralType is required");
  }
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/stakers', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const stakerCount = await getStakerCount(chain, collateralType);
    if (Object.values(stakerCount).every(count => count === 0)) {
      return res.status(404).json({ error: 'No staker count data found' });
    }
    return res.json(stakerCount);
  } catch (error) {
    console.error('Error in /stakers route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/stakers/cumulative', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const result = await getCumulativeUniqueStakers(chain, collateralType);
    if (Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'No cumulative unique stakers data found' });
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /stakers/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/account/:accountId', async (req, res) => {
  try {
    const { accountId } = req.params;
    const result = await getCoreAccountDelegationsDataByAccount(accountId);
    if (!result.length) {
      return res.status(404).json({ error: 'Core account delegations data not found for this account ID' });
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /account route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/stakers/summary', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const stats = await getUniqueStakersSummaryStats(chain, collateralType);
    if (Object.keys(stats).length === 0) {
      return res.status(404).json({ error: 'No summary stats found' });
    }
    res.json(stats);
  } catch (error) {
    console.error('Error in /stakers/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/stakers/daily', async (req, res) => {
  try {
    const { chain, collateralType } = req.query;
    validateParameters(chain, collateralType);
    const result = await getDailyNewUniqueStakers(chain, collateralType);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).json({ error: 'Daily new unique stakers data not found for the specified chain' });
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).json({ error: 'Daily new unique stakers data not found' });
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /stakers/daily route:', error);
    return res.status(400).json({ error: error.message });
  }
});

module.exports = router;