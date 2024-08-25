const express = require('express');
const router = express.Router();
const {
  getCumulativeUniqueTraders,
  getUniqueTradersSummaryStats,
  getDailyNewUniqueTraders,
} = require('../services/perpAccountStatsService');
const { CHAINS } = require('../helpers');

const validateOptionalChain = (chain) => {
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/traders/cumulative', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const result = await getCumulativeUniqueTraders(chain);
    return res.json(result);
  } catch (error) {
    console.error('Error in /perp-account-stats/traders/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/traders/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getUniqueTradersSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-account-stats/traders/cumulative/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/traders/daily', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const result = await getDailyNewUniqueTraders(chain);
    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('Daily new unique traders data not found for the specified chain');
    }
    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('Daily new unique traders data not found');
    }
    return res.json(result);
  } catch (error) {
    console.error('Error in /perp-account-stats/traders/daily route:', error);
    return res.status(400).json({ error: error.message });
  }
});

module.exports = router;