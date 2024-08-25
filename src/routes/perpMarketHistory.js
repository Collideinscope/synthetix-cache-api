const express = require('express');
const router = express.Router();
const {
  getOpenInterestData,
  getOpenInterestSummaryStats,
  getDailyOpenInterestChangeData,
} = require('../services/perpMarketHistoryService');
const { CHAINS } = require('../helpers');

const validateOptionalChain = (chain) => {
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/open-interest/daily', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getOpenInterestData(chain);
    res.json(data);
  } catch (error) {
    console.error('Error in /open-interest/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/open-interest/daily-change', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getDailyOpenInterestChangeData(chain);
    res.json(data);
  } catch (error) {
    console.error('Error in /open-interest/daily/change route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/open-interest/daily/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getOpenInterestSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /open-interest/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

module.exports = router;