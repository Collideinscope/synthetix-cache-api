const express = require('express');
const router = express.Router();
const {
  getCumulativeVolumeSummaryStats,
  getCumulativeExchangeFeesSummaryStats,
  getCumulativeVolumeData,
  getCumulativeExchangeFeesData,
  getDailyVolumeData,
  getDailyExchangeFeesData,
} = require('../services/perpStatsService');
const { CHAINS } = require('../helpers');

const validateOptionalChain = (chain) => {
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/volume/cumulative', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getCumulativeVolumeData(chain);
    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/volume/cumulative route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/volume/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getCumulativeVolumeSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/volume/cumulative/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/volume/daily', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getDailyVolumeData(chain);
    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/volume/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/exchange-fees/cumulative', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getCumulativeExchangeFeesData(chain);
    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/exchange-fees/cumulative route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/exchange-fees/summary', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const stats = await getCumulativeExchangeFeesSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/exchange-fees/cumulative/summary route:', error);
    res.status(400).json({ error: error.message });
  }
});

router.get('/exchange-fees/daily', async (req, res) => {
  try {
    const { chain } = req.query;
    validateOptionalChain(chain);
    const data = await getDailyExchangeFeesData(chain);
    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/exchange-fees/daily route:', error);
    res.status(400).json({ error: error.message });
  }
});

module.exports = router;