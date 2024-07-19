const express = require('express');
const router = express.Router();
const { 
  getAllPerpMarketHistoryData, 
  getDailyOIData,
  getDailyOISummaryStats,
} = require('../services/perpMarketHistoryService');
const { CHAINS } = require('../helpers');

router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllPerpMarketHistoryData(chain);

    if (!result.length) {
      return res.status(404).send('Perp market history data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-oi/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getDailyOIData(chain);

    if (!data.length) {
      return res.status(404).send('Cumulative OI data not found');
    }

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-market-history/cumulative-oi route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/cumulative-oi/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getDailyOISummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-market-history/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
