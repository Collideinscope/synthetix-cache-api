const express = require('express');
const router = express.Router();
const {
  getCumulativeUniqueTraders,
  getUniqueTradersSummaryStats,
  getAllPerpAccountStatsData,
} = require('../services/perpAccountStatsService');
const { CHAINS } = require('../helpers');


router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllPerpAccountStatsData(chain);

    if (!result.length) {
      return res.status(404).send('Perp account stats data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-unique-traders/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getCumulativeUniqueTraders(chain);

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-unique-traders/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getUniqueTradersSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-account-stats/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
