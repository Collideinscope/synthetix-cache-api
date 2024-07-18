const express = require('express');
const router = express.Router();
const {
  getLatestPerpStatsData,
  getAllPerpStatsData,
  getCumulativeVolumeSummarystats,
  getCumulativeExchangeFeesSummaryData,
  getCumulativeCollectedFeesSummaryData,
} = require('../services/perpStatsService');
const { CHAINS } = require('../helpers');

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestPerpStatsData(chain);

    if (!result.length) {
      return res.status(404).send('Perp stats data not found');
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
    const result = await getAllPerpStatsData(chain);

    if (!result.length) {
      return res.status(404).send('Perp stats data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-volume/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getCumulativeVolumeSummarystats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/cumulative-exchange-fees/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getCumulativeExchangeFeesSummaryData(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/cumulative-collected-fees/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getCumulativeCollectedFeesSummaryData(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});


module.exports = router;
