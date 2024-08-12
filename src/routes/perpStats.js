const express = require('express');
const router = express.Router();
const {
  getLatestPerpStatsData,
  getAllPerpStatsData,
  getCumulativeVolumeSummaryStats,
  getCumulativeExchangeFeesSummaryStats,
  getCumulativeCollectedFeesSummaryStats,
  getCumulativeVolumeData,
  getCumulativeExchangeFeesData,
  getCumulativeCollectedFeesData,
  getDailyVolumeData,
  getDailyCollectedFeesData,
  getDailyExchangeFeesData,
  getDailyVolumeSummaryStats,
  getDailyExchangeFeesSummaryStats,
  getDailyCollectedFeesSummaryStats,
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

    const stats = await getCumulativeVolumeSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/cumulative-volume/summary route:', error);
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

    const stats = await getCumulativeExchangeFeesSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/cumulative-exchange-fees/summary route:', error);
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

    const stats = await getCumulativeCollectedFeesSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/cumulative-collected-fees/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/cumulative-volume/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getCumulativeVolumeData(chain);

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/cumulative-volume route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/cumulative-exchange-fees/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getCumulativeExchangeFeesData(chain);

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/cumulative-exchange-fees route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/cumulative-collected-fees/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getCumulativeCollectedFeesData(chain);

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/cumulative-collected-fees route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-volume/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getDailyVolumeData(chain);

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/daily-volume route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-exchange-fees/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getDailyExchangeFeesData(chain);

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/daily-exchange-fees route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-collected-fees/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const data = await getDailyCollectedFeesData(chain);

    res.json(data);
  } catch (error) {
    console.error('Error in /perp-stats/daily-collected-fees route:', error);
    res.status(500).json({ error: error.message });
  }
});


router.get('/daily-volume/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getDailyVolumeSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/daily-volume/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-exchange-fees/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getDailyExchangeFeesSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/daily-exchange-fees/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-collected-fees/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getDailyCollectedFeesSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /perp-stats/daily-collected-fees/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
