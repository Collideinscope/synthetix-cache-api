const express = require('express');
const router = express.Router();
const { 
  getLatestTVLData, 
  getAllTVLData, 
  getTVLSummaryStats ,
  getDailyTVLData,
  getDailyTVLSummaryStats,
} = require('../services/tvlService');
const { CHAINS } = require('../helpers')

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestTVLData(chain);
    if (chain) {
      if (!result[chain] || result[chain].length === 0) {
        return res.status(404).send('TVL data not found for the specified chain');
      }
    } else {
      if (Object.values(result).every(data => data.length === 0)) {
        return res.status(404).send('TVL data not found');
      }
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
    const result = await getAllTVLData(chain);
    if (chain) {
      if (!result[chain] || result[chain].length === 0) {
        return res.status(404).send('TVL data not found for the specified chain');
      }
    } else {
      if (Object.values(result).every(data => data.length === 0)) {
        return res.status(404).send('TVL data not found');
      }
    }
    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getTVLSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /tvl/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});


router.get('/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const stats = await getTVLSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /tvl/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily/:chain', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const data = await getDailyTVLData(chain);
    if (!data[chain] || data[chain].length === 0) {
      return res.status(404).send('Daily TVL data not found for the specified chain');
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /tvl/daily route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const stats = await getDailyTVLSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /tvl/daily/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
