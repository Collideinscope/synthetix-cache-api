const express = require('express');
const router = express.Router();
const { 
  getLatestAPYData, 
  getAllAPYData, 
  getAPYSummaryStats,
  getDailyAggregatedAPYData,
  getDailyAPYSummaryStats, 
} = require('../services/apyService');
const { modifyAPYDataWithTimeframes } = require('../transformers');
const { CHAINS } = require('../helpers')

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestAPYData(chain);

    if (!result.length) {
      return res.status(404).send('APY data not found');
    }

    const transformedData = modifyAPYDataWithTimeframes(result);

    return res.json(transformedData); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/all/:chain', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const result = await getAllAPYData(chain);
    if (!result[chain] || result[chain].length === 0) {
      return res.status(404).send('APY data not found for the specified chain');
    }
    const transformedData = modifyAPYDataWithTimeframes(result[chain]);
    res.json({[chain]: transformedData});
  } catch (error) {
    console.error('Error in /apy/all route:', error);
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

    const stats = await getAPYSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /apy/summary route:', error);
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
    const data = await getDailyAggregatedAPYData(chain);
    if (!data[chain] || data[chain].length === 0) {
      return res.status(404).send('Daily aggregated APY data not found for the specified chain');
    }
    res.json(data);
  } catch (error) {
    console.error('Error in /apy/daily route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily/summary/:chain', async (req, res) => {
  try {
    const { chain } = req.params;
    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }
    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }
    const stats = await getDailyAPYSummaryStats(chain);
    res.json(stats);
  } catch (error) {
    console.error('Error in /apy/daily/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
