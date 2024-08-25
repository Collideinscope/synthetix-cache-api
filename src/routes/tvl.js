const express = require('express');
const router = express.Router();
const {
  getLatestTVLData,
  getCumulativeTVLData,
  getTVLSummaryStats,
  getDailyTVLData,
} = require('../services/tvlService');

const { CHAINS } = require('../helpers');

const validateOptionalChain = (chain) => {
  if (chain && !CHAINS.includes(chain)) {
    throw new Error("Invalid chain parameter");
  }
};

router.get('/latest', async (req, res) => {
  try {
    const { chain } = req.query;

    validateOptionalChain(chain);

    const result = await getLatestTVLData(chain);

    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('TVL data not found for the specified chain');
    }

    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('TVL data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error('Error in /tvl/latest route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/cumulative', async (req, res) => {
  try {
    const { chain } = req.query;

    validateOptionalChain(chain);

    const result = await getCumulativeTVLData(chain);

    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('TVL data not found for the specified chain');
    }

    if (!chain && Object.values(result).every(data => data.length === 0)) {
      return res.status(404).send('TVL data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error('Error in /tvl/cumulative route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/summary', async (req, res) => {
  try {
    const { chain } = req.query;

    validateOptionalChain(chain);

    const result = await getTVLSummaryStats(chain);

    if (chain && (!result[chain] || Object.keys(result[chain]).length === 0)) {
      return res.status(404).send('TVL summary not found for the specified chain');
    }

    if (!chain && Object.keys(result).every(key => Object.keys(result[key]).length === 0)) {
      return res.status(404).send('TVL summary not found');
    }

    return res.json(result);
  } catch (error) {
    console.error('Error in /tvl/summary route:', error);
    return res.status(400).json({ error: error.message });
  }
});

router.get('/daily', async (req, res) => {
  try {
    const { chain } = req.query;

    validateOptionalChain(chain);

    const result = await getDailyTVLData(chain);

    if (chain && (!result[chain] || result[chain].length === 0)) {
      return res.status(404).send('Daily TVL data not found for the specified chain');
    }

    if (!chain && Object.values(result).every(chainData => chainData.length === 0)) {
      return res.status(404).send('Daily TVL data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error('Error in /tvl/daily route:', error);
    return res.status(400).json({ error: error.message });
  }
});

module.exports = router;