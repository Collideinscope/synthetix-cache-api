const express = require('express');
const router = express.Router();
const {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData,
  getCoreDelegationsSummaryStats,
} = require('../services/coreDelegationsService');

const { CHAINS } = require('../helpers')

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestCoreDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core delegations data not found');
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
    const result = await getAllCoreDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core delegations data not found');
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

    const stats = await getCoreDelegationsSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /core-delegations/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
