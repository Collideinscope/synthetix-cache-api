const express = require('express');
const router = express.Router();
const {
  getStakerCount,
  getCumulativeUniqueStakers,
  getLatestCoreAccountDelegationsDataOrderedByAccount,
  getCoreAccountDelegationsDataByAccount,
  getAllCoreAccountDelegationsData,
  getUniqueStakersSummaryStats,
  getDailyNewUniqueStakers,
} = require('../services/coreAccountDelegationsService');
const { CHAINS } = require('../helpers');

router.get('/staker-count/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const stakerCount = await getStakerCount(chain);

    return res.json(stakerCount);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-unique-stakers/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getCumulativeUniqueStakers(chain);

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllCoreAccountDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/account/:accountId', async (req, res) => {
  try {
    const { accountId } = req.params;
    const result = await getCoreAccountDelegationsDataByAccount(accountId);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found for this account ID');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/ordered-by-account/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestCoreAccountDelegationsDataOrderedByAccount(chain);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-unique-stakers/summary/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getUniqueStakersSummaryStats(chain);

    res.json(stats);
  } catch (error) {
    console.error('Error in /tvl/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-new-unique-stakers/:chain', async (req, res) => {
  try {
    const { chain } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const result = await getDailyNewUniqueStakers(chain);

    if (!result.length) {
      return res.status(404).send('Daily new unique stakers data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error('Error in /daily-new-unique-stakers route:', error);
    return res.status(500).json({ error: error.message });
  }
});

module.exports = router;
