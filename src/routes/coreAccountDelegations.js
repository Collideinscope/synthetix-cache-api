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
  getDailyNewUniqueStakersSummary,
} = require('../services/coreAccountDelegationsService');
const { CHAINS } = require('../helpers');

router.get('/staker-count/:chain?/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;
    const stakerCount = await getStakerCount(chain, collateralType);

    return res.json(stakerCount);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-unique-stakers/:chain?/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;
    const result = await getCumulativeUniqueStakers(chain, collateralType);

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/all/:chain?/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;
    const result = await getAllCoreAccountDelegationsData(chain, collateralType);

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

router.get('/ordered-by-account/:chain?/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;
    const result = await getLatestCoreAccountDelegationsDataOrderedByAccount(chain, collateralType);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/cumulative-unique-stakers/summary/:chain?/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getUniqueStakersSummaryStats(chain, collateralType);

    res.json(stats);
  } catch (error) {
    console.error('Error in /tvl/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/daily-new-unique-stakers/:chain/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const result = await getDailyNewUniqueStakers(chain, collateralType);

    if (!result[chain] || result[chain].length === 0) {
      return res.status(404).send('Daily new unique stakers data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error('Error in /daily-new-unique-stakers route:', error);
    return res.status(500).json({ error: error.message });
  }
});

router.get('/daily-new-unique-stakers/summary/:chain/:collateralType?', async (req, res) => {
  try {
    const { chain, collateralType } = req.params;

    if (!chain) {
      return res.status(400).json({ error: "Chain parameter is required" });
    }

    if (!CHAINS.includes(chain)) {
      return res.status(400).json({ error: "Invalid chain parameter" });
    }

    const stats = await getDailyNewUniqueStakersSummary(chain, collateralType);
    
    res.json(stats);
  } catch (error) {
    console.error('Error in /daily-new-unique-stakers/summary route:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;