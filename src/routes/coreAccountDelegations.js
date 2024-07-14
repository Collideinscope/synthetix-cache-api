const express = require('express');
const router = express.Router();
const {
  getStakerCount,
  getCumulativeUniqueStakers,
  getLatestCoreAccountDelegationsDataOrderedByAccount,
  getCoreAccountDelegationsDataByAccount,
  getAllCoreAccountDelegationsData,
} = require('../services/coreAccountDelegationsService');

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

module.exports = router;
