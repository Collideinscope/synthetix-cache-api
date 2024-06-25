const express = require('express');
const router = express.Router();
const { getLatestPoolRewardsData, getAllPoolRewardsData } = require('../services/poolRewardsService');

router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestPoolRewardsData(chain);

    if (!result.length) {
      return res.status(404).send('Pool Rewards data not found');
    }

    // return the only value in the array (latest)
    return res.json(result); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllPoolRewardsData(chain);

    if (!result.length) {
      return res.status(404).send('Pool Rewards data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = router;
