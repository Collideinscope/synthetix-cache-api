const express = require('express');
const router = express.Router();
const {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData
} = require('../services/coreDelegationsService');

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

module.exports = router;
