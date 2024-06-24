const express = require('express');
const { getLatestAPYData, getAllAPYData } = require('./services/apyService');
const { getLatestTVLData, getAllTVLData } = require('./services/tvlService');
const { getLatestCoreDelegationsData, getAllCoreDelegationsData } = require('./services/coreDelegationsService');
const { getLatestPoolRewardsData, getAllPoolRewardsData } = require('./services/poolRewardsService');
const {
  getStakerCount,
  getLatestCoreAccountDelegationsDataOrderedByAccount,
  getCoreAccountDelegationsDataByAccount,
  getAllCoreAccountDelegationsData
} = reqiure('/services/coreAccountDelegationsService.js');
const { modifyAPYDataWithTimeframes } = require('./transformers/index');

const app = express();

app.get('/', async (req, res) => {
  try {
    return res.send('Synthetix V3 Cache API'); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/apy/latest/:chain?', async (req, res) => {
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

app.get('/apy/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllAPYData(chain);

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

app.get('/tvl/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestTVLData(chain);

    if (!result.length) {
      return res.status(404).send('TVL data not found');
    }

    // return the only value in the array (latest)
    return res.json(result); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/tvl/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllTVLData(chain);

    if (!result.length) {
      return res.status(404).send('TVL data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/core-delegations/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestCoreDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core Delegations data not found');
    }

    // return the only value in the array (latest)
    return res.json(result); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/core-delegations/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllCoreDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core Delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

app.get('/pool-rewards/latest/:chain?', async (req, res) => {
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

app.get('/pool-rewards/all/:chain?', async (req, res) => {
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

router.get('/core-account-delegations/staker-count/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const stakerCount = await getStakerCount(chain);

    return res.json(stakerCount);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

router.get('/core-account-delegations/all/:chain?', async (req, res) => {
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

router.get('/core-account-delegations/account/:accountId', async (req, res) => {
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

router.get('/core-account-delegations/ordered-by-account/:chain?', async (req, res) => {
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

module.exports = app;
