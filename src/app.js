const express = require('express');
const app = express();

const apyRoutes = require('./routes/apy');
const tvlRoutes = require('./routes/tvl');
const coreDelegationsRoutes = require('./routes/coreDelegations');
const poolRewardsRoutes = require('./routes/poolRewards');
const coreAccountDelegationsRoutes = require('./routes/coreAccountDelegations');

app.use('/apy', apyRoutes);
app.use('/tvl', tvlRoutes);
app.use('/core-delegations', coreDelegationsRoutes);
app.use('/pool-rewards', poolRewardsRoutes);
app.use('/core-account-delegations', coreAccountDelegationsRoutes);

app.get('/', async (req, res) => {
  try {
    return res.send('Synthetix V3 Cache API'); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = app;
