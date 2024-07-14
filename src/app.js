const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');
const path = require('path');

const app = express();

// Load the combined Swagger documentation file
let swaggerDocument;
try {
  swaggerDocument = YAML.load('./src/docs/combined-swagger.yaml');
  console.log('Swagger documentation loaded successfully.');
} catch (error) {
  console.error('Error loading Swagger documentation:', error);
}

const apyRoutes = require('./routes/apy');
const tvlRoutes = require('./routes/tvl');
const coreDelegationsRoutes = require('./routes/coreDelegations');
const poolRewardsRoutes = require('./routes/poolRewards');
const coreAccountDelegationsRoutes = require('./routes/coreAccountDelegations');

app.use(cors());

app.use('/apy', apyRoutes);
app.use('/tvl', tvlRoutes);
app.use('/core-delegations', coreDelegationsRoutes);
app.use('/pool-rewards', poolRewardsRoutes);
app.use('/core-account-delegations', coreAccountDelegationsRoutes);

// Setup Swagger UI with the combined document
if (swaggerDocument) {
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));
} else {
  console.error('Swagger documentation is not available.');
}

app.get('/', async (req, res) => {
  try {
    return res.send('Synthetix V3 Cache API');
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = app;
