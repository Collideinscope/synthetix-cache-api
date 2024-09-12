const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');
const fs = require('fs');
const path = require('path');

const app = express();

const apiRouter = express.Router();

const apyRoutes = require('./routes/apy');
const tvlRoutes = require('./routes/tvl');
const coreDelegationsRoutes = require('./routes/coreDelegations');
const poolRewardsRoutes = require('./routes/poolRewards');
const coreAccountDelegationsRoutes = require('./routes/coreAccountDelegations');
const perpStatsRoutes = require('./routes/perpStats');
const perpAccountStatsRoutes = require('./routes/perpAccountStats');
const perpMarketHistoryRoutes = require('./routes/perpMarketHistory');

app.use(cors());

app.use('/api/v1', apiRouter);

apiRouter.use('/apy', apyRoutes);
apiRouter.use('/tvl', tvlRoutes);
apiRouter.use('/core-delegations', coreDelegationsRoutes);
apiRouter.use('/pool-rewards', poolRewardsRoutes);
apiRouter.use('/core-account-delegations', coreAccountDelegationsRoutes);
apiRouter.use('/perp-stats', perpStatsRoutes);
apiRouter.use('/perp-account-stats', perpAccountStatsRoutes);
apiRouter.use('/perp-market-history', perpMarketHistoryRoutes);

const customCss = fs.readFileSync(path.join(__dirname, 'swagger-custom.css'), 'utf8');

// Load the combined Swagger documenttion file
let swaggerDocument;
try {
  swaggerDocument = YAML.load('./src/docs/combined-swagger.yaml');
  console.log('Swagger documentation loaded successfully.');
} catch (error) {
  console.error('Error loading Swagger documentation:', error);
}

// Setup Swagger UI with the combined document and custom options
if (swaggerDocument) {
  const options = {
    customCss,
    customSiteTitle: "Synthetix Stats API Documentation",
  };
  
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument, options));
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
