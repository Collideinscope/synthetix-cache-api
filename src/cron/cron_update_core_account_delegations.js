const redisService = require('../services/redisService');
const { refreshAllCoreAccountDelegationsData } = require('../services/coreAccountDelegationsService');
const { troyDBKnex } = require('../config/db');

const cronRefreshCoreAccountDelegations = async () => {
  console.log('Starting core account delegations refresh cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    const collateralType = '0xC74eA762cF06c9151cE074E6a569a5945b6302E7';
    console.log(`Refreshing core account delegations data for collateral type: ${collateralType}`);
    await refreshAllCoreAccountDelegationsData(collateralType);
    console.log('Core account delegations data refreshed for all chains');
  } catch (error) {
    console.error('Error in core account delegations refresh cron job:', error);
  } finally {
    try {
      if (redisService.connected) {
        console.log('Disconnecting from Redis...');
        await redisService.disconnect();
        console.log('Redis disconnected');
      }
      
      // Close database connections
      console.log('Closing database connections...');
      await troyDBKnex.destroy();
      console.log('Database connections closed');
    } catch (cleanupError) {
      console.error('Error during cleanup:', cleanupError);
    }

    console.log('Cron job finished at:', new Date().toISOString());
    process.exit(0);
  }
};

cronRefreshCoreAccountDelegations().catch(error => {
  console.error('Unhandled error in cronRefreshCoreAccountDelegations:', error);
  process.exit(1);
});