const redisService = require('../services/redisService');
const { refreshAllTVLData } = require('../services/tvlService');
const { troyDBKnex } = require('../config/db');

const cronRefreshTVL = async () => {
  console.log('Starting TVL refresh cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    const collateralType = '0xc74ea762cf06c9151ce074e6a569a5945b6302e7';
    console.log(`Refreshing TVL data for collateral type: ${collateralType}`);
    console.time('Total refresh time');
    await refreshAllTVLData(collateralType);
    console.timeEnd('Total refresh time');
    console.log('TVL data refreshed for all chains');
  } catch (error) {
    console.error('Error in TVL refresh cron job:', error);
  } finally {
    try {
      if (redisService.connected) {
        console.log('Disconnecting from Redis...');
        await redisService.disconnect();
        console.log('Redis disconnected');
      }
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

cronRefreshTVL().catch(error => {
  console.error('Unhandled error in cronRefreshTVL:', error);
  process.exit(1);
});