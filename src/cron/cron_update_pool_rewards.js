const redisService = require('../services/redisService');
const { refreshAllPoolRewardsData } = require('../services/poolRewardsService');
const { troyDBKnex } = require('../config/db');

const cronRefreshPoolRewards = async () => {
  console.log('Starting Pool Rewards refresh cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    const collateralType = '0xC74eA762cF06c9151cE074E6a569a5945b6302E7';
    console.log(`Refreshing Pool Rewards data for collateral type: ${collateralType}`);
    console.time('Total refresh time');
    await refreshAllPoolRewardsData(collateralType);
    console.timeEnd('Total refresh time');
    console.log('Pool Rewards data refreshed for all chains');
  } catch (error) {
    console.error('Error in Pool Rewards refresh cron job:', error);
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

cronRefreshPoolRewards().catch(error => {
  console.error('Unhandled error in cronRefreshPoolRewards:', error);
  process.exit(1);
});