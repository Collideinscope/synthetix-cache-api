const redisService = require('../services/redisService');
const { refreshAllPerpStatsData } = require('../services/perpStatsService');
const { troyDBKnex } = require('../config/db');

const cronRefreshPerpStats = async () => {
  console.log('Starting Perp Stats refresh cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    console.log('Refreshing Perp Stats data for all chains');
    console.time('Total refresh time');
    await refreshAllPerpStatsData();
    console.timeEnd('Total refresh time');
    console.log('Perp Stats data refreshed for all chains');
  } catch (error) {
    console.error('Error in Perp Stats refresh cron job:', error);
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

cronRefreshPerpStats().catch(error => {
  console.error('Unhandled error in cronRefreshPerpStats:', error);
  process.exit(1);
});