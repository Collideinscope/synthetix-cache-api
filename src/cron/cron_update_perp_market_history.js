const redisService = require('../services/redisService');
const { refreshAllPerpMarketHistoryData } = require('../services/perpMarketHistoryService');
const { troyDBKnex } = require('../config/db');

const cronRefreshPerpMarketHistory = async () => {
  console.log('Starting Perp Market History refresh cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    console.log('Refreshing Perp Market History data for all chains');
    console.time('Total refresh time');
    await refreshAllPerpMarketHistoryData();
    console.timeEnd('Total refresh time');
    console.log('Perp Market History data refreshed for all chains');
  } catch (error) {
    console.error('Error in Perp Market History refresh cron job:', error);
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

cronRefreshPerpMarketHistory().catch(error => {
  console.error('Unhandled error in cronRefreshPerpMarketHistory:', error);
  process.exit(1);
});