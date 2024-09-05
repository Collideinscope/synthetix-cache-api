const redisService = require('../services/redisService');
const { refreshAllPerpAccountStatsData } = require('../services/perpAccountStatsService');
const { troyDBKnex } = require('../config/db');

const cronRefreshPerpAccountStats = async () => {
  console.log('Starting Perp Account Stats refresh cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    console.log('Refreshing Perp Account Stats data for all chains');
    await refreshAllPerpAccountStatsData();
    console.log('Perp Account Stats data refreshed for all chains');
  } catch (error) {
    console.error('Error in Perp Account Stats refresh cron job:', error);
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

cronRefreshPerpAccountStats().catch(error => {
  console.error('Unhandled error in cronRefreshPerpAccountStats:', error);
  process.exit(1);
});