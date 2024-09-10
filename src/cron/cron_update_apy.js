const redisService = require('../services/redisService'); 
const { refreshAllAPYData } = require('../services/apyService');
const { troyDBKnex } = require('../config/db');

const cronRefreshAPY = async () => {
  console.log('Starting cron job at:', new Date().toISOString());
  try {
    console.log('Attempting to connect to Redis...');
    await redisService.connect();
    console.log('Redis connection established successfully');

    console.log(`Refreshing APY data for collateral type: ${'0xc74ea762cf06c9151ce074e6a569a5945b6302e7'}`);
    await refreshAllAPYData('0xc74ea762cf06c9151ce074e6a569a5945b6302e7');
    console.log(`APY data refreshed for collateral type: ${'0xc74ea762cf06c9151ce074e6a569a5945b6302e7'}`);
  
    console.log('Cron job APY Refresh completed for all collateral types');
  } catch (error) {
    console.error('Error in cron job:', error);
  } finally {
    if (redisService.connected) {
      console.log('Disconnecting from Redis...');
      await redisService.disconnect();
      console.log('Redis disconnected');
    }
    
    // Destroy the Knex instance to close all database connections
    await troyDBKnex.destroy();
    console.log('Database connections closed');

    console.log('Cron job finished at:', new Date().toISOString());
    process.exit(0);
  }
}

cronRefreshAPY().catch(error => {
  console.error('Unhandled error in cronRefreshAPY:', error);
  process.exit(1);
});

module.exports = cronRefreshAPY;