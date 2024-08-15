const { fetchAndUpdateLatestAPYData } = require('../services/apyService');

const cronUpdateAPY = async () => {
  try {
    console.log('Running cron job to update APY data...');

    await fetchAndUpdateLatestAPYData('base');
    await fetchAndUpdateLatestAPYData('arbitrum');

    console.log('Cron job APY Update completed');
  } catch (error) {
    console.error('Error running cron job for APY update:', error);
  } finally {
    process.exit(0); 
  }
}

cronUpdateAPY();
