const { fetchAndUpdateLatestAPYData } = require('../services/apyService');

const cronUpdateAPY = async () => {
  try {
    console.log('Running cron job to update APY data...');

    await fetchAndUpdateLatestAPYData();

    console.log('Cron job APY Update completed successfully');
  } catch (error) {
    console.error('Error running APY Update cron job:', error);
  }
}

cronUpdateAPY();
