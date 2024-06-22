const { fetchAndUpdateLatestAPYData } = require('../services/apyService');

const cronUpdateAPY = async () => {
  console.log('Running cron job to update APY data...');

  await fetchAndUpdateLatestAPYData('base');

  console.log('Cron job APY Update completed successfully');
}

cronUpdateAPY();
