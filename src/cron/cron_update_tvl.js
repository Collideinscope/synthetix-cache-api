const { fetchAndUpdateLatestTVLData } = require('../services/tvlService');

const cronUpdateTVL = async () => {
  console.log('Running cron job to update TVL data...');

  await fetchAndUpdateLatestTVLData('base');
  await fetchAndUpdateLatestTVLData('arbitrum');

  console.log('Cron job TVL Update completed successfully');
}

cronUpdateTVL();
