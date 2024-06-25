const { fetchAndUpdateLatestTVLData } = require('../services/tvlService');

const cronUpdateTVL = async () => {
  try {
    console.log('Running cron job to update TVL data...');

    await fetchAndUpdateLatestTVLData('base');
    await fetchAndUpdateLatestTVLData('arbitrum');

    console.log('Cron job TVL Update completed');
  } catch (error) {
    console.error('Error running cron job for TVL update:', error);
  } finally {
    process.exit(0);
  }
}

cronUpdateTVL();
