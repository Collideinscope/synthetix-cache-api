const { fetchAndUpdateLatestPerpMarketHistoryData } = require('../services/perpMarketHistoryService');

const cronUpdateTVL = async () => {
  try {
    console.log('Running cron job to update Perp Market History data...');

    await fetchAndUpdateLatestPerpMarketHistoryData('base');

    console.log('Cron job Perp Market History Update completed');
  } catch (error) {
    console.error('Error running cron job for Perp Market History update:', error);
  } finally {
    process.exit(0);
  }
}

cronUpdateTVL();
