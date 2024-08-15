const { fetchAndUpdateLatestPerpStatsData } = require('../services/perpStatsService');

const cronUpdatePerpStats = async () => {
  try {
    console.log('Running cron job to update Perp Stats data...');

    await fetchAndUpdateLatestPerpStatsData('base');

    console.log('Cron job Perp Stats Update completed');
  } catch (error) {
    console.error('Error running cron job for Perp Stats update:', error);
  } finally {
    process.exit(0);
  }
}

cronUpdatePerpStats();
