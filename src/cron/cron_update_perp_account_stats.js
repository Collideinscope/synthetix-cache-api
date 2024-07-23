const { fetchAndUpdateLatestPerpAccountStatsData } = require('../services/perpAccountStatsService');

const cronUpdateTVL = async () => {
  try {
    console.log('Running cron job to update Perp Account Stats data...');

    await fetchAndUpdateLatestPerpAccountStatsData('base');

    console.log('Cron job Perp Account Stats Update completed');
  } catch (error) {
    console.error('Error running cron job for Perp Account Stats update:', error);
  } finally {
    process.exit(0);
  }
}

cronUpdateTVL();
