const { fetchAndUpdateLatestPoolRewardsData } = require('../services/poolRewardsService');

const cronUpdatePoolRewards = async () => {
  try {
    console.log('Running cron job to update Pool Rewards data...');

    await fetchAndUpdateLatestPoolRewardsData('base');
    await fetchAndUpdateLatestPoolRewardsData('arbitrum');

    console.log('Cron job Pool Rewards Update completed');
  } catch (error) {
    console.error('Error running cron job for Pool Rewards update:', error);
  } finally {
    process.exit(0); 
  }
}

cronUpdatePoolRewards();
