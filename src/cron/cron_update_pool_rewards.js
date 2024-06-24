const { fetchAndUpdateLatestPoolRewardsData } = require('../services/poolRewardsService');

const cronUpdatePoolRewards = async () => {
  console.log('Running cron job to update Pool Rewards data...');

  await fetchAndUpdateLatestPoolRewardsData('base');
  await fetchAndUpdateLatestPoolRewardsData('arbitrum');

  console.log('Cron job Pool Rewards Update completed');
}

cronUpdatePoolRewards();
