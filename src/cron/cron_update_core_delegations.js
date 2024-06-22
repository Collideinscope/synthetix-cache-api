const { fetchAndUpdateLatestCoreDelegationsData } = require('../services/coreDelegationsService');

const cronUpdateCoreDelegations = async () => {
  console.log('Running cron job to update CoreDelegations data...');

  await fetchAndUpdateLatestCoreDelegationsData('base');
  await fetchAndUpdateLatestCoreDelegationsData('arbitrum');

  console.log('Cron job Core Delegations Update completed successfully');
}

cronUpdateCoreDelegations();
