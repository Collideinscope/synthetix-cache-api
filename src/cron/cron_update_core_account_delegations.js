const { fetchAndUpdateLatestCoreAccountDelegationsData } = require('../services/coreAccountDelegationsService');

const cronUpdateCoreAccountDelegations = async () => {
  console.log('Running cron job to update Core Account Delegations data...');

  await fetchAndUpdateLatestCoreAccountDelegationsData('base');
  await fetchAndUpdateLatestCoreAccountDelegationsData('arbitrum');

  console.log('Cron job Core Account Delegations Update completed');
}

cronUpdateCoreAccountDelegations();
