const { fetchAndUpdateLatestCoreAccountDelegationsData } = require('../services/coreAccountDelegationsService');

const cronUpdateCoreAccountDelegations = async () => {
  try {
    console.log('Running cron job to update Core Account Delegations data...');

    await fetchAndUpdateLatestCoreAccountDelegationsData('base');
    await fetchAndUpdateLatestCoreAccountDelegationsData('arbitrum');

    console.log('Cron job Core Account Delegations Update completed');
  } catch (error) {
    console.error('Error running cron job for Core Account Delegations update:', error);
  } finally {
    process.exit(0); 
  }
}

cronUpdateCoreAccountDelegations();
