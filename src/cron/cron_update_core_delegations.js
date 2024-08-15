const { fetchAndUpdateLatestCoreDelegationsData } = require('../services/coreDelegationsService');

const cronUpdateCoreDelegations = async () => {
  try {
    console.log('Running cron job to update Core Delegations data...');

    await fetchAndUpdateLatestCoreDelegationsData('base');
    await fetchAndUpdateLatestCoreDelegationsData('arbitrum');

    console.log('Cron job Core Delegations Update completed');
  } catch (error) {
    console.error('Error running cron job for Core Delegations update:', error);
  } finally {
    process.exit(0); 
  }
}

module.exports = cronUpdateCoreDelegations;

cronUpdateCoreDelegations();
