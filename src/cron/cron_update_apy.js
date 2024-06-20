const { updateAPYData } = require('../services/apyService');

const cronUpdateAPY = async () => {
  try {
    console.log('Running cron job to update APY data...');

    await updateAPYData();

    console.log('Cron job completed successfully');
  } catch (error) {
    console.error('Error running update APY cron job:', error);
  }
}

cronUpdateAPY();
