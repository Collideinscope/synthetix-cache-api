const { updateTVLData } = require('../services/tvlService');

const cronUpdateTVL = async () => {
  try {
    console.log('Running cron job to update TVL data...');

    await updateTVLData();

    console.log('Cron job completed successfully');
  } catch (error) {
    console.error('Error running update TVL cron job:', error);
  }
}

cronUpdateTVL();