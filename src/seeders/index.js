const { fetchAndInsertAllAPYData } = require('../services/apyService');

const seedAPYData = async () => {
  try {
    await fetchAndInsertAllAPYData();

    console.log('Seeding APY data completed.');
  } catch (error) {
    console.error('Seeding APY failed:', error);
  }
};

module.exports = { seedAPYData };
