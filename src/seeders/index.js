const { fetchAndInsertAllAPYData } = require('../services/apyService');

const seedAPYData = async () => {
  console.log('Seeding APY data...');

  await fetchAndInsertAllAPYData('base');

  await fetchAndInsertAllAPYData('arbitrum');

  console.log('Seeding APY data completed.');
};

module.exports = { seedAPYData };
