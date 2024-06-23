const { fetchAndInsertAllAPYData } = require('../services/apyService');
const { fetchAndInsertAllTVLData } = require('../services/tvlService');
const { fetchAndInsertAllCoreDelegationsData } = require('../services/coreDelegationsService');

const seedAllData = async () => {
  /* APY Seeding */
  console.log('Seeding APY data...');

  await fetchAndInsertAllAPYData('base');
  await fetchAndInsertAllAPYData('arbitrum');

  console.log('Seeding APY data completed.');

  /* TVL Seeding */
  console.log('Seeding TVL data...');

  await fetchAndInsertAllTVLData('base');
  await fetchAndInsertAllTVLData('arbitrum');

  console.log('Seeding TVL data completed.');

  /* Core Delegations Seeding */
  console.log('Seeding Core Delegations data...');

  await fetchAndInsertAllCoreDelegationsData('base');
  await fetchAndInsertAllCoreDelegationsData('arbitrum');

  console.log('Seeding TVL Core Delegations completed.');

};

seedAllData();

module.exports = { seedAllData };
