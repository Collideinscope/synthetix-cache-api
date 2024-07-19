const { fetchAndInsertAllAPYData } = require('../services/apyService');
const { fetchAndInsertAllTVLData } = require('../services/tvlService');
const { fetchAndInsertAllCoreDelegationsData } = require('../services/coreDelegationsService');
const { fetchAndInsertAllPoolRewardsData } = require('../services/poolRewardsService');
const { fetchAndInsertAllCoreAccountDelegationsData } = require('../services/coreAccountDelegationsService');
const { fetchAndInsertAllPerpStatsData } = require('../services/perpStatsService');
const { fetchAndInsertAllPerpAccountStatsData } = require('../services/perpAccountStatsService');
const { fetchAndInsertAllPerpMarketHistoryData } = require('../services/perpMarketHistoryService');

const seedAllData = async () => {
  
  /* APY Seeding 
  console.log('Seeding APY data...');

  await fetchAndInsertAllAPYData('base');
  await fetchAndInsertAllAPYData('arbitrum');

  console.log('Seeding APY data completed.');

  /* TVL Seeding 
  console.log('Seeding TVL data...');

  await fetchAndInsertAllTVLData('base');
  await fetchAndInsertAllTVLData('arbitrum');

  console.log('Seeding TVL data completed.');

  /* Core Delegations Seeding 
  console.log('Seeding Core Delegations data...');

  await fetchAndInsertAllCoreDelegationsData('base');
  await fetchAndInsertAllCoreDelegationsData('arbitrum');

  console.log('Seeding TVL Core Delegations completed.');

  /* Pool Rewards Seeding 
  console.log('Seeding Pool Rewards data...');

  await fetchAndInsertAllPoolRewardsData('base');
  await fetchAndInsertAllPoolRewardsData('arbitrum');

  console.log('Seeding Pool Rewards completed.');  

  /* Core Accounts Delegations 
  console.log('Seeding Core Accounts Delegations data...');

  await fetchAndInsertAllCoreAccountDelegationsData('base');
  await fetchAndInsertAllCoreAccountDelegationsData('arbitrum');

  console.log('Seeding Core Accounts Delegations completed.'); 

  /* Perp Stats 
  console.log('Seeding Perp Stats data...');

  await fetchAndInsertAllPerpStatsData('base');

  console.log('Seeding Perp Stats completed.'); 

  /* Perp Account Stats 
  console.log('Seeding Perp Account Stats data...');

  await fetchAndInsertAllPerpAccountStatsData('base');

  console.log('Seeding Perp Account Stats completed.'); */

  /* Perp Market History */
  console.log('Seeding Perp Market History data...');

  await fetchAndInsertAllPerpMarketHistoryData('base');

  console.log('Seeding Perp Market History completed.'); 
};

module.exports = { seedAllData };
