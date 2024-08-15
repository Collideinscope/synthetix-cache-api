const updateAPY = require('./cron_update_apy');
const updateTVL = require('./cron_update_tvl');
const cronUpdateCoreAccountDelegations = require('./cron_update_core_account_delegations');
const cronUpdateCoreDelegations = require('./cron_update_core_delegations');
const cronUpdatePerpAccountStats = require('./cron_update_perp_account_stats');
const cronUpdatePerpMarketHistory = require('./cron_update_perp_market_history');
const cronUpdatePerpStats = require('./cron_update_perp_stats');
const cronUpdatePoolRewards = require('./cron_update_pool_rewards');

const cronUpdateAll = async () => {
  try {
    console.log('Starting update for all services...');

    await updateAPY();
    console.log('APY update completed');

    await updateTVL();
    console.log('TVL update completed');

    await cronUpdateCoreAccountDelegations();
    console.log('Core Account Delegations update completed');

    await cronUpdateCoreDelegations();
    console.log('Core Delegations update completed');

    await cronUpdatePerpAccountStats();
    console.log('Perp Account Stats update completed');

    await cronUpdatePerpMarketHistory();
    console.log('Perp Market History update completed');

    await cronUpdatePerpStats();
    console.log('Perp Stats update completed');

    await cronUpdatePoolRewards();
    console.log('Pool Rewards update completed');

    console.log('All updates completed successfully');
  } catch (error) {
    console.error('Error during cronUpdateAll:', error);
  }
};

module.exports = cronUpdateAll;

cronUpdateAll();