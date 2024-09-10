
const { cronRefreshAPY } = require('./cron_update_apy');
const { cronRefreshCoreAccountDelegations } = require('./cron_update_core_account_delegations');
const { cronRefreshCoreDelegations } = require('./cron_update_core_delegations');
const { cronRefreshPerpAccountStats } = require('./cron_update_perp_account_stats');
const { cronRefreshPerpMarketHistory } = require('./cron_update_perp_market_history');
const { cronRefreshPerpStats } = require('./cron_update_perp_stats');
const { cronRefreshPoolRewards } = require('./cron_update_pool_rewards');
const { cronRefreshTVL } = require('./cron_update_tvl');

const cronUpdateAll = async () => {
  console.log('Starting cron job Update All at:', new Date().toISOString());
  try {
    await cronRefreshAPY();
    await cronRefreshCoreAccountDelegations();
    await cronRefreshCoreDelegations();
    await cronRefreshPerpAccountStats();
    await cronRefreshPerpMarketHistory();
    await cronRefreshPerpStats();
    await cronRefreshPoolRewards();
    await cronRefreshTVL();
    
    console.log('All cron jobs completed successfully');
  } catch (error) {
    console.error('Error in cron job Updating All:', error);
  } finally {
    console.log('Cron job Update All finished at:', new Date().toISOString());
  }
}

module.exports = cronUpdateAll;