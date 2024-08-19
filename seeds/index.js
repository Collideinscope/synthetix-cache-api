require('dotenv').config();

const { seedAllData } = require('../src/seeders/');

// ensure server timezone is UTC for DB consistency
process.env.TZ = 'UTC';

exports.seed = async function(knex) {
  // Deletes ALL existing entries
  /*
  await knex('apy').del();
  await knex('tvl').del();
  await knex('core_delegations').del();
  await knex('pool_rewards').del();
  await knex('core_account_delegations').del();
  await knex('perp_stats').del();*/
  await knex('perp_account_stats').del(); 
  //await knex('perp_market_history').del();

  await seedAllData();
};
