require('dotenv').config();

const { fetchAndInsertAllAPYData } = require('../src/services/apyService');

// ensure server timezone is UTC for DB consistency
process.env.TZ = 'UTC';

exports.seed = async function(knex) {
  // Deletes ALL existing entries
  await knex('apy').del();

  // Seed APY data for base chain
  await fetchAndInsertAllAPYData('base');

  // Seed APY data for arbitrum chain
  await fetchAndInsertAllAPYData('arbitrum');
};
