require('dotenv').config();

const { seedAPYData } = require('../src/seeders/');

// ensure server timezone is UTC for DB consistency
process.env.TZ = 'UTC';

exports.seed = async function(knex) {
  // Deletes ALL existing entries
  await knex('apy').del();

  await seedAPYData();
};
