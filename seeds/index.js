require('dotenv').config();

const { fetchAndInsertAllAPYData } = require('../src/services/apyService');

// ensure server timezone is UTC for DB consistency
process.env.TZ = 'UTC';

exports.seed = async function(knex) {
  // Deletes ALL existing entries
  await knex('apy').del();

  // Insert seed data
  try {
    await fetchAndInsertAllAPYData();

    console.log('Data seeded successfully.');
  } catch (error) {
    console.error('Error seeding APY data:', error);
  }
};
