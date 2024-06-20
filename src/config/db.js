const knexfile = require('../../knexfile');
const environment = process.env.NODE_ENV || 'development';

// Primary database for writes and reads (local,cache databases)
const knex = require('knex')(knexfile[environment]);

// Troy's database for external data queries
const troyDBKnex = require('knex')(knexfile.troyDB);

module.exports = { knex, troyDBKnex };