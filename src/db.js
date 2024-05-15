const { Pool } = require('pg');
require('dotenv').config();

const sourcePool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASS,
  port: process.env.DB_PORT,
});

const cachePool = new Pool({
  connectionString: process.env.CACHE_DB_URL,
  min: 2,
  max: 10,
  afterCreate: (conn, done) => {
    conn.query('SET timezone="UTC";', (err) => {
      done(err, conn);
    });
  },
  ssl: {
    rejectUnauthorized: false,
  },
});

module.exports = {
  sourcePool,
  cachePool,
};
