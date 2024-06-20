require('dotenv').config();

const developmentConfig = {
  client: 'pg',
  connection: {
    connectionString: process.env.LOCAL_DATABASE_URL,
    timezone: 'UTC', // Set timezone to UTC
  },
  pool: {
    afterCreate: (conn, done) => {
      // Ensure that the connection uses UTC timezone
      conn.query('SET timezone="UTC";', (err) => {
        done(err, conn);
      });
    },
  },
  migrations: {
    directory: __dirname + '/migrations'
  },
  seeds: {
    directory: __dirname + '/seeds'
  },
  debug: true
};

const productionConfig = {
  client: 'pg',
  connection: {
    connectionString: process.env.CACHE_DB_URL,
    ssl: {
      rejectUnauthorized: false
    },
    timezone: 'UTC', // Set timezone to UTC
  },
  pool: {
    min: 2,
    max: 10,
    afterCreate: (conn, done) => {
      // Ensure that the connection uses UTC timezone
      conn.query('SET timezone="UTC";', (err) => {
        done(err, conn);
      });
    },
  },
  migrations: {
    tableName: 'knex_migrations',
    directory: __dirname + '/migrations'
  }
};

const troyDBConfig = {
  client: 'pg',
  connection: {
    user: process.env.TROY_DB_USER,
    host: process.env.TROY_DB_HOST,
    database: process.env.TROY_DB_NAME,
    password: process.env.TROY_DB_PASS,
    port: process.env.TROY_DB_PORT,
    timezone: 'UTC', // Set timezone to UTC
  },
  pool: {
    min: 2,
    max: 10,
    afterCreate: (conn, done) => {
      // Ensure that the connection uses UTC timezone
      conn.query('SET timezone="UTC";', (err) => {
        done(err, conn);
      });
    },
  },
  migrations: {
    tableName: 'knex_migrations',
    directory: __dirname + '/migrations'
  }
};

module.exports = {
  development: developmentConfig,
  production: productionConfig,
  troyDB: troyDBConfig
};
