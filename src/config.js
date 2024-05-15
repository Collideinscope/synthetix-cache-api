require('dotenv').config();

const environment = process.env.NODE_ENV || 'development';

const CLIENT_URL = environment === 'development' || environment === 'test'
  ? process.env.DEVELOPMENT_CLIENT_URL
  : process.env.PRODUCTION_CLIENT_URL;

module.exports = {
  CLIENT_URL
}