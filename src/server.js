require('dotenv').config();
const app = require('./app');
const redisService = require('./services/redisService');

const PORT = process.env.PORT || 3001;

async function startServer() {
  try {
    console.log('Starting server...');
    console.log('Redis URL:', process.env.REDIS_URL);
    console.log('Connecting to Redis...');
    await redisService.waitForConnection();
    console.log('Redis connected successfully');

    app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    if (error.code) {
      console.error('Error code:', error.code);
    }
    process.exit(1);
  }
}

startServer();