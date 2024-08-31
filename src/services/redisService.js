const redis = require('redis');

class RedisService {
  constructor() {
    this.client = null;
    this.connected = false;
    this.connect();
  }

  async connect() {
    console.log('Attempting to connect to Redis at:', process.env.REDIS_URL);
    const options = {
      url: process.env.REDIS_URL
    };

    if (process.env.REDIS_URL && !process.env.REDIS_URL.includes('localhost')) {
      options.socket = {
        tls: true,
        rejectUnauthorized: false
      };
    }

    this.client = redis.createClient(options);

    this.client.on('error', (error) => {
      console.error('Redis Client Error', error);
      this.connected = false;
    });

    this.client.on('ready', () => {
      console.log('Redis client ready');
      this.connected = true;
    });

    try {
      await this.client.connect();
      console.log('Redis client connected');
      this.connected = true;
    } catch (error) {
      console.error('Failed to connect to Redis:', error);
      this.connected = false;
      // Instead of throwing, we'll log the error and continue
    }
  }

  async waitForConnection(timeout = 30000) {
    if (this.connected) return true;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        console.log('Redis connection timeout');
        resolve(false); // Resolve with false instead of rejecting
      }, timeout);
      this.client.once('ready', () => {
        clearTimeout(timer);
        resolve(true);
      });
      this.client.once('error', (err) => {
        clearTimeout(timer);
        console.error('Redis connection error:', err);
        resolve(false); // Resolve with false instead of rejecting
      });
    });
  }

  async get(key) {
    if (!this.connected) {
      console.warn('Redis client is not connected. Returning null.');
      return null;
    }
    try {
      const value = await this.client.get(key);
      return value ? JSON.parse(value) : null;
    } catch (error) {
      console.error('Redis Get Error', error);
      return null; // Return null instead of throwing
    }
  }

  async set(key, value, ttl) {
    if (!this.connected) {
      console.warn('Redis client is not connected. Skipping set operation.');
      return;
    }
    try {
      await this.client.set(key, JSON.stringify(value), {
        EX: ttl
      });
    } catch (error) {
      console.error('Redis Set Error', error);
      // Log error but don't throw
    }
  }
}

module.exports = new RedisService();