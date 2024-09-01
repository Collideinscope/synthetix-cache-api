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

    // Only add TLS options if we're not in a local environment
    if (process.env.REDIS_URL && !process.env.REDIS_URL.includes('localhost')) {
      options.socket = {
        tls: false,
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
    }
  }

  async waitForConnection(timeout = 30000) {
    if (this.connected) return true;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error('Redis connection timeout'));
      }, timeout);
      this.client.once('ready', () => {
        clearTimeout(timer);
        resolve(true);
      });
      this.client.once('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
  }

  async get(key) {
    if (!this.connected) {
      throw new Error('Redis client is not connected');
    }
    try {
      const value = await this.client.get(key);
      return value ? JSON.parse(value) : null;
    } catch (error) {
      console.error('Redis Get Error', error);
      throw error;
    }
  }

  async set(key, value, ttl) {
    if (!this.connected) {
      throw new Error('Redis client is not connected');
    }
    try {
      await this.client.set(key, JSON.stringify(value), {
        EX: ttl
      });
    } catch (error) {
      console.error('Redis Set Error', error);
      throw error;
    }
  }

  async del(key) {
    if (!this.connected) {
      throw new Error('Redis client is not connected');
    }
    try {
      await this.client.del(key);
    } catch (error) {
      console.error('Redis Delete Error', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.connected) {
      try {
        await this.client.quit();
        console.log('Redis client disconnected');
        this.connected = false;
      } catch (error) {
        console.error('Error disconnecting Redis client:', error);
      }
    }
  }
}

module.exports = new RedisService();