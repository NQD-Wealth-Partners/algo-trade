import Redis from 'ioredis';
import config from './index.js';
import logger from '../utils/logger.js';

let redisClient = null;

/**
 * Connect to Redis server
 */
export const connectRedis = () => {
  try {
    redisClient = new Redis({
      host: config.redis.host,
      port: config.redis.port,
      password: config.redis.password || undefined,
      db: config.redis.db,
      retryStrategy: (times) => {
        const delay = Math.min(times * 50, 2000);
        return delay;
      }
    });

    redisClient.on('connect', () => {
      logger.info('Redis client connected');
    });

    redisClient.on('error', (err) => {
      logger.error('Redis client error:', err);
    });

    return redisClient;
  } catch (error) {
    logger.error('Redis connection error:', error);
    process.exit(1);
  }
};

/**
 * Get Redis client instance
 * @returns {Object} Redis client
 */
export const getRedisClient = () => {
  if (!redisClient) {
    connectRedis();
  }
  return redisClient;
};