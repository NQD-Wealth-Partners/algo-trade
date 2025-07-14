import { getRedisClient } from '../config/redis.js';
import asyncHandler from '../utils/asyncHandler.js';
import { successResponse } from '../utils/responseFormatter.js';

/**
 * Get API health status
 * @route GET /api/v1/health
 * @access Public
 */
export const getHealth = asyncHandler(async (req, res) => {
  const health = {
    status: 'OK',
    timestamp: new Date().toISOString(),
    uptime: `${Math.floor(process.uptime())} seconds`,
    environment: process.env.NODE_ENV,
    version: process.env.npm_package_version || '1.0.0',
    node_version: process.version
  };

  return successResponse(res, health);
});

/**
 * Get Redis health status
 * @route GET /api/v1/health/redis
 * @access Public
 */
export const getRedisHealth = asyncHandler(async (req, res) => {
  const redisClient = getRedisClient();
  
  // Test Redis connection by setting and getting a value
  const testKey = `health:test:${Date.now()}`;
  const testValue = 'Redis is working!';
  
  await redisClient.set(testKey, testValue, 'EX', 60); // 60 seconds expiry
  const retrievedValue = await redisClient.get(testKey);
  
  const health = {
    status: retrievedValue === testValue ? 'OK' : 'ERROR',
    redis: {
      connected: redisClient.status === 'ready',
      connectionInfo: {
        host: redisClient.options.host,
        port: redisClient.options.port,
        db: redisClient.options.db
      },
      memoryUsage: await redisClient.info('memory'),
      commandStats: await redisClient.info('commandstats')
    },
    timestamp: new Date().toISOString()
  };

  return successResponse(res, health);
});