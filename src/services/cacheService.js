import { getRedisClient } from '../config/redis.js';
import config from '../config/index.js';
import logger from '../utils/logger.js';

/**
 * Set a value in cache
 * @param {string} key - Cache key
 * @param {any} value - Value to cache (will be JSON stringified)
 * @param {number} ttl - Time to live in seconds
 * @returns {Promise<boolean>} - Success status
 */
export async function set(key, value, ttl = config.cache.ttl) {
  try {
    const redisClient = getRedisClient();
    const valueStr = JSON.stringify(value);
    await redisClient.set(key, valueStr, 'EX', ttl);
    logger.debug(`Cache set: ${key} (TTL: ${ttl}s)`);
    return true;
  } catch (error) {
    logger.error(`Cache set error for key ${key}:`, error);
    return false;
  }
}

/**
 * Get a value from cache
 * @param {string} key - Cache key
 * @returns {Promise<any>} - Cached value or null if not found
 */
export async function get(key) {
  try {
    const redisClient = getRedisClient();
    const value = await redisClient.get(key);
    if (!value) {
      logger.debug(`Cache miss: ${key}`);
      return null;
    }
    
    logger.debug(`Cache hit: ${key}`);
    return JSON.parse(value);
  } catch (error) {
    logger.error(`Cache get error for key ${key}:`, error);
    return null;
  }
}

/**
 * Delete a value from cache
 * @param {string} key - Cache key
 * @returns {Promise<boolean>} - Success status
 */
export async function invalidate(key) {
  try {
    const redisClient = getRedisClient();
    await redisClient.del(key);
    logger.debug(`Cache invalidated: ${key}`);
    return true;
  } catch (error) {
    logger.error(`Cache invalidation error for key ${key}:`, error);
    return false;
  }
}

/**
 * Invalidate multiple cache keys by pattern
 * @param {string} pattern - Redis key pattern with wildcards
 * @returns {Promise<boolean>} - Success status
 */
export async function invalidatePattern(pattern) {
  try {
    const redisClient = getRedisClient();
    const keys = await redisClient.keys(pattern);
    if (keys.length > 0) {
      await redisClient.del(...keys);
      logger.debug(`Invalidated ${keys.length} keys matching pattern: ${pattern}`);
    }
    return true;
  } catch (error) {
    logger.error(`Pattern invalidation error for ${pattern}:`, error);
    return false;
  }
}

/**
 * Get or set cache - retrieves from cache or executes function and caches result
 * @param {string} key - Cache key
 * @param {Function} fn - Function to execute if cache miss
 * @param {number} ttl - Time to live in seconds
 * @returns {Promise<any>} - Result from cache or function
 */
export async function getOrSet(key, fn, ttl = config.cache.ttl) {
  // Try to get from cache
  const cachedValue = await get(key);
  if (cachedValue !== null) {
    return cachedValue;
  }

  // Execute function and cache result
  const result = await fn();
  await set(key, result, ttl);
  return result;
}