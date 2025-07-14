import rateLimit from 'express-rate-limit';
import { getRedisClient } from '../config/redis.js';
import config from '../config/index.js';
import * as cacheService from '../services/cacheService.js';
import logger from '../utils/logger.js';

// Create rate limiter
export const apiLimiter = rateLimit({
  windowMs: config.rateLimit.windowMs,
  max: config.rateLimit.max,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    status: 'error',
    message: 'Too many requests, please try again later.'
  }
});

/**
 * Cache middleware - caches responses for specified duration
 * @param {number} duration - Cache duration in seconds
 */
export const cacheMiddleware = (duration = config.cache.ttl) => {
  return async (req, res, next) => {
    // Skip caching for non-GET requests
    if (req.method !== 'GET') {
      return next();
    }
    
    // Create a cache key from the request path and query params
    const cacheKey = `cache:${req.originalUrl || req.url}`;
    
    try {
      // Try to get response from cache
      const cachedResponse = await cacheService.get(cacheKey);
      
      if (cachedResponse) {
        // Add cache header
        res.setHeader('X-Cache', 'HIT');
        return res.status(200).json(cachedResponse);
      }
      
      // If not in cache, capture the response
      const originalSend = res.send;
      
      res.send = function(body) {
        // Only cache successful responses
        if (res.statusCode === 200) {
          try {
            // Parse the response body and cache it
            const responseBody = JSON.parse(body);
            cacheService.set(cacheKey, responseBody, duration);
          } catch (e) {
            // If body is not JSON, don't cache
          }
        }
        
        // Add cache header
        res.setHeader('X-Cache', 'MISS');
        
        // Call the original send
        return originalSend.call(this, body);
      };
      
      next();
    } catch (error) {
      // If caching fails, just continue
      next();
    }
  };
};