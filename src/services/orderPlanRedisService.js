import Redis from 'ioredis';
import logger from '../utils/logger.js';
import config from '../config/index.js';
import { v4 as uuidv4 } from 'uuid';

// Initialize Redis client
const redis = new Redis({
  host: config.redis?.host || 'algo-trade-redis',
  port: config.redis?.port || 6379,
  password: config.redis?.password || '',
  db: config.redis?.db || 0
});

redis.on('error', (error) => {
  logger.error('Redis connection error:', error);
});

redis.on('connect', () => {
  logger.info('Connected to Redis server');
});

/**
 * Store order plan in Redis
 * @param {Object} orderPlan - Order plan details
 * @returns {Promise<Object>} Stored order plan with ID
 */
export async function storeOrderPlan(orderPlan) {
  try {
    // Generate unique ID for the order plan
    const planId = uuidv4();
    
    // Create storage key
    const key = `orderplan:${planId}`;
    
    // Add metadata
    const planWithMetadata = {
      ...orderPlan,
      id: planId,
      createdAt: new Date().toISOString(),
      status: 'CREATED'
    };
    
    // Store in Redis
    await redis.set(key, JSON.stringify(planWithMetadata));
    
    // Add to global plans set
    await redis.sadd('orderplans:all', key);
    
    // Add to symbol index if available
    if (orderPlan.tradingsymbol) {
      await redis.sadd(`orderplans:symbol:${orderPlan.tradingsymbol}`, planId);
    }
    
    // Add to exchange index if available
    if (orderPlan.exchange) {
      await redis.sadd(`orderplans:exchange:${orderPlan.exchange}`, planId);
    }
    
    logger.info(`Order plan ${planId} stored successfully`);
    
    return planWithMetadata;
  } catch (error) {
    logger.error(`Error storing order plan:`, error);
    throw new Error(`Failed to store order plan: ${error.message}`);
  }
}

/**
 * Get order plan by ID
 * @param {string} planId - Plan ID
 * @returns {Promise<Object|null>} Order plan or null if not found
 */
export async function getOrderPlan(planId) {
  try {
    const key = `orderplan:${planId}`;
    const planData = await redis.get(key);
    
    if (!planData) {
      return null;
    }
    
    return JSON.parse(planData);
  } catch (error) {
    logger.error(`Error getting order plan ${planId}:`, error);
    throw new Error(`Failed to get order plan: ${error.message}`);
  }
}

/**
 * Get all order plans
 * @param {Object} filters - Optional filters
 * @param {string} filters.symbol - Filter by trading symbol
 * @param {string} filters.exchange - Filter by exchange
 * @param {string} filters.status - Filter by status
 * @param {number} limit - Maximum number of results to return
 * @param {number} offset - Offset for pagination
 * @returns {Promise<Array>} List of order plans
 */
export async function getAllOrderPlans(filters = {}, limit = 100, offset = 0) {
  try {
    let planIds = [];
    
    // Apply filters if provided
    if (filters.symbol) {
      // Get plans for the specific symbol
      planIds = await redis.smembers(`orderplans:symbol:${filters.symbol}`);
    } else if (filters.exchange) {
      // Get plans for the specific exchange
      planIds = await redis.smembers(`orderplans:exchange:${filters.exchange}`);
    } else {
      // Get all plan keys
      const planKeys = await redis.smembers('orderplans:all');
      planIds = planKeys.map(key => key.split(':')[1]);
    }
    
    if (planIds.length === 0) {
      return [];
    }
    
    // Apply pagination
    const paginatedIds = planIds.slice(offset, offset + limit);
    
    // Get all plans in parallel
    const pipeline = redis.pipeline();
    paginatedIds.forEach(planId => {
      pipeline.get(`orderplan:${planId}`);
    });
    
    const results = await pipeline.exec();
    
    // Process results
    let plans = results
      .map(result => {
        // Redis pipeline returns [error, value]
        if (result[0]) {
          logger.error(`Error fetching plan: ${result[0]}`);
          return null;
        }
        return result[1] ? JSON.parse(result[1]) : null;
      })
      .filter(Boolean);
    
    // Apply status filter if provided
    if (filters.status && plans.length > 0) {
      plans = plans.filter(plan => plan.status === filters.status);
    }
    
    return plans;
  } catch (error) {
    logger.error(`Error getting order plans:`, error);
    throw new Error(`Failed to get order plans: ${error.message}`);
  }
}

/**
 * Delete order plan
 * @param {string} planId - Plan ID
 * @returns {Promise<boolean>} True if deleted, false if not found
 */
export async function deleteOrderPlan(planId) {
  try {
    const key = `orderplan:${planId}`;
    
    // Get plan details first to extract indexing information
    const planData = await redis.get(key);
    if (!planData) {
      return false;
    }
    
    const plan = JSON.parse(planData);
    
    // Delete plan
    await redis.del(key);
    
    // Remove from global plans set
    await redis.srem('orderplans:all', key);
    
    // Remove from symbol index if available
    if (plan.tradingsymbol) {
      await redis.srem(`orderplans:symbol:${plan.tradingsymbol}`, planId);
    }
    
    // Remove from exchange index if available
    if (plan.exchange) {
      await redis.srem(`orderplans:exchange:${plan.exchange}`, planId);
    }
    
    logger.info(`Order plan ${planId} deleted successfully`);
    
    return true;
  } catch (error) {
    logger.error(`Error deleting order plan ${planId}:`, error);
    throw new Error(`Failed to delete order plan: ${error.message}`);
  }
}

/**
 * Update order plan status
 * @param {string} planId - Plan ID
 * @param {string} status - New status
 * @returns {Promise<Object|null>} Updated plan or null if not found
 */
export async function updateOrderPlanStatus(planId, status) {
  try {
    const key = `orderplan:${planId}`;
    const planData = await redis.get(key);
    
    if (!planData) {
      return null;
    }
    
    const plan = JSON.parse(planData);
    plan.status = status;
    plan.updatedAt = new Date().toISOString();
    
    await redis.set(key, JSON.stringify(plan));
    
    logger.info(`Order plan ${planId} status updated to ${status}`);
    
    return plan;
  } catch (error) {
    logger.error(`Error updating order plan status ${planId}:`, error);
    throw new Error(`Failed to update order plan status: ${error.message}`);
  }
}

/**
 * Get order plan count
 * @param {Object} filters - Optional filters
 * @returns {Promise<number>} Number of order plans
 */
export async function getOrderPlanCount(filters = {}) {
  try {
    if (filters.symbol) {
      return await redis.scard(`orderplans:symbol:${filters.symbol}`);
    } else if (filters.exchange) {
      return await redis.scard(`orderplans:exchange:${filters.exchange}`);
    } else {
      return await redis.scard('orderplans:all');
    }
  } catch (error) {
    logger.error(`Error getting order plan count:`, error);
    throw new Error(`Failed to get order plan count: ${error.message}`);
  }
}

/**
 * Update order plan
 * @param {string} planId - Plan ID
 * @param {Object} updates - Fields to update
 * @returns {Promise<Object|null>} Updated plan or null if not found
 */
export async function updateOrderPlan(planId, updates) {
  try {
    const key = `orderplan:${planId}`;
    const planData = await redis.get(key);
    
    if (!planData) {
      return null;
    }
    
    const plan = JSON.parse(planData);
    
    // Don't allow updating certain fields
    const updatesFiltered = { ...updates };
    delete updatesFiltered.id;
    delete updatesFiltered.createdAt;
    
    // Apply updates
    const updatedPlan = {
      ...plan,
      ...updatesFiltered,
      updatedAt: new Date().toISOString()
    };
    
    await redis.set(key, JSON.stringify(updatedPlan));
    
    logger.info(`Order plan ${planId} updated successfully`);
    
    return updatedPlan;
  } catch (error) {
    logger.error(`Error updating order plan ${planId}:`, error);
    throw new Error(`Failed to update order plan: ${error.message}`);
  }
}