import asyncHandler from '../utils/asyncHandler.js';
import { successResponse, errorResponse } from '../utils/responseFormatter.js';
import * as orderPlanRedisService from '../services/orderPlanRedisService.js';
import logger from '../utils/logger.js';

/**
 * Create order plan
 * @route POST /api/v1/orderplan
 * @access Public
 */
export const createOrderPlan = asyncHandler(async (req, res) => {
  try {
    const orderPlanData = req.body;
    
    // Validate required parameters
    const requiredParams = [
      'variety', 'tradingsymbol', 'symboltoken', 'transactiontype', 
      'exchange', 'ordertype', 'producttype', 'duration',
      'quantity', 'lotsize', 'exit_tick_size'
    ];
    
    for (const param of requiredParams) {
      if (!orderPlanData[param]) {
        return errorResponse(res, `Parameter ${param} is required`, 400);
      }
    }
    
    // Store the order plan in Redis
    const storedPlan = await orderPlanRedisService.storeOrderPlan(orderPlanData);
    
    return successResponse(res, {
      message: 'Order plan created successfully',
      plan: storedPlan
    });
  } catch (error) {
    logger.error(`Order plan creation error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get order plan by ID
 * @route GET /api/v1/orderplan/:id
 * @access Public
 */
export const getOrderPlan = asyncHandler(async (req, res) => {
  try {
    const { id } = req.params;
    
    const plan = await orderPlanRedisService.getOrderPlan(id);
    
    if (!plan) {
      return errorResponse(res, `Order plan with ID ${id} not found`, 404);
    }
    
    return successResponse(res, {
      message: 'Order plan retrieved successfully',
      plan
    });
  } catch (error) {
    logger.error(`Get order plan error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get all order plans with optional filters
 * @route GET /api/v1/orderplan
 * @access Public
 */
export const getAllOrderPlans = asyncHandler(async (req, res) => {
  try {
    const { symbol, exchange, status, limit = 100, offset = 0 } = req.query;
    
    const filters = {};
    if (symbol) filters.symbol = symbol;
    if (exchange) filters.exchange = exchange;
    if (status) filters.status = status;
    
    const plans = await orderPlanRedisService.getAllOrderPlans(
      filters, 
      parseInt(limit), 
      parseInt(offset)
    );
    
    // Get total count for pagination
    const totalCount = await orderPlanRedisService.getOrderPlanCount(filters);
    
    return successResponse(res, {
      message: 'Order plans retrieved successfully',
      count: plans.length,
      total: totalCount,
      hasMore: parseInt(offset) + plans.length < totalCount,
      plans
    });
  } catch (error) {
    logger.error(`Get order plans error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Delete order plan
 * @route DELETE /api/v1/orderplan/:id
 * @access Public
 */
export const deleteOrderPlan = asyncHandler(async (req, res) => {
  try {
    const { id } = req.params;
    
    const deleted = await orderPlanRedisService.deleteOrderPlan(id);
    
    if (!deleted) {
      return errorResponse(res, `Order plan with ID ${id} not found`, 404);
    }
    
    return successResponse(res, {
      message: 'Order plan deleted successfully'
    });
  } catch (error) {
    logger.error(`Delete order plan error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Update order plan status
 * @route PATCH /api/v1/orderplan/:id/status
 * @access Public
 */
export const updateOrderPlanStatus = asyncHandler(async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    
    if (!status) {
      return errorResponse(res, 'Status is required', 400);
    }
    
    const validStatuses = ['CREATED', 'READY', 'EXECUTING', 'EXECUTED', 'CANCELLED', 'FAILED'];
    if (!validStatuses.includes(status)) {
      return errorResponse(res, `Invalid status. Must be one of: ${validStatuses.join(', ')}`, 400);
    }
    
    const updatedPlan = await orderPlanRedisService.updateOrderPlanStatus(id, status);
    
    if (!updatedPlan) {
      return errorResponse(res, `Order plan with ID ${id} not found`, 404);
    }
    
    return successResponse(res, {
      message: 'Order plan status updated successfully',
      plan: updatedPlan
    });
  } catch (error) {
    logger.error(`Update order plan status error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Update order plan
 * @route PUT /api/v1/orderplan/:id
 * @access Public
 */
export const updateOrderPlan = asyncHandler(async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;
    
    if (Object.keys(updates).length === 0) {
      return errorResponse(res, 'No updates provided', 400);
    }
    
    const updatedPlan = await orderPlanRedisService.updateOrderPlan(id, updates);
    
    if (!updatedPlan) {
      return errorResponse(res, `Order plan with ID ${id} not found`, 404);
    }
    
    return successResponse(res, {
      message: 'Order plan updated successfully',
      plan: updatedPlan
    });
  } catch (error) {
    logger.error(`Update order plan error:`, error);
    return errorResponse(res, error.message, 400);
  }
});