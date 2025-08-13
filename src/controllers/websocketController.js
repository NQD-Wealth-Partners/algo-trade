import asyncHandler from '../utils/asyncHandler.js';
import { successResponse, errorResponse } from '../utils/responseFormatter.js';
import * as websocketService from '../services/websocketService.js';
import logger from '../utils/logger.js';

/**
 * Get current WebSocket subscriptions
 * @route GET /api/v1/websocket/subscriptions
 * @access Public
 */
export const getSubscriptions = asyncHandler(async (req, res) => {
  try {
    const subscriptions = websocketService.getCurrentSubscriptions();
    
    return successResponse(res, {
      message: 'Current subscriptions retrieved successfully',
      subscriptions
    });
  } catch (error) {
    logger.error('Get subscriptions error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get last tick for a symbol
 * @route GET /api/v1/websocket/tick/:symbol
 * @access Public
 */
export const getLastTick = asyncHandler(async (req, res) => {
  try {
    const { symbol } = req.params;
    
    const tickData = await websocketService.getLastTick(symbol);
    
    if (!tickData) {
      return errorResponse(res, `No tick data available for symbol ${symbol}`, 404);
    }
    
    return successResponse(res, {
      message: 'Tick data retrieved successfully',
      tickData
    });
  } catch (error) {
    logger.error(`Get last tick error for ${req.params.symbol}:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Manually subscribe to a symbol
 * @route POST /api/v1/websocket/subscribe
 * @access Public
 */
export const subscribeToSymbol = asyncHandler(async (req, res) => {
  try {
    const { symbol, token, exchange } = req.body;
    
    if (!symbol || !token || !exchange) {
      return errorResponse(res, 'Symbol, token, and exchange are required', 400);
    }
    
    // Create a temporary order plan
    const tempPlan = {
      id: `temp_${Date.now()}`,
      tradingsymbol: symbol,
      symboltoken: token,
      exchange,
      status: 'MONITORING',
      createdAt: new Date().toISOString()
    };
    
    // Store in Redis
    await websocketService.orderPlanService.storeOrderPlan(tempPlan);
    
    return successResponse(res, {
      message: `Successfully subscribed to ${symbol}`,
      plan: tempPlan
    });
  } catch (error) {
    logger.error('Subscribe to symbol error:', error);
    return errorResponse(res, error.message, 400);
  }
});