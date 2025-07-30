import * as orderService from '../services/orderService.js';
import asyncHandler from '../utils/asyncHandler.js';
import { successResponse, errorResponse } from '../utils/responseFormatter.js';
import logger from '../utils/logger.js';

/**
 * Place a new order
 * @route POST /api/v1/orders
 * @access Private
 */
export const placeOrder = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const orderParams = req.body;
    
    const result = await orderService.placeOrder(userId, orderParams);
    
    return successResponse(res, {
      message: 'Order placed successfully',
      orderid: result.data,
      status: result.status
    });
  } catch (error) {
    logger.error(`Order placement error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Modify an existing order
 * @route PUT /api/v1/orders/:orderId
 * @access Private
 */
export const modifyOrder = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { orderId } = req.params;
    const modifyParams = {
      ...req.body,
      orderid: orderId
    };
    
    const result = await orderService.modifyOrder(userId, modifyParams);
    
    return successResponse(res, {
      message: 'Order modified successfully',
      orderid: result.data,
      status: result.status
    });
  } catch (error) {
    logger.error(`Order modification error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Cancel an order
 * @route DELETE /api/v1/orders/:orderId
 * @access Private
 */
export const cancelOrder = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { orderId } = req.params;
    const { variety } = req.body;
    
    if (!variety) {
      return errorResponse(res, 'Variety is required', 400);
    }
    
    const result = await orderService.cancelOrder(userId, { 
      orderid: orderId,
      variety: variety
    });
    
    return successResponse(res, {
      message: 'Order cancelled successfully',
      orderid: result.data,
      status: result.status
    });
  } catch (error) {
    logger.error(`Order cancellation error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get order book (list of orders)
 * @route GET /api/v1/orders
 * @access Private
 */
export const getOrderBook = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    
    const result = await orderService.getOrderBook(userId);
    
    return successResponse(res, {
      message: 'Order book retrieved successfully',
      orders: result.data
    });
  } catch (error) {
    logger.error(`Order book retrieval error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get order details
 * @route GET /api/v1/orders/:orderId
 * @access Private
 */
export const getOrderDetails = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { orderId } = req.params;
    
    const result = await orderService.getOrderDetails(userId, orderId);
    
    return successResponse(res, {
      message: 'Order details retrieved successfully',
      order: result
    });
  } catch (error) {
    logger.error(`Order details retrieval error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get positions (day and holdings)
 * @route GET /api/v1/orders/positions
 * @access Private
 */
export const getPositions = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    
    const result = await orderService.getPositions(userId);
    
    return successResponse(res, {
      message: 'Positions retrieved successfully',
      positions: result.data
    });
  } catch (error) {
    logger.error(`Positions retrieval error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get holdings
 * @route GET /api/v1/orders/holdings
 * @access Private
 */
export const getHoldings = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    
    const result = await orderService.getHoldings(userId);
    
    return successResponse(res, {
      message: 'Holdings retrieved successfully',
      holdings: result.data
    });
  } catch (error) {
    logger.error(`Holdings retrieval error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Convert position
 * @route POST /api/v1/orders/positions/convert
 * @access Private
 */
export const convertPosition = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const conversionParams = req.body;
    
    const result = await orderService.convertPosition(userId, conversionParams);
    
    return successResponse(res, {
      message: 'Position converted successfully',
      data: result.data,
      status: result.status
    });
  } catch (error) {
    logger.error(`Position conversion error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Place a GTT order
 * @route POST /api/v1/orders/gtt
 * @access Private
 */
export const placeGttOrder = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const gttParams = req.body;
    
    const result = await orderService.placeGttOrder(userId, gttParams);
    
    return successResponse(res, {
      message: 'GTT order placed successfully',
      ruleId: result.data?.id,
      status: result.status
    });
  } catch (error) {
    logger.error(`GTT order placement error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get GTT rules
 * @route GET /api/v1/orders/gtt
 * @access Private
 */
export const getGttRules = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { status } = req.query;
    
    const result = await orderService.getGttRules(userId, status);
    
    return successResponse(res, {
      message: 'GTT rules retrieved successfully',
      rules: result.data
    });
  } catch (error) {
    logger.error(`GTT rules retrieval error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get specific GTT rule
 * @route GET /api/v1/orders/gtt/:ruleId
 * @access Private
 */
export const getGttRule = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { ruleId } = req.params;
    
    const result = await orderService.getGttRule(userId, ruleId);
    
    return successResponse(res, {
      message: 'GTT rule retrieved successfully',
      rule: result.data
    });
  } catch (error) {
    logger.error(`GTT rule retrieval error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Modify GTT rule
 * @route PUT /api/v1/orders/gtt/:ruleId
 * @access Private
 */
export const modifyGttRule = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { ruleId } = req.params;
    const modifyParams = {
      ...req.body,
      id: ruleId
    };
    
    const result = await orderService.modifyGttRule(userId, modifyParams);
    
    return successResponse(res, {
      message: 'GTT rule modified successfully',
      ruleId: result.data,
      status: result.status
    });
  } catch (error) {
    logger.error(`GTT rule modification error:`, error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Cancel GTT rule
 * @route DELETE /api/v1/orders/gtt/:ruleId
 * @access Private
 */
export const cancelGttRule = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { ruleId } = req.params;
    const { symboltoken, exchange } = req.body;
    
    if (!symboltoken || !exchange) {
      return errorResponse(res, 'Symbol token and exchange are required', 400);
    }
    
    const result = await orderService.cancelGttRule(userId, ruleId, symboltoken, exchange);
    
    return successResponse(res, {
      message: 'GTT rule cancelled successfully',
      ruleId: result.data,
      status: result.status
    });
  } catch (error) {
    logger.error(`GTT rule cancellation error:`, error);
    return errorResponse(res, error.message, 400);
  }
});