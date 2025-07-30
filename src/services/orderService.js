import { SmartAPI } from 'smartapi-javascript';
import logger from '../utils/logger.js';
import * as angelOneService from './angelOneService.js';
import config from '../config/index.js';

/**
 * Place a new order
 * @param {string} userId - User ID
 * @param {Object} orderParams - Order parameters
 * @returns {Promise<Object>} Order response
 */
export async function placeOrder(userId, orderParams) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Validate order parameters
    validateOrderParams(orderParams);
    
    // Place order using SmartAPI
    logger.info(`Placing order for user ${userId}: ${JSON.stringify(orderParams)}`);
    const response = await smartApi.placeOrder(orderParams);
    
    logger.info(`Order placed successfully for user ${userId}: ${JSON.stringify(response.data)}`);
    return response;
  } catch (error) {
    logger.error(`Error placing order for user ${userId}:`, error);
    throw new Error(`Failed to place order: ${error.message}`);
  }
}

/**
 * Modify an existing order
 * @param {string} userId - User ID
 * @param {Object} modifyParams - Order modification parameters
 * @returns {Promise<Object>} Modification response
 */
export async function modifyOrder(userId, modifyParams) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Validate required parameters
    if (!modifyParams.orderid) {
      throw new Error('Order ID is required');
    }
    
    // Modify order using SmartAPI
    logger.info(`Modifying order for user ${userId}: ${JSON.stringify(modifyParams)}`);
    const response = await smartApi.modifyOrder(modifyParams);
    
    logger.info(`Order modified successfully for user ${userId}: ${JSON.stringify(response.data)}`);
    return response;
  } catch (error) {
    logger.error(`Error modifying order for user ${userId}:`, error);
    throw new Error(`Failed to modify order: ${error.message}`);
  }
}

/**
 * Cancel an order
 * @param {string} userId - User ID
 * @param {Object} cancelParams - Order cancellation parameters
 * @returns {Promise<Object>} Cancellation response
 */
export async function cancelOrder(userId, cancelParams) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Validate required parameters
    if (!cancelParams.orderid) {
      throw new Error('Order ID is required');
    }
    
    if (!cancelParams.variety) {
      throw new Error('Variety is required');
    }
    
    // Cancel order using SmartAPI
    logger.info(`Cancelling order for user ${userId}: ${JSON.stringify(cancelParams)}`);
    const response = await smartApi.cancelOrder(cancelParams);
    
    logger.info(`Order cancelled successfully for user ${userId}: ${JSON.stringify(response.data)}`);
    return response;
  } catch (error) {
    logger.error(`Error cancelling order for user ${userId}:`, error);
    throw new Error(`Failed to cancel order: ${error.message}`);
  }
}

/**
 * Get order book (list of orders)
 * @param {string} userId - User ID
 * @returns {Promise<Array>} List of orders
 */
export async function getOrderBook(userId) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Get order book using SmartAPI
    logger.info(`Fetching order book for user ${userId}`);
    const response = await smartApi.getOrderBook();
    
    logger.info(`Retrieved order book for user ${userId}`);
    return response;
  } catch (error) {
    logger.error(`Error fetching order book for user ${userId}:`, error);
    throw new Error(`Failed to fetch order book: ${error.message}`);
  }
}

/**
 * Get individual order details
 * @param {string} userId - User ID
 * @param {string} orderId - Order ID
 * @returns {Promise<Object>} Order details
 */
export async function getOrderDetails(userId, orderId) {
  try {
    // Get order book
    const orderBook = await getOrderBook(userId);
    
    // Find the specific order
    const order = orderBook.data.find(order => order.orderid === orderId);
    
    if (!order) {
      throw new Error(`Order with ID ${orderId} not found`);
    }
    
    return order;
  } catch (error) {
    logger.error(`Error fetching order details for user ${userId}, order ${orderId}:`, error);
    throw new Error(`Failed to fetch order details: ${error.message}`);
  }
}

/**
 * Get positions (day and holdings)
 * @param {string} userId - User ID
 * @returns {Promise<Object>} Positions data
 */
export async function getPositions(userId) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Get positions using SmartAPI
    logger.info(`Fetching positions for user ${userId}`);
    const response = await smartApi.getPosition();
    
    logger.info(`Retrieved positions for user ${userId}`);
    return response;
  } catch (error) {
    logger.error(`Error fetching positions for user ${userId}:`, error);
    throw new Error(`Failed to fetch positions: ${error.message}`);
  }
}

/**
 * Get holding positions
 * @param {string} userId - User ID
 * @returns {Promise<Object>} Holdings data
 */
export async function getHoldings(userId) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Get holdings using SmartAPI
    logger.info(`Fetching holdings for user ${userId}`);
    const response = await smartApi.getHolding();
    
    logger.info(`Retrieved holdings for user ${userId}`);
    return response;
  } catch (error) {
    logger.error(`Error fetching holdings for user ${userId}:`, error);
    throw new Error(`Failed to fetch holdings: ${error.message}`);
  }
}

/**
 * Convert positions (day to delivery or vice versa)
 * @param {string} userId - User ID
 * @param {Object} conversionParams - Position conversion parameters
 * @returns {Promise<Object>} Conversion response
 */
export async function convertPosition(userId, conversionParams) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Validate required parameters
    if (!conversionParams.symboltoken || !conversionParams.exchange || !conversionParams.oldproducttype || !conversionParams.newproducttype || !conversionParams.tradingsymbol || !conversionParams.quantity) {
      throw new Error('Missing required parameters for position conversion');
    }
    
    // Convert position using SmartAPI
    logger.info(`Converting position for user ${userId}: ${JSON.stringify(conversionParams)}`);
    const response = await smartApi.convertPosition(conversionParams);
    
    logger.info(`Position converted successfully for user ${userId}: ${JSON.stringify(response.data)}`);
    return response;
  } catch (error) {
    logger.error(`Error converting position for user ${userId}:`, error);
    throw new Error(`Failed to convert position: ${error.message}`);
  }
}

/**
 * Place a GTT (Good Till Triggered) order
 * @param {string} userId - User ID
 * @param {Object} gttParams - GTT order parameters
 * @returns {Promise<Object>} GTT order response
 */
export async function placeGttOrder(userId, gttParams) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Validate required parameters
    validateGttOrderParams(gttParams);
    
    // Place GTT order using SmartAPI
    logger.info(`Placing GTT order for user ${userId}: ${JSON.stringify(gttParams)}`);
    const response = await smartApi.gttCreateRule(gttParams);
    
    logger.info(`GTT order placed successfully for user ${userId}: ${JSON.stringify(response.data)}`);
    return response;
  } catch (error) {
    logger.error(`Error placing GTT order for user ${userId}:`, error);
    throw new Error(`Failed to place GTT order: ${error.message}`);
  }
}

/**
 * Get GTT (Good Till Triggered) rules
 * @param {string} userId - User ID
 * @param {string} status - Rule status filter
 * @returns {Promise<Object>} GTT rules
 */
export async function getGttRules(userId, status) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Get GTT rules using SmartAPI
    logger.info(`Fetching GTT rules for user ${userId}`);
    const response = await smartApi.gttRuleList(status);
    
    logger.info(`Retrieved GTT rules for user ${userId}`);
    return response;
  } catch (error) {
    logger.error(`Error fetching GTT rules for user ${userId}:`, error);
    throw new Error(`Failed to fetch GTT rules: ${error.message}`);
  }
}

/**
 * Get specific GTT (Good Till Triggered) rule
 * @param {string} userId - User ID
 * @param {string} ruleId - GTT rule ID
 * @returns {Promise<Object>} GTT rule details
 */
export async function getGttRule(userId, ruleId) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Get GTT rule using SmartAPI
    logger.info(`Fetching GTT rule for user ${userId}, rule ${ruleId}`);
    const response = await smartApi.gttRuleDetails(ruleId);
    
    logger.info(`Retrieved GTT rule for user ${userId}, rule ${ruleId}`);
    return response;
  } catch (error) {
    logger.error(`Error fetching GTT rule for user ${userId}, rule ${ruleId}:`, error);
    throw new Error(`Failed to fetch GTT rule: ${error.message}`);
  }
}

/**
 * Modify GTT (Good Till Triggered) rule
 * @param {string} userId - User ID
 * @param {Object} modifyParams - GTT rule modification parameters
 * @returns {Promise<Object>} Modification response
 */
export async function modifyGttRule(userId, modifyParams) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Validate required parameters
    if (!modifyParams.id) {
      throw new Error('GTT rule ID is required');
    }
    
    // Modify GTT rule using SmartAPI
    logger.info(`Modifying GTT rule for user ${userId}: ${JSON.stringify(modifyParams)}`);
    const response = await smartApi.gttModifyRule(modifyParams);
    
    logger.info(`GTT rule modified successfully for user ${userId}: ${JSON.stringify(response.data)}`);
    return response;
  } catch (error) {
    logger.error(`Error modifying GTT rule for user ${userId}:`, error);
    throw new Error(`Failed to modify GTT rule: ${error.message}`);
  }
}

/**
 * Cancel GTT (Good Till Triggered) rule
 * @param {string} userId - User ID
 * @param {string} ruleId - GTT rule ID
 * @param {string} symbolToken - Symbol token
 * @param {string} exchange - Exchange
 * @returns {Promise<Object>} Cancellation response
 */
export async function cancelGttRule(userId, ruleId, symbolToken, exchange) {
  try {
    // Get Angel One tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId);
    
    // Initialize SmartAPI with tokens
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token
    });
    
    // Cancel GTT rule using SmartAPI
    logger.info(`Cancelling GTT rule for user ${userId}, rule ${ruleId}`);
    const response = await smartApi.gttCancelRule({
      id: ruleId,
      symboltoken: symbolToken,
      exchange: exchange
    });
    
    logger.info(`GTT rule cancelled successfully for user ${userId}, rule ${ruleId}`);
    return response;
  } catch (error) {
    logger.error(`Error cancelling GTT rule for user ${userId}, rule ${ruleId}:`, error);
    throw new Error(`Failed to cancel GTT rule: ${error.message}`);
  }
}

/**
 * Validate order parameters
 * @param {Object} params - Order parameters to validate
 * @throws {Error} If validation fails
 */
function validateOrderParams(params) {
  const requiredParams = [
    'variety',
    'tradingsymbol',
    'symboltoken',
    'transactiontype',
    'exchange',
    'ordertype',
    'producttype',
    'duration',
    'price',
    'quantity'
  ];
  
  for (const param of requiredParams) {
    if (!params[param] && params[param] !== 0) {
      throw new Error(`Missing required parameter: ${param}`);
    }
  }
  
  // Validate transaction type
  if (!['BUY', 'SELL'].includes(params.transactiontype)) {
    throw new Error('Transaction type must be BUY or SELL');
  }
  
  // Validate order type
  if (!['MARKET', 'LIMIT', 'STOPLOSS_LIMIT', 'STOPLOSS_MARKET'].includes(params.ordertype)) {
    throw new Error('Invalid order type');
  }
  
  // Validate exchange
  if (!['NSE', 'BSE', 'NFO', 'BFO', 'CDS', 'MCX'].includes(params.exchange)) {
    throw new Error('Invalid exchange');
  }
  
  // Validate product type
  if (!['DELIVERY', 'CARRYFORWARD', 'MARGIN', 'INTRADAY', 'BO'].includes(params.producttype)) {
    throw new Error('Invalid product type');
  }
  
  // Validate duration
  if (!['DAY', 'IOC'].includes(params.duration)) {
    throw new Error('Duration must be DAY or IOC');
  }
}

/**
 * Validate GTT order parameters
 * @param {Object} params - GTT parameters to validate
 * @throws {Error} If validation fails
 */
function validateGttOrderParams(params) {
  if (!params.tradingsymbol || !params.symboltoken || !params.exchange || !params.producttype || !params.triggerprices || !params.orders) {
    throw new Error('Missing required GTT parameters');
  }
  
  if (!Array.isArray(params.triggerprices) || params.triggerprices.length === 0) {
    throw new Error('Trigger prices must be a non-empty array');
  }
  
  if (!Array.isArray(params.orders) || params.orders.length === 0) {
    throw new Error('Orders must be a non-empty array');
  }
  
  // Validate each order
  for (const order of params.orders) {
    if (!order.transactiontype || !order.quantity || !order.price) {
      throw new Error('Each order must have transaction type, quantity, and price');
    }
    
    if (!['BUY', 'SELL'].includes(order.transactiontype)) {
      throw new Error('Transaction type must be BUY or SELL');
    }
  }
}