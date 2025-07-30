import asyncHandler from '../utils/asyncHandler.js';
import { successResponse, errorResponse } from '../utils/responseFormatter.js';
import * as redisScriptService from '../services/redisScriptService.js';
import * as scriptConversionService from '../services/scriptConversionService.js';
import * as scriptBackgroundService from '../services/scriptBackgroundService.js';
import logger from '../utils/logger.js';

/**
 * Search scripts
 * @route GET /api/v1/scripts/search
 * @access Public
 */
export const searchScripts = asyncHandler(async (req, res) => {
  try {
    const { q, exchange, instrumentType, limit = 20 } = req.query;
    
    if (!q || q.length < 2) {
      return errorResponse(res, 'Search term must be at least 2 characters', 400);
    }
    
    const filters = {};
    if (exchange) filters.exchange = exchange;
    if (instrumentType) filters.instrumentType = instrumentType;
    
    const results = await redisScriptService.searchScripts(q, filters, parseInt(limit));
    
    return successResponse(res, {
      message: 'Scripts retrieved successfully',
      count: results.length,
      scripts: results
    });
  } catch (error) {
    logger.error('Script search error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get script by token
 * @route GET /api/v1/scripts/token/:token
 * @access Public
 */
export const getScriptByToken = asyncHandler(async (req, res) => {
  try {
    const { token } = req.params;
    
    const script = await redisScriptService.getScriptByToken(token);
    
    if (!script) {
      return errorResponse(res, `Script with token ${token} not found`, 404);
    }
    
    return successResponse(res, {
      message: 'Script retrieved successfully',
      script
    });
  } catch (error) {
    logger.error('Get script by token error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get script by symbol
 * @route GET /api/v1/scripts/symbol/:symbol
 * @access Public
 */
export const getScriptBySymbol = asyncHandler(async (req, res) => {
  try {
    const { symbol } = req.params;
    
    const script = await redisScriptService.getScriptBySymbol(symbol);
    
    if (!script) {
      return errorResponse(res, `Script with symbol ${symbol} not found`, 404);
    }
    
    return successResponse(res, {
      message: 'Script retrieved successfully',
      script
    });
  } catch (error) {
    logger.error('Get script by symbol error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Convert script to trading symbol
 * @route GET /api/v1/scripts/convert/:symbol
 * @access Public
 */
export const convertToTradingSymbol = asyncHandler(async (req, res) => {
  try {
    const { symbol } = req.params;
    
    const script = await redisScriptService.getScriptBySymbol(symbol);
    
    if (!script) {
      return errorResponse(res, `Script with symbol ${symbol} not found`, 404);
    }
    
    const tradingSymbol = scriptConversionService.convertToTradingSymbol(script);
    
    return successResponse(res, {
      message: 'Script converted successfully',
      originalSymbol: symbol,
      tradingSymbol,
      script
    });
  } catch (error) {
    logger.error('Convert to trading symbol error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get available exchanges
 * @route GET /api/v1/scripts/exchanges
 * @access Public
 */
export const getExchanges = asyncHandler(async (req, res) => {
  try {
    const exchanges = await redisScriptService.getAllExchanges();
    
    return successResponse(res, {
      message: 'Exchanges retrieved successfully',
      count: exchanges.length,
      exchanges
    });
  } catch (error) {
    logger.error('Get exchanges error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get available instrument types
 * @route GET /api/v1/scripts/instrument-types
 * @access Public
 */
export const getInstrumentTypes = asyncHandler(async (req, res) => {
  try {
    const instrumentTypes = await redisScriptService.getAllInstrumentTypes();
    
    return successResponse(res, {
      message: 'Instrument types retrieved successfully',
      count: instrumentTypes.length,
      instrumentTypes
    });
  } catch (error) {
    logger.error('Get instrument types error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get script statistics
 * @route GET /api/v1/scripts/stats
 * @access Public
 */
export const getScriptStats = asyncHandler(async (req, res) => {
  try {
    const count = await redisScriptService.getScriptCount();
    const exchanges = await redisScriptService.getAllExchanges();
    const instrumentTypes = await redisScriptService.getAllInstrumentTypes();
    
    return successResponse(res, {
      message: 'Script statistics retrieved successfully',
      stats: {
        totalScripts: count,
        exchangeCount: exchanges.length,
        instrumentTypeCount: instrumentTypes.length
      }
    });
  } catch (error) {
    logger.error('Get script stats error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Manually trigger script update (admin only)
 * @route POST /api/v1/scripts/update
 * @access Admin
 */
export const triggerScriptUpdate = asyncHandler(async (req, res) => {
  try {
    // This endpoint should be restricted to admin users
    // Add appropriate authentication middleware
    
    // Trigger the update
    await scriptBackgroundService.updateScriptsInRedis();
    
    return successResponse(res, {
      message: 'Script update triggered successfully'
    });
  } catch (error) {
    logger.error('Script update error:', error);
    return errorResponse(res, error.message, 400);
  }
});