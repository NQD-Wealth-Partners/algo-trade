import * as angelOneService from '../services/angelOneService.js';
import * as userAuthService from '../services/userAuthService.js';
import asyncHandler from '../utils/asyncHandler.js';
import { successResponse, errorResponse } from '../utils/responseFormatter.js';
import logger from '../utils/logger.js';

/**
 * Link Angel One account
 * @route POST /api/v1/angelone/link
 * @access Private (requires authentication)
 */
export const linkAccount = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const { clientCode, password, totpSecret } = req.body;
    
    if (!clientCode || !password || !totpSecret) {
      return errorResponse(res, 'Please provide clientCode, password, and totpSecret', 400);
    }
    
    const linkResult = await angelOneService.linkAngelOneAccount(userId, {
      clientCode,
      password,
      totpSecret
    });
    
    return successResponse(res, linkResult);
  } catch (error) {
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Unlink Angel One account
 * @route DELETE /api/v1/angelone/link
 * @access Private (requires authentication)
 */
export const unlinkAccount = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    
    const result = await angelOneService.deleteAngelOneCredentials(userId);
    
    if (result) {
      return successResponse(res, {
        success: true,
        message: 'Successfully unlinked Angel One account'
      });
    } else {
      return errorResponse(res, 'No Angel One account linked to this user', 404);
    }
  } catch (error) {
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get Angel One authentication tokens
 * @route GET /api/v1/angelone/token
 * @access Private (requires authentication)
 */
export const getAuthToken = asyncHandler(async (req, res) => {
  const userId = req.user.id;
  
  // Check if force refresh is requested
  const forceRefresh = req.query.refresh === 'true';
  
  try {
    // Check if user has credentials
    const credentials = await angelOneService.getAngelOneCredentials(userId);
    
    if (!credentials) {
      return errorResponse(res, `No Angel One account linked. Please link your account first.`, 404);
    }
    
    // Get authentication tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId, forceRefresh);
    
    // Return only the necessary data (exclude the SmartAPI instance)
    const responseData = {
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      feed_token: tokenData.feed_token,
      client_code: tokenData.client_code,
      login_time: tokenData.login_time,
      expires_in: tokenData.expires_in
    };
    
    logger.debug(`Returning Angel One tokens for user: ${userId}`);
    return successResponse(res, responseData);
  } catch (error) {
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Get Angel One user profile
 * @route GET /api/v1/angelone/profile
 * @access Private (requires authentication)
 */
export const getProfile = asyncHandler(async (req, res) => {
  const userId = req.user.id;
  
  try {
    const profile = await angelOneService.getAngelOneProfile(userId);
    logger.debug(`Retrieved Angel One profile for user: ${userId}`);
    return successResponse(res, profile);
  } catch (error) {
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Refresh Angel One tokens
 * @route POST /api/v1/angelone/refresh
 * @access Private (requires authentication)
 */
export const refreshTokens = asyncHandler(async (req, res) => {
  const userId = req.user.id;
  
  try {
    // Invalidate current tokens
    await angelOneService.invalidateAngelOneTokens(userId);
    
    // Get fresh tokens
    const tokenData = await angelOneService.getAngelOneTokens(userId, true);
    
    // Return only the necessary data
    const responseData = {
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      feed_token: tokenData.feed_token,
      client_code: tokenData.client_code,
      login_time: tokenData.login_time,
      expires_in: tokenData.expires_in
    };
    
    logger.debug(`Refreshed Angel One tokens for user: ${userId}`);
    return successResponse(res, responseData);
  } catch (error) {
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Check Angel One link status
 * @route GET /api/v1/angelone/status
 * @access Private (requires authentication)
 */
export const getLinkStatus = asyncHandler(async (req, res) => {
  const userId = req.user.id;
  
  try {
    const credentials = await angelOneService.getAngelOneCredentials(userId);
    const clientCode = await userAuthService.getAngelOneClientCode(userId);
    
    return successResponse(res, {
      linked: !!credentials,
      clientCode: clientCode || (credentials ? credentials.clientCode : null)
    });
  } catch (error) {
    return errorResponse(res, error.message, 400);
  }
});