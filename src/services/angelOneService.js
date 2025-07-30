import { SmartAPI } from 'smartapi-javascript';
import speakeasy from 'speakeasy';
import config from '../config/index.js';
import logger from '../utils/logger.js';
import * as cacheService from './cacheService.js';
import { getRedisClient } from '../config/redis.js';
import * as userAuthService from './userAuthService.js';

// Redis keys for Angel One credentials and tokens
const ANGEL_ONE_CREDENTIALS_KEY_PREFIX = 'angelone:credentials:';
const ANGEL_ONE_TOKENS_KEY_PREFIX = 'angelone:tokens:';

/**
 * Parse the TOTP parameters from otpauth URL
 * @param {string} uri - TOTP URI in otpauth format
 * @returns {Object} TOTP parameters
 */
function parseOTPAuthURI(uri) {
  try {
    const url = new URL(uri);
    const params = url.searchParams;
    
    return {
      secret: params.get('secret'),
      algorithm: (params.get('algorithm') || 'SHA1').toUpperCase(),
      digits: parseInt(params.get('digits') || '6', 10),
      period: parseInt(params.get('period') || '30', 10)
    };
  } catch (error) {
    logger.error('Error parsing TOTP URI:', error);
    return null;
  }
}

/**
 * Generate TOTP code for Angel One authentication
 * @param {string} totpSecret - TOTP secret or otpauth URL
 * @returns {string} 6-digit TOTP code
 */
export function generateTOTP(totpSecret) {
  try {
    let secret, algorithm, digits, period;
    
    // Check if the secret is an otpauth URL
    if (totpSecret.startsWith('otpauth://')) {
      const params = parseOTPAuthURI(totpSecret);
      if (!params) {
        throw new Error('Invalid TOTP URI');
      }
      
      secret = params.secret;
      algorithm = params.algorithm;
      digits = params.digits;
      period = params.period;
    } else {
      // Assume it's just the secret
      secret = totpSecret;
      algorithm = 'SHA1';
      digits = 6;
      period = 30;
    }
    
    return speakeasy.totp({
      secret: secret,
      encoding: 'base32',
      algorithm: algorithm.toLowerCase(),
      digits: digits,
      period: period
    });
  } catch (error) {
    logger.error('Error generating TOTP:', error);
    throw new Error('Failed to generate TOTP');
  }
}

/**
 * Link Angel One account and store credentials
 * @param {string} userId - User ID
 * @param {Object} angelOneData - Angel One login data
 * @returns {Promise<Object>} Link result
 */
export async function linkAngelOneAccount(userId, angelOneData) {
  try {
    const { clientCode, password, totpSecret } = angelOneData;
    
    if (!clientCode || !password || !totpSecret) {
      throw new Error('Client code, password, and TOTP secret are required');
    }
    
    const normalizedClientCode = clientCode.toUpperCase();
    
    logger.info(`Linking Angel One account for user: ${userId}`);
    
    // Link client code to user profile
    await userAuthService.linkAngelOneClientCode(userId, normalizedClientCode);
    
    // Store credentials in Redis
    const redisClient = getRedisClient();
    const credentialKey = `${ANGEL_ONE_CREDENTIALS_KEY_PREFIX}${userId}`;
    
    await redisClient.hset(credentialKey, {
      clientCode: normalizedClientCode,
      password,
      totpSecret
    });
    
    // Set expiry for credentials (30 days)
    await redisClient.expire(credentialKey, 30 * 24 * 60 * 60);
    
    // Try to get tokens to validate credentials
    try {
      const tokens = await getAngelOneTokens(userId, true);
      
      return {
        success: true,
        message: 'Successfully linked Angel One account',
        clientCode: normalizedClientCode,
        accessToken: tokens.access_token
      };
    } catch (tokenError) {
      // Delete the stored credentials if token generation fails
      await deleteAngelOneCredentials(userId);
      
      // Remove client code link from user profile
      const redisClient = getRedisClient();
      await redisClient.hdel(`${USER_PREFIX}${userId}`, 'angelOneClientCode', 'angelOneLinkedAt');
      
      throw new Error(`Invalid Angel One credentials: ${tokenError.message}`);
    }
  } catch (error) {
    logger.error(`Angel One account linking failed for user ${userId}:`, error);
    throw error;
  }
}

/**
 * Get stored Angel One credentials for a user
 * @param {string} userId - User ID
 * @returns {Promise<Object|null>} User credentials
 */
export async function getAngelOneCredentials(userId) {
  try {
    const redisClient = getRedisClient();
    const credentialKey = `${ANGEL_ONE_CREDENTIALS_KEY_PREFIX}${userId}`;
    
    const credentials = await redisClient.hgetall(credentialKey);
    
    if (!credentials || Object.keys(credentials).length === 0) {
      logger.warn(`No Angel One credentials found for user: ${userId}`);
      return null;
    }
    
    return credentials;
  } catch (error) {
    logger.error(`Failed to get Angel One credentials for user ${userId}:`, error);
    throw new Error('Failed to retrieve Angel One credentials');
  }
}

/**
 * Delete Angel One credentials for a user
 * @param {string} userId - User ID
 * @returns {Promise<boolean>} Success status
 */
export async function deleteAngelOneCredentials(userId) {
  try {
    const redisClient = getRedisClient();
    
    // Delete credentials
    const credentialKey = `${ANGEL_ONE_CREDENTIALS_KEY_PREFIX}${userId}`;
    await redisClient.del(credentialKey);
    
    // Delete tokens
    const tokenKey = `${ANGEL_ONE_TOKENS_KEY_PREFIX}${userId}`;
    await redisClient.del(tokenKey);
    
    // Remove client code link from user profile
    await redisClient.hdel(`${USER_PREFIX}${userId}`, 'angelOneClientCode', 'angelOneLinkedAt');
    
    logger.info(`Deleted Angel One credentials for user: ${userId}`);
    return true;
  } catch (error) {
    logger.error(`Failed to delete Angel One credentials for user ${userId}:`, error);
    return false;
  }
}

/**
 * Authenticate with Angel One API and get tokens
 * @param {string} userId - User ID 
 * @param {boolean} forceRefresh - Force refresh token even if cached
 * @returns {Promise<Object>} Authentication tokens and details
 */
export async function getAngelOneTokens(userId, forceRefresh = false) {
  try {
    // Check if we have tokens cached already
    if (!forceRefresh) {
      const cachedTokens = await cacheService.get(`${ANGEL_ONE_TOKENS_KEY_PREFIX}${userId}`);
      if (cachedTokens) {
        logger.debug(`Using cached Angel One tokens for user: ${userId}`);
        return cachedTokens;
      }
    }
    
    // Get credentials for this user
    const credentials = await getAngelOneCredentials(userId);
    
    if (!credentials) {
      throw new Error(`No Angel One credentials found for user: ${userId}`);
    }
    
    logger.info(`Authenticating with Angel One for user: ${userId}`);
    
    // Generate fresh TOTP
    const totp = generateTOTP(credentials.totpSecret);
    logger.debug(`Generated TOTP for user ${userId}`);
    
    // Initialize SmartAPI
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey
    });
    
    // Generate session
    const sessionData = await smartApi.generateSession(
      credentials.clientCode,
      credentials.password,
      totp
    );
    
    // Extract token data
    const tokenData = {
      access_token: sessionData.data.jwtToken,
      refresh_token: sessionData.data.refreshToken,
      feed_token: sessionData.data.feedToken,
      client_code: sessionData.data.clientcode,
      login_time: new Date().toISOString(),
      expires_in: sessionData.data.tokenExpiryTime
    };
    
    // Cache tokens - set expiry to 90% of token lifetime to ensure we refresh before expiry
    // Default to 6 hours (21600 seconds) if expiry time is not provided
    const expirySeconds = tokenData.expires_in ? 
      parseInt(tokenData.expires_in) * 0.9 : 
      21600;
    
    await cacheService.set(`${ANGEL_ONE_TOKENS_KEY_PREFIX}${userId}`, tokenData, expirySeconds);
    
    logger.info(`Successfully authenticated with Angel One for user: ${userId}`);
    return tokenData;
  } catch (error) {
    logger.error(`Angel One authentication failed for user ${userId}:`, error);
    throw new Error(`Angel One authentication failed: ${error.message}`);
  }
}

/**
 * Get user profile from Angel One
 * @param {string} userId - User ID
 * @returns {Promise<Object>} User profile data
 */
export async function getAngelOneProfile(userId) {
  try {
    const tokenData = await getAngelOneTokens(userId);
    const smartApi = new SmartAPI({
      api_key: config.angelOne.apiKey,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token

    });
    return await smartApi.getProfile();
  } catch (error) {
    logger.error(`Failed to get Angel One profile for user ${userId}:`, error);
    throw new Error(`Failed to get Angel One profile: ${error.message}`);
  }
}

/**
 * Invalidate cached Angel One tokens
 * @param {string} userId - User ID
 * @returns {Promise<boolean>} Success status
 */
export async function invalidateAngelOneTokens(userId) {
  return await cacheService.invalidate(`${ANGEL_ONE_TOKENS_KEY_PREFIX}${userId}`);
}

/**
 * List all users with Angel One credentials
 * @returns {Promise<Array>} List of user IDs
 */
export async function listAngelOneUsers() {
  try {
    const redisClient = getRedisClient();
    const keys = await redisClient.keys(`${ANGEL_ONE_CREDENTIALS_KEY_PREFIX}*`);
    
    // Extract just the user IDs from the keys
    return keys.map(key => key.replace(ANGEL_ONE_CREDENTIALS_KEY_PREFIX, ''));
  } catch (error) {
    logger.error('Failed to list Angel One users:', error);
    throw new Error('Failed to list Angel One users');
  }
}