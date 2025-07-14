import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import { getRedisClient } from '../config/redis.js';
import config from '../config/index.js';
import logger from '../utils/logger.js';

// Redis key prefixes
const USER_PREFIX = 'user:';
const USER_MOBILE_INDEX = 'user:mobile:';
const TOKEN_BLACKLIST = 'token:blacklist:';

/**
 * Validate mobile number format (basic validation)
 * @param {string} mobile - Mobile number to validate
 * @returns {boolean} True if valid
 */
function isValidMobile(mobile) {
  // Basic validation - 10 digits, optionally with country code
  return /^(\+\d{1,3})?[0-9]{10}$/.test(mobile);
}

/**
 * Normalize mobile number to standard format
 * @param {string} mobile - Mobile number to normalize
 * @returns {string} Normalized mobile number
 */
function normalizeMobile(mobile) {
  // Remove any non-digit characters
  const digitsOnly = mobile.replace(/\D/g, '');
  
  // If it's a 10-digit number, assume Indian number and add +91
  if (digitsOnly.length === 10) {
    return `+91${digitsOnly}`;
  }
  
  // If it already has a country code, return as is with + prefix
  return `+${digitsOnly}`;
}

/**
 * Create a new user
 * @param {Object} userData - User registration data
 * @returns {Promise<Object>} Created user (without password)
 */
export async function createUser(userData) {
  try {
    const { mobile, password, name } = userData;
    
    if (!mobile || !password) {
      throw new Error('Mobile number and password are required');
    }
    
    if (!isValidMobile(mobile)) {
      throw new Error('Invalid mobile number format');
    }
    
    // Normalize mobile number
    const normalizedMobile = normalizeMobile(mobile);
    
    // Check if mobile already exists
    const redisClient = getRedisClient();
    const existingUserId = await redisClient.get(`${USER_MOBILE_INDEX}${normalizedMobile}`);
    
    if (existingUserId) {
      throw new Error('Mobile number already registered');
    }
    
    // Generate user ID
    const userId = uuidv4();
    
    // Hash password
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);
    
    // Create user object
    const user = {
      id: userId,
      mobile: normalizedMobile,
      name: name || 'User', // Default name if not provided
      hashedPassword,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    
    // Store user in Redis
    await redisClient.hset(
      `${USER_PREFIX}${userId}`,
      Object.entries(user).filter(([key]) => key !== 'hashedPassword').flat()
    );
    
    // Store password separately for better security
    await redisClient.hset(`${USER_PREFIX}${userId}:auth`, {
      hashedPassword
    });
    
    // Create mobile index
    await redisClient.set(`${USER_MOBILE_INDEX}${normalizedMobile}`, userId);
    
    // Return user without password
    const { hashedPassword: _, ...userWithoutPassword } = user;
    return userWithoutPassword;
  } catch (error) {
    logger.error('Error creating user:', error);
    throw error;
  }
}

/**
 * Get user by ID
 * @param {string} userId - User ID
 * @returns {Promise<Object|null>} User object (without password) or null
 */
export async function getUserById(userId) {
  try {
    const redisClient = getRedisClient();
    const user = await redisClient.hgetall(`${USER_PREFIX}${userId}`);
    
    if (!user || Object.keys(user).length === 0) {
      return null;
    }
    
    return user;
  } catch (error) {
    logger.error(`Error getting user by ID ${userId}:`, error);
    throw error;
  }
}

/**
 * Get user by mobile number
 * @param {string} mobile - Mobile number
 * @returns {Promise<Object|null>} User object (without password) or null
 */
export async function getUserByMobile(mobile) {
  try {
    if (!isValidMobile(mobile)) {
      throw new Error('Invalid mobile number format');
    }
    
    const normalizedMobile = normalizeMobile(mobile);
    
    const redisClient = getRedisClient();
    const userId = await redisClient.get(`${USER_MOBILE_INDEX}${normalizedMobile}`);
    
    if (!userId) {
      return null;
    }
    
    return await getUserById(userId);
  } catch (error) {
    logger.error(`Error getting user by mobile ${mobile}:`, error);
    throw error;
  }
}

/**
 * Authenticate user and generate JWT token
 * @param {Object} credentials - Login credentials
 * @returns {Promise<Object>} Authentication token and user info
 */
export async function loginUser(credentials) {
  try {
    const { mobile, password } = credentials;
    
    if (!mobile || !password) {
      throw new Error('Mobile number and password are required');
    }
    
    if (!isValidMobile(mobile)) {
      throw new Error('Invalid mobile number format');
    }
    
    const normalizedMobile = normalizeMobile(mobile);
    
    // Get user by mobile
    const redisClient = getRedisClient();
    const userId = await redisClient.get(`${USER_MOBILE_INDEX}${normalizedMobile}`);
    
    if (!userId) {
      throw new Error('Invalid mobile number or password');
    }
    
    // Get user password
    const authData = await redisClient.hgetall(`${USER_PREFIX}${userId}:auth`);
    
    if (!authData || !authData.hashedPassword) {
      throw new Error('User account is corrupted');
    }
    
    // Compare passwords
    const passwordMatch = await bcrypt.compare(password, authData.hashedPassword);
    
    if (!passwordMatch) {
      throw new Error('Invalid mobile number or password');
    }
    
    // Get user data
    const user = await getUserById(userId);
    
    // Generate JWT token
    const token = jwt.sign(
      { userId: user.id, mobile: user.mobile },
      config.jwt.secret,
      { expiresIn: config.jwt.expiresIn }
    );
    
    return {
      token,
      user
    };
  } catch (error) {
    logger.error('User login error:', error);
    throw error;
  }
}

/**
 * Logout user by blacklisting token
 * @param {string} token - JWT token to blacklist
 * @returns {Promise<boolean>} Success status
 */
export async function logoutUser(token) {
  try {
    // Verify token to get expiration
    const decoded = jwt.verify(token, config.jwt.secret, { ignoreExpiration: true });
    const exp = decoded.exp;
    
    // Calculate TTL (time to live) for the blacklist entry
    const now = Math.floor(Date.now() / 1000);
    const ttl = Math.max(exp - now, 0);
    
    // Add token to blacklist
    const redisClient = getRedisClient();
    await redisClient.set(`${TOKEN_BLACKLIST}${token}`, '1', 'EX', ttl);
    
    return true;
  } catch (error) {
    logger.error('Error during logout:', error);
    return false;
  }
}

/**
 * Check if a token is blacklisted
 * @param {string} token - JWT token to check
 * @returns {Promise<boolean>} True if blacklisted
 */
export async function isTokenBlacklisted(token) {
  try {
    const redisClient = getRedisClient();
    const result = await redisClient.get(`${TOKEN_BLACKLIST}${token}`);
    return !!result;
  } catch (error) {
    logger.error('Error checking token blacklist:', error);
    return false;
  }
}

/**
 * Update user profile
 * @param {string} userId - User ID
 * @param {Object} updates - Profile updates
 * @returns {Promise<Object>} Updated user
 */
export async function updateUserProfile(userId, updates) {
  try {
    const allowedUpdates = ['name', 'email'];
    const filteredUpdates = Object.entries(updates)
      .filter(([key]) => allowedUpdates.includes(key))
      .reduce((obj, [key, value]) => {
        obj[key] = value;
        return obj;
      }, {});
    
    if (Object.keys(filteredUpdates).length === 0) {
      throw new Error('No valid fields to update');
    }
    
    // Add updated timestamp
    filteredUpdates.updatedAt = new Date().toISOString();
    
    // Update user in Redis
    const redisClient = getRedisClient();
    await redisClient.hset(`${USER_PREFIX}${userId}`, Object.entries(filteredUpdates).flat());
    
    // Get updated user
    return await getUserById(userId);
  } catch (error) {
    logger.error(`Error updating user ${userId}:`, error);
    throw error;
  }
}

/**
 * Link an Angel One client code to a user
 * @param {string} userId - User ID
 * @param {string} clientCode - Angel One client code
 * @returns {Promise<boolean>} Success status
 */
export async function linkAngelOneClientCode(userId, clientCode) {
  try {
    if (!clientCode) {
      throw new Error('Client code is required');
    }
    
    const normalizedClientCode = clientCode.toUpperCase();
    
    // Update user record with Angel One client code
    const redisClient = getRedisClient();
    await redisClient.hset(`${USER_PREFIX}${userId}`, {
      angelOneClientCode: normalizedClientCode,
      angelOneLinkedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });
    
    logger.info(`Linked Angel One client code ${normalizedClientCode} to user ${userId}`);
    return true;
  } catch (error) {
    logger.error(`Error linking Angel One client code for user ${userId}:`, error);
    throw error;
  }
}

/**
 * Get Angel One client code for a user
 * @param {string} userId - User ID
 * @returns {Promise<string|null>} Client code or null
 */
export async function getAngelOneClientCode(userId) {
  try {
    const user = await getUserById(userId);
    
    if (!user) {
      return null;
    }
    
    return user.angelOneClientCode || null;
  } catch (error) {
    logger.error(`Error getting Angel One client code for user ${userId}:`, error);
    return null;
  }
}