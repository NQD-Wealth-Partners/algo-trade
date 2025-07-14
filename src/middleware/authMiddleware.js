import jwt from 'jsonwebtoken';
import config from '../config/index.js';
import { isTokenBlacklisted, getUserById } from '../services/userAuthService.js';
import { errorResponse } from '../utils/responseFormatter.js';
import logger from '../utils/logger.js';

/**
 * Authentication middleware
 */
export const authenticate = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return errorResponse(res, 'Authentication required', 401);
    }
    
    const token = authHeader.split(' ')[1];
    
    // Check if token is blacklisted
    const isBlacklisted = await isTokenBlacklisted(token);
    if (isBlacklisted) {
      return errorResponse(res, 'Token is no longer valid', 401);
    }
    
    // Verify token
    const decoded = jwt.verify(token, config.jwt.secret);
    
    // Get user from database
    const user = await getUserById(decoded.userId);
    
    if (!user) {
      return errorResponse(res, 'User not found', 404);
    }
    
    // Attach user to request
    req.user = user;
    req.token = token;
    
    next();
  } catch (error) {
    if (error.name === 'JsonWebTokenError') {
      return errorResponse(res, 'Invalid token', 401);
    }
    
    if (error.name === 'TokenExpiredError') {
      return errorResponse(res, 'Token expired', 401);
    }
    
    logger.error('Authentication error:', error);
    return errorResponse(res, 'Authentication failed', 401);
  }
};