import * as userAuthService from '../services/userAuthService.js';
import asyncHandler from '../utils/asyncHandler.js';
import { successResponse, errorResponse } from '../utils/responseFormatter.js';
import logger from '../utils/logger.js';

/**
 * Register a new user
 * @route POST /api/v1/auth/register
 * @access Public
 */
export const register = asyncHandler(async (req, res) => {
  try {
    const { mobile, password, name } = req.body;
    
    if (!mobile || !password) {
      return errorResponse(res, 'Mobile number and password are required', 400);
    }
    
    const user = await userAuthService.createUser({
      mobile,
      password,
      name
    });
    
    return successResponse(res, {
      message: 'User registered successfully',
      user
    }, 201);
  } catch (error) {
    if (error.message === 'Mobile number already registered') {
      return errorResponse(res, error.message, 409);
    }
    
    if (error.message === 'Invalid mobile number format') {
      return errorResponse(res, error.message, 400);
    }
    
    logger.error('Registration error:', error);
    return errorResponse(res, error.message, 400);
  }
});

/**
 * Login user
 * @route POST /api/v1/auth/login
 * @access Public
 */
export const login = asyncHandler(async (req, res) => {
  try {
    const { mobile, password } = req.body;
    
    if (!mobile || !password) {
      return errorResponse(res, 'Mobile number and password are required', 400);
    }
    
    const result = await userAuthService.loginUser({
      mobile,
      password
    });
    
    return successResponse(res, {
      message: 'Login successful',
      token: result.token,
      user: result.user
    });
  } catch (error) {
    if (error.message === 'Invalid mobile number or password') {
      return errorResponse(res, error.message, 401);
    }
    
    if (error.message === 'Invalid mobile number format') {
      return errorResponse(res, error.message, 400);
    }
    
    logger.error('Login error:', error);
    return errorResponse(res, 'Login failed', 400);
  }
});

/**
 * Logout user
 * @route POST /api/v1/auth/logout
 * @access Private
 */
export const logout = asyncHandler(async (req, res) => {
  try {
    const token = req.token;
    
    if (!token) {
      return errorResponse(res, 'No token provided', 400);
    }
    
    const result = await userAuthService.logoutUser(token);
    
    if (result) {
      return successResponse(res, {
        message: 'Logout successful'
      });
    } else {
      return errorResponse(res, 'Logout failed', 500);
    }
  } catch (error) {
    logger.error('Logout error:', error);
    return errorResponse(res, 'Logout failed', 400);
  }
});

/**
 * Get current user profile
 * @route GET /api/v1/auth/me
 * @access Private
 */
export const getProfile = asyncHandler(async (req, res) => {
  return successResponse(res, {
    user: req.user
  });
});

/**
 * Update user profile
 * @route PUT /api/v1/auth/profile
 * @access Private
 */
export const updateProfile = asyncHandler(async (req, res) => {
  try {
    const userId = req.user.id;
    const updates = req.body;
    
    const updatedUser = await userAuthService.updateUserProfile(userId, updates);
    
    return successResponse(res, {
      message: 'Profile updated successfully',
      user: updatedUser
    });
  } catch (error) {
    logger.error('Profile update error:', error);
    return errorResponse(res, error.message, 400);
  }
});