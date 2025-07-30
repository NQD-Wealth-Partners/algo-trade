import express from 'express';
import * as angelOneController from '../controllers/angelOneController.js';
import { authenticate } from '../middleware/authMiddleware.js';
import { cacheMiddleware } from '../middleware/rateLimiter.js';

const router = express.Router();

// All routes require authentication
router.use(authenticate);

// Link/unlink Angel One account
router.post('/link', angelOneController.linkAccount);
router.delete('/link', angelOneController.unlinkAccount);

// Get link status
router.get('/status', angelOneController.getLinkStatus);

// Get authentication token
router.get('/token', angelOneController.getAuthToken);

// Get user profile
router.get('/profile', angelOneController.getProfile);

// Refresh tokens
router.post('/refresh', angelOneController.refreshTokens);

export default router;