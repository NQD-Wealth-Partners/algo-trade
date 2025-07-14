import express from 'express';
import * as healthController from '../controllers/healthController.js';
import { cacheMiddleware } from '../middleware/rateLimiter.js';

const router = express.Router();

// Health check endpoint (with cache for 1 minute)
router.get('/', cacheMiddleware(60), healthController.getHealth);

// Redis health check endpoint
router.get('/redis', healthController.getRedisHealth);

export default router;