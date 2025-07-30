import express from 'express';
import healthRoutes from './health.js';
import authRoutes from './auth.js';
import angelOneRoutes from './angelOne.js';
import orderRoutes from './orders.js';
import scriptRoutes from './scripts.js';

const router = express.Router();

// Health check routes
router.use('/health', healthRoutes);

// Authentication routes
router.use('/auth', authRoutes);

// Angel One routes
router.use('/angelone', angelOneRoutes);

// Order routes
router.use('/orders', orderRoutes);

// Script routes
router.use('/scripts', scriptRoutes);

// Catch-all route
router.all('*', (req, res) => {
  res.status(404).json({
    status: 'error',
    message: `Cannot find ${req.originalUrl} on this server`
  });
});

export default router;