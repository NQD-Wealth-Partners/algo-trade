import express from 'express';
import * as websocketController from '../controllers/websocketController.js';

const router = express.Router();

// Get current subscriptions
router.get('/subscriptions', websocketController.getSubscriptions);

// Get last tick for a symbol
router.get('/tick/:symbol', websocketController.getLastTick);

// Manually subscribe to a symbol
router.post('/subscribe', websocketController.subscribeToSymbol);

export default router;