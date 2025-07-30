import express from 'express';
import * as orderController from '../controllers/orderController.js';
import { authenticate } from '../middleware/authMiddleware.js';
import { cacheMiddleware } from '../middleware/rateLimiter.js';

const router = express.Router();

// All routes require authentication
router.use(authenticate);

// Regular orders
router.route('/')
  .post(orderController.placeOrder)
  .get(orderController.getOrderBook);

router.route('/:orderId')
  .get(orderController.getOrderDetails)
  .put(orderController.modifyOrder)
  .delete(orderController.cancelOrder);

// Positions
router.get('/positions', orderController.getPositions);
router.post('/positions/convert', orderController.convertPosition);

// Holdings
router.get('/holdings', orderController.getHoldings);

// GTT orders
router.route('/gtt')
  .post(orderController.placeGttOrder)
  .get(cacheMiddleware(60), orderController.getGttRules);

router.route('/gtt/:ruleId')
  .get(orderController.getGttRule)
  .put(orderController.modifyGttRule)
  .delete(orderController.cancelGttRule);

export default router;