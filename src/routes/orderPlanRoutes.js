import express from 'express';
import * as orderPlanController from '../controllers/orderPlanController.js';

const router = express.Router();

// Create order plan
router.post('/', orderPlanController.createOrderPlan);

// Get all order plans with optional filters
router.get('/', orderPlanController.getAllOrderPlans);

// Get order plan by ID
router.get('/:id', orderPlanController.getOrderPlan);

// Update order plan
router.put('/:id', orderPlanController.updateOrderPlan);

// Delete order plan
router.delete('/:id', orderPlanController.deleteOrderPlan);

// Update order plan status
router.patch('/:id/status', orderPlanController.updateOrderPlanStatus);

export default router;