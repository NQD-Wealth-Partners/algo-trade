import express from 'express';
import * as scriptController from '../controllers/scriptController.js';
import { authenticate } from '../middleware/authMiddleware.js';

const router = express.Router();


// All routes require authentication
router.use(authenticate);

router.get('/search', scriptController.searchScripts);
router.get('/token/:token', scriptController.getScriptByToken);
router.get('/symbol/:symbol', scriptController.getScriptBySymbol);
router.get('/convert/:symbol', scriptController.convertToTradingSymbol);
router.get('/exchanges', scriptController.getExchanges);
router.get('/instrument-types', scriptController.getInstrumentTypes);
router.get('/stats', scriptController.getScriptStats);

// Admin routes
router.post('/update', scriptController.triggerScriptUpdate);

export default router;