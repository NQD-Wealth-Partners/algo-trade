import logger from '../utils/logger.js';

/**
 * Request logger middleware
 */
export default (req, res, next) => {
  const start = Date.now();
  
  // Log request when finished
  res.on('finish', () => {
    const duration = Date.now() - start;
    const log = {
      method: req.method,
      path: req.originalUrl,
      status: res.statusCode,
      duration: `${duration}ms`
    };
    
    if (res.statusCode >= 400) {
      logger.warn('API Request', log);
    } else {
      logger.info('API Request', log);
    }
  });
  
  next();
};