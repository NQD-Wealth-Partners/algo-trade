import 'dotenv/config';
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import morgan from 'morgan';
import config from './src/config/index.js';
import routes from './src/routes/index.js';
import { errorHandler } from './src/middleware/errorHandler.js';
import logger from './src/utils/logger.js';
import { connectRedis } from './src/config/redis.js';
import * as websocketService from './src/services/websocketService.js';

// Initialize Express app
const app = express();

// Connect to Redis
connectRedis();

// Middleware
app.use(helmet()); // Security headers
app.use(cors()); // CORS support
app.use(compression()); // Response compression
app.use(express.json()); // JSON body parser
app.use(express.urlencoded({ extended: true })); // URL-encoded body parser
app.use(morgan('combined', { stream: { write: message => logger.info(message.trim()) } })); // Request logging

// Routes
app.use(config.apiPrefix, routes);

// Error handling
app.use(errorHandler);

// Start server
const server = app.listen(config.port, () => {
  logger.info(`Server running on port ${config.port} in ${config.nodeEnv} mode`);

    // Initialize WebSocket service
  websocketService.initializeWebSocket()
    .then(() => {
      logger.info('WebSocket service initialized successfully');
    })
    .catch(error => {
      logger.error('Failed to initialize WebSocket service:', error);
    });
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (err) => {
  logger.error('UNHANDLED REJECTION! Shutting down...');
  logger.error(err.name, err.message);
  server.close(() => {
    process.exit(1);
  });
});

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
  logger.error('UNCAUGHT EXCEPTION! Shutting down...');
  logger.error(err.name, err.message);
  process.exit(1);
});

export default app; // For testing
