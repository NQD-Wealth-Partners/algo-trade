import WebSocket from 'ws';
import logger from '../utils/logger.js';
import config from '../config/index.js';
import * as orderPlanRedisService from './orderPlanRedisService.js';
import {generateTOTP} from './angelOneService.js';
import Redis from 'ioredis';
import {SmartAPI} from 'smartapi-javascript';

// Initialize Redis clients - separate connections for different purposes
const redisSubscriber = new Redis({
  host: config.redis?.host || 'algo-trade-redis',
  port: config.redis?.port || 6379,
  password: config.redis?.password || '',
  db: config.redis?.db || 0
});

const redisPublisher = new Redis({
  host: config.redis?.host || 'algo-trade-redis',
  port: config.redis?.port || 6379,
  password: config.redis?.password || '',
  db: config.redis?.db || 0
});

const redisClient = new Redis({
  host: config.redis?.host || 'algo-trade-redis',
  port: config.redis?.port || 6379,
  password: config.redis?.password || '',
  db: config.redis?.db || 0
});

// Initialize SmartAPI
const smartApi = new SmartAPI({
  api_key: config.angelOne.apiKey,
});

// WebSocket connection states
const ConnectionState = {
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  AUTHENTICATING: 'authenticating',
  AUTHENTICATED: 'authenticated',
  READY: 'ready',
  RECONNECTING: 'reconnecting'
};

// Constants
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 5000;
const MESSAGE_ASSEMBLY_TIMEOUT = 5000;

// Connection state variables
let ws = null;
let connectionState = ConnectionState.DISCONNECTED;
let currentSession = null;
let lastMessageTime = Date.now();
let lastPongTime = Date.now();
let reconnectAttempts = 0;

// Message handling variables
let binaryMessageBuffer = null;
let expectedMessageLength = null;
let messageAssemblyTimeout = null;

// Intervals
let pingInterval = null;
let marketDataRequestInterval = null;
let connectionHealthCheckInterval = null;
let bufferCleanupInterval = null;

// Subscription tracking
const subscriptions = new Map(); // Map<token, Set<planId>>
const pendingSubscriptions = new Map(); // token -> { exchange, planId, symbol }
const tokenToSymbolMap = new Map(); // Map<token, symbol>
const symbolToTokenMap = new Map(); // Map<symbol, token>
const lastTicks = new Map(); // Map<token, tickData>

const priceMode = 3;

// WebSocket connection states and initialization
let wsLTP = null; // For Mode 1 (LTP)
let wsMarketDepth = null; // For Mode 3 (Snap Quote with Best Five)
let ltpConnectionState = ConnectionState.DISCONNECTED;
let depthConnectionState = ConnectionState.DISCONNECTED;

// Variables for intervals
let ltpPingInterval = null;
let ltpMarketDataRequestInterval = null;
let ltpConnectionHealthCheckInterval = null;
let ltpBufferCleanupInterval = null;

let depthPingInterval = null;
let depthMarketDataRequestInterval = null;
let depthConnectionHealthCheckInterval = null;
let depthBufferCleanupInterval = null;

// Connection monitoring variables for LTP WebSocket
let ltpLastMessageTime = Date.now();
let ltpLastPongTime = Date.now();
let ltpReconnectAttempts = 0;

// Connection monitoring variables for Market Depth WebSocket
let depthLastMessageTime = Date.now();
let depthLastPongTime = Date.now();
let depthReconnectAttempts = 0;

// Message handling variables for LTP WebSocket
let ltpBinaryMessageBuffer = null;
let ltpExpectedMessageLength = null;
let ltpMessageAssemblyTimeout = null;

// Message handling variables for Market Depth WebSocket
let depthBinaryMessageBuffer = null;
let depthExpectedMessageLength = null;
let depthMessageAssemblyTimeout = null;

/**
 * Initialize WebSocket connections and subscriptions
 */
export async function initializeWebSockets() {
  try {
    // Authenticate with Angel Broking
    const session = await authenticate();
    
    // Connect both WebSockets
    await Promise.all([
      connectLTPWebSocket(session),
      connectMarketDepthWebSocket(session)
    ]);
    
    // Set up subscription refresh interval
    setInterval(refreshSubscriptions, 60 * 60 * 1000); // Every hour
    
    // Initial subscription setup
    await setupInitialSubscriptions();
    
    // Listen for new order plans using the subscriber connection
    redisSubscriber.subscribe('orderplan:new');
    redisSubscriber.subscribe('orderplan:delete');
    
    redisSubscriber.on('message', async (channel, message) => {
      if (channel === 'orderplan:new') {
        const planId = message;
        await subscribeToOrderPlan(planId);
      } else if (channel === 'orderplan:delete') {
        const planId = message;
        await unsubscribeFromOrderPlan(planId);
      }
    });
    
    logger.info('WebSocket services initialized');
  } catch (error) {
    logger.error('Failed to initialize WebSocket services:', error);
    throw error;
  }
}

/**
 * Connect LTP WebSocket (Mode 1)
 * @param {Object} session - Authentication session with jwtToken and feedToken
 * @returns {Promise<void>}
 */
function connectLTPWebSocket(session) {
  return new Promise((resolve, reject) => {
    try {
      // Validate session
      if (!session || !session.jwtToken) {
        throw new Error('Invalid session: missing JWT token');
      }
      
      // Close existing connection if any
      closeLTPConnection();
      
      // Update connection state
      ltpConnectionState = ConnectionState.CONNECTING;
      logger.info('LTP WebSocket connection state:', ltpConnectionState);
      
      // Create new WebSocket connection
      const wsUrl = config.angelOne.wsUrl || 'wss://smartapisocket.angelone.in/smart-stream';
      logger.info(`Connecting to LTP WebSocket at ${wsUrl}`);
      
      // Create headers
      const headers = {
        'Authorization': `Bearer ${session.jwtToken}`,
        'x-api-key': config.angelOne.apiKey,
        'x-client-code': config.angelOneWebSocket.clientId,
        'x-feed-token': session.feedToken || session.jwtToken
      };
      
      wsLTP = new WebSocket(wsUrl, { headers });
      
      // Set binary type
      wsLTP.binaryType = 'arraybuffer';
      
      // Set connection timeout
      const connectionTimeout = setTimeout(() => {
        if (ltpConnectionState !== ConnectionState.READY) {
          logger.error('LTP WebSocket connection timeout after 30 seconds');
          ltpConnectionState = ConnectionState.DISCONNECTED;
          
          if (wsLTP) {
            try {
              wsLTP.terminate();
            } catch (err) {
              logger.warn(`Error terminating LTP WebSocket: ${err.message}`);
            }
            wsLTP = null;
          }
          
          reject(new Error('LTP WebSocket connection timeout'));
        }
      }, 30000);
      
      // Set up event handlers
      setupLTPEventHandlers(resolve, reject, connectionTimeout, session);
      
    } catch (error) {
      logger.error('Error connecting to LTP WebSocket:', error);
      
      // Update state
      ltpConnectionState = ConnectionState.DISCONNECTED;
      logger.info('LTP WebSocket connection state:', ltpConnectionState);
      
      reject(error);
    }
  });
}

/**
 * Set up LTP WebSocket event handlers
 * @param {Function} resolve - Promise resolve function
 * @param {Function} reject - Promise reject function
 * @param {Timeout} connectionTimeout - Connection timeout
 * @param {Object} session - Session information
 */
function setupLTPEventHandlers(resolve, reject, connectionTimeout, session) {
  wsLTP.on('open', () => {
    logger.info('LTP WebSocket connection established');
    
    // Set up intervals
    setupLTPIntervals();
    
    // Send authentication message
    sendLTPAuthenticationMessage(session);
  });
  
  wsLTP.on('message', (data) => {
    try {
      handleLTPWebSocketMessage(data);
    } catch (err) {
      logger.error(`Error in LTP message handler: ${err.message}`);
      if (err.stack) logger.debug(`Stack trace: ${err.stack}`);
    }
  });
  
  wsLTP.on('error', (error) => {
    logger.error('LTP WebSocket error:', error);
    
    // Clear connection timeout
    clearTimeout(connectionTimeout);
    
    // Update state
    ltpConnectionState = ConnectionState.DISCONNECTED;
    logger.info('LTP WebSocket connection state:', ltpConnectionState);
    
    // Reject the promise if we're still connecting
    reject(error);
  });
  
  wsLTP.on('close', (code, reason) => {
    logger.warn(`LTP WebSocket connection closed: ${code} ${reason}`);
    
    // Clear connection timeout
    clearTimeout(connectionTimeout);
    
    // Update state
    ltpConnectionState = ConnectionState.DISCONNECTED;
    logger.info('LTP WebSocket connection state:', ltpConnectionState);
    
    // Clear intervals
    clearLTPIntervals();
    
    // Handle reconnect
    handleLTPReconnect();
  });
  
  // Set up a special handler to update connection state to READY after authentication
  setTimeout(() => {
    if (ltpConnectionState === ConnectionState.AUTHENTICATED || ltpConnectionState === ConnectionState.CONNECTING) {
      logger.info('Setting LTP connection state to READY after timeout');
      ltpConnectionState = ConnectionState.READY;
      
      // Process subscriptions for LTP
      try {
        processLTPSubscriptions();
      } catch (err) {
        logger.error(`Error processing LTP subscriptions: ${err.message}`);
      }
      
      // Resolve the promise
      resolve();
    }
  }, 5000); // 5 second timeout
}

/**
 * Set up maintenance intervals for LTP WebSocket
 */
function setupLTPIntervals() {
  try {
    // Update time trackers
    ltpLastMessageTime = Date.now();
    ltpLastPongTime = Date.now();
    
    // Set up ping interval
    ltpPingInterval = setInterval(() => {
      if (wsLTP && wsLTP.readyState === WebSocket.OPEN) {
        try {
          logger.debug('Sending ping to keep LTP WebSocket alive');
          wsLTP.ping();
        } catch (err) {
          logger.warn(`Error sending ping to LTP WebSocket: ${err.message}`);
        }
      }
    }, 30000); // Every 30 seconds
    
    // Set up market data request interval
    ltpMarketDataRequestInterval = setInterval(() => {
      if (ltpConnectionState === ConnectionState.READY && wsLTP && wsLTP.readyState === WebSocket.OPEN) {
        try {
          requestLTPMarketData();
        } catch (err) {
          logger.warn(`Error requesting LTP market data: ${err.message}`);
        }
      }
    }, 60000); // Every 60 seconds
    
    // Set up connection health check
    ltpConnectionHealthCheckInterval = setInterval(() => {
      try {
        checkLTPConnectionHealth();
      } catch (err) {
        logger.warn(`Error checking LTP connection health: ${err.message}`);
      }
    }, 60000); // Every 60 seconds
    
    // Set up buffer cleanup interval
    ltpBufferCleanupInterval = setInterval(() => {
      try {
        cleanupLTPMessageBuffers();
      } catch (err) {
        logger.warn(`Error cleaning up LTP message buffers: ${err.message}`);
      }
    }, 10000); // Every 10 seconds
    
    logger.info('LTP WebSocket intervals set up');
  } catch (error) {
    logger.error(`Error setting up LTP intervals: ${error.message}`);
  }
}

/**
 * Clean up LTP message buffers
 */
function cleanupLTPMessageBuffers() {
  try {
    if (ltpBinaryMessageBuffer && ltpExpectedMessageLength) {
      const now = Date.now();
      const bufferAge = now - ltpLastMessageTime;
      
      // If buffer is older than 30 seconds, clean it up
      if (bufferAge > 30000) {
        logger.warn(`Cleaning up stale LTP message buffer (age: ${bufferAge}ms)`);
        
        // Reset buffer state
        ltpBinaryMessageBuffer = null;
        ltpExpectedMessageLength = null;
        
        // Clear timeout
        if (ltpMessageAssemblyTimeout) {
          clearTimeout(ltpMessageAssemblyTimeout);
          ltpMessageAssemblyTimeout = null;
        }
      }
    }
  } catch (error) {
    logger.error(`Error cleaning up LTP message buffers: ${error.message}`);
  }
}

/**
 * Check LTP connection health
 */
function checkLTPConnectionHealth() {
  try {
    if (!wsLTP) {
      logger.warn('No LTP WebSocket connection to check');
      return;
    }
    
    const now = Date.now();
    const timeSinceLastMessage = now - ltpLastMessageTime;
    const timeSinceLastPong = now - ltpLastPongTime;
    
    logger.debug(`LTP connection health: Last message ${timeSinceLastMessage}ms ago, last pong ${timeSinceLastPong}ms ago`);
    
    // If no message for 5 minutes, reconnect
    if (timeSinceLastMessage > 5 * 60 * 1000) {
      logger.warn(`No LTP messages received for ${Math.floor(timeSinceLastMessage/1000)}s, reconnecting...`);
      reconnectLTPWebSocket();
    }
    
    // If no pong for 2 minutes, reconnect
    if (timeSinceLastPong > 2 * 60 * 1000) {
      logger.warn(`No LTP pong received for ${Math.floor(timeSinceLastPong/1000)}s, reconnecting...`);
      reconnectLTPWebSocket();
    }
  } catch (error) {
    logger.error(`Error checking LTP connection health: ${error.message}`);
  }
}

/**
 * Reconnect LTP WebSocket
 */
function reconnectLTPWebSocket() {
  try {
    logger.info('Initiating LTP WebSocket reconnection');
    
    // Close existing connection
    closeLTPConnection();
    
    // Update state
    ltpConnectionState = ConnectionState.RECONNECTING;
    logger.info('LTP WebSocket connection state:', ltpConnectionState);
    
    // Re-authenticate
    authenticate()
      .then(newSession => connectLTPWebSocket(newSession))
      .then(() => {
        logger.info('LTP WebSocket reconnection successful');
        
        // Resubscribe to tokens
        processLTPSubscriptions();
      })
      .catch(error => {
        logger.error('LTP WebSocket reconnection failed:', error);
        
        // Schedule another reconnect attempt if within limits
        if (ltpReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          handleLTPReconnect();
        } else {
          logger.error('Maximum LTP reconnection attempts reached. Giving up.');
        }
      });
  } catch (error) {
    logger.error('Error in reconnectLTPWebSocket:', error);
  }
}

/**
 * Request LTP market data for all subscribed tokens
 */
function requestLTPMarketData() {
  try {
    if (ltpConnectionState !== ConnectionState.READY || !wsLTP || wsLTP.readyState !== WebSocket.OPEN) {
      logger.warn(`Cannot request LTP market data: WebSocket not ready (state: ${ltpConnectionState})`);
      return;
    }
    
    // Group tokens by exchange
    const tokensByExchange = new Map(); // exchangeType -> array of tokens
    
    // Process all subscriptions
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.size === 0) continue;
      
      // Get symbol for the token
      const symbol = tokenToSymbolMap.get(token);
      if (!symbol) {
        logger.warn(`Cannot find symbol for token ${token}`);
        continue;
      }
      
      // Get exchange type for this symbol
      const plan = findOrderPlanBySymbolSync(symbol);
      if (!plan) {
        logger.warn(`Cannot find order plan for symbol ${symbol}`);
        continue;
      }
      
      const exchangeType = mapExchangeToType(plan.exchange);
      
      // Add to tokens by exchange map
      if (!tokensByExchange.has(exchangeType)) {
        tokensByExchange.set(exchangeType, []);
      }
      
      tokensByExchange.get(exchangeType).push(parseInt(token, 10));
    }
    
    // If no tokens to request, return
    if (tokensByExchange.size === 0) {
      logger.debug('No tokens to request LTP market data for');
      return;
    }
    
    // Build token list for request
    const tokenList = [];
    for (const [exchangeType, tokens] of tokensByExchange.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    // Create request message
    const requestMessage = {
      correlationID: "ltp_marketdata_" + Date.now(),
      action: 2, // MARKET_DATA_REQUEST
      params: {
        mode: 1, // LTP mode
        tokenList
      }
    };
    
    // Log request summary
    const tokenCount = tokenList.reduce((sum, item) => sum + item.tokens.length, 0);
    logger.info(`Requesting LTP market data for ${tokenCount} tokens across ${tokenList.length} exchanges`);
    
    // Send request
    wsLTP.send(JSON.stringify(requestMessage));
  } catch (error) {
    logger.error('Error requesting LTP market data:', error);
  }
}


/**
 * Process subscriptions for LTP WebSocket
 */
function processLTPSubscriptions() {
  try {
    logger.info('Processing LTP subscriptions');
    
    // Check connection state
    if (ltpConnectionState !== ConnectionState.READY) {
      logger.warn(`Cannot process LTP subscriptions: WebSocket not ready (state: ${ltpConnectionState})`);
      return;
    }
    
    // Group tokens by exchange for subscription
    const tokensByExchange = new Map(); // exchangeType -> array of tokens
    
    // Process all subscriptions
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.size === 0) continue;
      
      // Get symbol for the token
      const symbol = tokenToSymbolMap.get(token);
      if (!symbol) {
        logger.warn(`Cannot find symbol for token ${token}`);
        continue;
      }
      
      // Get exchange type for this symbol
      const plan = findOrderPlanBySymbolSync(symbol);
      if (!plan) {
        logger.warn(`Cannot find order plan for symbol ${symbol}`);
        continue;
      }
      
      const exchangeType = mapExchangeToType(plan.exchange);
      
      // Add to tokens by exchange map
      if (!tokensByExchange.has(exchangeType)) {
        tokensByExchange.set(exchangeType, []);
      }
      
      tokensByExchange.get(exchangeType).push(parseInt(token, 10));
    }
    
    // If no tokens to subscribe, return
    if (tokensByExchange.size === 0) {
      logger.info('No tokens to subscribe to for LTP');
      return;
    }
    
    // Build token list for subscription
    const tokenList = [];
    for (const [exchangeType, tokens] of tokensByExchange.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    // Create subscription message
    const subscriptionMessage = {
      correlationID: "ltp_subscription_" + Date.now(),
      action: 1, // SUBSCRIBE
      params: {
        mode: 1, // LTP mode
        tokenList
      }
    };
    
    // Log subscription summary
    const tokenCount = tokenList.reduce((sum, item) => sum + item.tokens.length, 0);
    logger.info(`Subscribing to ${tokenCount} tokens across ${tokenList.length} exchanges for LTP`);
    
    // Send subscription
    if (wsLTP && wsLTP.readyState === WebSocket.OPEN) {
      wsLTP.send(JSON.stringify(subscriptionMessage));
      logger.info(`Subscribed to ${tokensByExchange.size} exchanges with ${tokenCount} tokens for LTP`);
    } else {
      logger.error('LTP WebSocket not connected, cannot subscribe');
    }
  } catch (error) {
    logger.error('Error processing LTP subscriptions:', error);
  }
}


/**
 * Handle LTP WebSocket messages
 * @param {any} data - Message data
 */
function handleLTPWebSocketMessage(data) {
  try {
    // Update last message time
    ltpLastMessageTime = Date.now();
    // Handle binary data
    if (data instanceof Buffer || data instanceof ArrayBuffer) {
      try {
        // Get size for logging
        const size = data instanceof Buffer ? data.length : data.byteLength;
        logger.debug(`Received LTP WebSocket message: ${data instanceof Buffer ? 'Buffer' : 'ArrayBuffer'}, size: ${size} bytes`);
        
        // Get first byte for message type detection
        const bytes = data instanceof Buffer ? 
          new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : 
          new Uint8Array(data);
        
        if (bytes.length === 0) {
          logger.warn('Received empty binary message');
          return;
        }
        
        const firstByte = bytes[0];
        
        // Check message type
        if (firstByte === 1) {
          // This is an LTP mode market data message
          const tickData = handleDataTicks(data);
          
          if (!tickData) {
            logger.warn('handleDataTicks returned null or undefined');
            return;
          }
          
          if (tickData.error) {
            logger.warn(`Error in tick data: ${tickData.error}`);
            return;
          }
          
          if (!tickData.token) {
            logger.warn('Tick data missing token');
            return;
          }
          
          // Process the tick data
          const token = tickData.token;
          logger.debug(`Processed LTP market data for token ${token}`);
          
          // Create a simplified tick object for existing code
          const simpleTick = {
            token,
            ltp: tickData.lastTradedPrice || 0,
            open: tickData.openPrice || 0,
            high: tickData.highPrice || 0,
            low: tickData.lowPrice || 0,
            close: tickData.closePrice || 0,
            totalTradedQty: tickData.volumeTradedToday || 0,
            totBuyQty: tickData.totalBuyQuantity || 0,
            totSellQty: tickData.totalSellQuantity || 0,
            sequenceNumber: tickData.sequenceNumber,
            exchangeTimestamp: tickData.exchangeTimestamp
          };
          
          // Store last tick data
          lastTicks.set(token, simpleTick);
          
          // Update symbol price in Redis and order plans
          const symbol = tokenToSymbolMap.get(token);
          if (symbol) {
            updateSymbolPrice(symbol, token, simpleTick)
              .catch(err => logger.error(`Async error updating symbol price: ${err.message}`));
            
            // Update order plans that use this symbol
            if (subscriptions.has(token)) {
              const planIds = subscriptions.get(token);
              for (const planId of planIds) {
                if (planId) {
                  updateOrderPlanPrice(planId, simpleTick)
                    .catch(err => logger.error(`Async error updating order plan: ${err.message}`));
                }
              }
            }
            
            // Calculate price change for logging
            const priceChange = simpleTick.close > 0 ? 
              ((simpleTick.ltp - simpleTick.close) / simpleTick.close * 100).toFixed(2) + '%' : 
              'N/A';
            
            logger.info(`LTP data for ${symbol}: LTP=${simpleTick.ltp}, Change=${priceChange}`);
          } else {
            logger.debug(`No symbol mapping found for token ${token}`);
          }
        } 
        else if (bytes.length > 2 && bytes[2] === 0x37) {
          // This looks like an acknowledgment message
          handleAcknowledgmentMessage(data);
        }
      } catch (binaryError) {
        logger.error(`Error processing LTP binary message: ${binaryError.message}`);
        console.error('Stack trace:', binaryError.stack);
      }
      return;
    }
    
    // Handle text messages (could be acknowledgments, errors, etc.)
    try {
      const message = JSON.parse(data.toString());
      logger.debug(`Received LTP text message: ${JSON.stringify(message)}`);
      
      // Handle authentication response
      if (message.action === 1 && message.response) {
        if (message.response.status) {
          logger.info('LTP WebSocket authentication successful');
          ltpConnectionState = ConnectionState.AUTHENTICATED;
        } else {
          logger.error(`LTP WebSocket authentication failed: ${message.response.message || 'Unknown error'}`);
          ltpConnectionState = ConnectionState.DISCONNECTED;
          if (wsLTP) wsLTP.close();
        }
      }
    } catch (textError) {
      logger.error(`Error processing LTP text message: ${textError.message}`);
    }
    
  } catch (error) {
    logger.error(`Error handling LTP WebSocket message: ${error.message}`);
    if (error.stack) {
      logger.debug(`Stack trace: ${error.stack}`);
    }
  }
}

/**
 * Send authentication message for LTP WebSocket
 * @param {Object} session - Session information
 */
function sendLTPAuthenticationMessage(session) {
  try {
    const authMessage = {
      correlationID: "ltp_websocket_" + Date.now(),
      action: 1, // AUTHENTICATE
      params: {
        clientCode: config.angelOneWebSocket.clientId,
        authorization: session.jwtToken
      }
    };
    
    logger.info('Sending authentication message to LTP WebSocket');
    wsLTP.send(JSON.stringify(authMessage));
    ltpConnectionState = ConnectionState.AUTHENTICATING;
  } catch (error) {
    logger.error('Error sending authentication message to LTP WebSocket:', error);
    if (wsLTP) wsLTP.close();
  }
}

/**
 * Close LTP WebSocket connection and clear intervals
 */
function closeLTPConnection() {
  if (wsLTP) {
    try {
      wsLTP.terminate();
    } catch (err) {
      logger.warn(`Error terminating existing LTP WebSocket: ${err.message}`);
    }
    wsLTP = null;
  }
  
  // Clear intervals specific to LTP connection
  clearLTPIntervals();
}

/**
 * Clear intervals for LTP WebSocket
 */
function clearLTPIntervals() {
  try {
    // Clear ping interval
    if (ltpPingInterval) {
      clearInterval(ltpPingInterval);
      ltpPingInterval = null;
    }
    
    // Clear market data request interval
    if (ltpMarketDataRequestInterval) {
      clearInterval(ltpMarketDataRequestInterval);
      ltpMarketDataRequestInterval = null;
    }
    
    // Clear connection health check interval
    if (ltpConnectionHealthCheckInterval) {
      clearInterval(ltpConnectionHealthCheckInterval);
      ltpConnectionHealthCheckInterval = null;
    }
    
    // Clear buffer cleanup interval
    if (ltpBufferCleanupInterval) {
      clearInterval(ltpBufferCleanupInterval);
      ltpBufferCleanupInterval = null;
    }
    
    // Clear message assembly timeout
    if (ltpMessageAssemblyTimeout) {
      clearTimeout(ltpMessageAssemblyTimeout);
      ltpMessageAssemblyTimeout = null;
    }
    
    // Reset buffer
    ltpBinaryMessageBuffer = null;
    ltpExpectedMessageLength = null;
    
    logger.info('LTP WebSocket intervals cleared');
  } catch (error) {
    logger.error(`Error clearing LTP intervals: ${error.message}`);
  }
}


/**
 * Connect Market Depth WebSocket (Mode 3)
 * @param {Object} session - Authentication session with jwtToken and feedToken
 * @returns {Promise<void>}
 */
function connectMarketDepthWebSocket(session) {
  return new Promise((resolve, reject) => {
    try {
      // Validate session
      if (!session || !session.jwtToken) {
        throw new Error('Invalid session: missing JWT token');
      }
      
      // Close existing connection if any
      closeMarketDepthConnection();
      
      // Update connection state
      depthConnectionState = ConnectionState.CONNECTING;
      logger.info('Market Depth WebSocket connection state:', depthConnectionState);
      
      // Create new WebSocket connection
      const wsUrl = config.angelOne.wsUrl || 'wss://smartapisocket.angelone.in/smart-stream';
      logger.info(`Connecting to Market Depth WebSocket at ${wsUrl}`);
      
      // Create headers
      const headers = {
        'Authorization': `Bearer ${session.jwtToken}`,
        'x-api-key': config.angelOne.apiKey,
        'x-client-code': config.angelOneWebSocket.clientId,
        'x-feed-token': session.feedToken || session.jwtToken
      };
      
      wsMarketDepth = new WebSocket(wsUrl, { headers });
      
      // Set binary type
      wsMarketDepth.binaryType = 'arraybuffer';
      
      // Set connection timeout
      const connectionTimeout = setTimeout(() => {
        if (depthConnectionState !== ConnectionState.READY) {
          logger.error('Market Depth WebSocket connection timeout after 30 seconds');
          depthConnectionState = ConnectionState.DISCONNECTED;
          
          if (wsMarketDepth) {
            try {
              wsMarketDepth.terminate();
            } catch (err) {
              logger.warn(`Error terminating Market Depth WebSocket: ${err.message}`);
            }
            wsMarketDepth = null;
          }
          
          reject(new Error('Market Depth WebSocket connection timeout'));
        }
      }, 30000);
      
      // Set up event handlers
      setupMarketDepthEventHandlers(resolve, reject, connectionTimeout, session);
      
    } catch (error) {
      logger.error('Error connecting to Market Depth WebSocket:', error);
      
      // Update state
      depthConnectionState = ConnectionState.DISCONNECTED;
      logger.info('Market Depth WebSocket connection state:', depthConnectionState);
      
      reject(error);
    }
  });
}


/**
 * Set up Market Depth WebSocket event handlers
 * @param {Function} resolve - Promise resolve function
 * @param {Function} reject - Promise reject function
 * @param {Timeout} connectionTimeout - Connection timeout
 * @param {Object} session - Session information
 */
function setupMarketDepthEventHandlers(resolve, reject, connectionTimeout, session) {
  wsMarketDepth.on('open', () => {
    logger.info('Market Depth WebSocket connection established');
    
    // Set up intervals
    setupMarketDepthIntervals();
    
    // Send authentication message
    sendMarketDepthAuthenticationMessage(session);
  });
  
  wsMarketDepth.on('message', (data) => {
    try {
      handleMarketDepthWebSocketMessage(data);
    } catch (err) {
      logger.error(`Error in Market Depth message handler: ${err.message}`);
      if (err.stack) logger.debug(`Stack trace: ${err.stack}`);
    }
  });
  
  wsMarketDepth.on('error', (error) => {
    logger.error('Market Depth WebSocket error:', error);
    
    // Clear connection timeout
    clearTimeout(connectionTimeout);
    
    // Update state
    depthConnectionState = ConnectionState.DISCONNECTED;
    logger.info('Market Depth WebSocket connection state:', depthConnectionState);
    
    // Reject the promise if we're still connecting
    reject(error);
  });
  
  wsMarketDepth.on('close', (code, reason) => {
    logger.warn(`Market Depth WebSocket connection closed: ${code} ${reason}`);
    
    // Clear connection timeout
    clearTimeout(connectionTimeout);
    
    // Update state
    depthConnectionState = ConnectionState.DISCONNECTED;
    logger.info('Market Depth WebSocket connection state:', depthConnectionState);
    
    // Clear intervals
    clearMarketDepthIntervals();
    
    // Handle reconnect
    handleMarketDepthReconnect();
  });
  
  // Set up a special handler to update connection state to READY after authentication
  setTimeout(() => {
    if (depthConnectionState === ConnectionState.AUTHENTICATED || depthConnectionState === ConnectionState.CONNECTING) {
      logger.info('Setting Market Depth connection state to READY after timeout');
      depthConnectionState = ConnectionState.READY;
      
      // Process subscriptions for Market Depth
      try {
        processMarketDepthSubscriptions();
      } catch (err) {
        logger.error(`Error processing Market Depth subscriptions: ${err.message}`);
      }
      
      // Resolve the promise
      resolve();
    }
  }, 5000); // 5 second timeout
}

/**
 * Set up maintenance intervals for Market Depth WebSocket
 */
function setupMarketDepthIntervals() {
  try {
    // Update time trackers
    depthLastMessageTime = Date.now();
    depthLastPongTime = Date.now();
    
    // Set up ping interval
    depthPingInterval = setInterval(() => {
      if (wsMarketDepth && wsMarketDepth.readyState === WebSocket.OPEN) {
        try {
          logger.debug('Sending ping to keep Market Depth WebSocket alive');
          wsMarketDepth.ping();
        } catch (err) {
          logger.warn(`Error sending ping to Market Depth WebSocket: ${err.message}`);
        }
      }
    }, 30000); // Every 30 seconds
    
    // Set up market data request interval
    depthMarketDataRequestInterval = setInterval(() => {
      if (depthConnectionState === ConnectionState.READY && wsMarketDepth && wsMarketDepth.readyState === WebSocket.OPEN) {
        try {
          requestMarketDepthData();
        } catch (err) {
          logger.warn(`Error requesting Market Depth data: ${err.message}`);
        }
      }
    }, 60000); // Every 60 seconds
    
    // Set up connection health check
    depthConnectionHealthCheckInterval = setInterval(() => {
      try {
        checkMarketDepthConnectionHealth();
      } catch (err) {
        logger.warn(`Error checking Market Depth connection health: ${err.message}`);
      }
    }, 60000); // Every 60 seconds
    
    // Set up buffer cleanup interval
    depthBufferCleanupInterval = setInterval(() => {
      try {
        cleanupMarketDepthMessageBuffers();
      } catch (err) {
        logger.warn(`Error cleaning up Market Depth message buffers: ${err.message}`);
      }
    }, 10000); // Every 10 seconds
    
    logger.info('Market Depth WebSocket intervals set up');
  } catch (error) {
    logger.error(`Error setting up Market Depth intervals: ${error.message}`);
  }
}

/**
 * Clean up Market Depth message buffers
 */
function cleanupMarketDepthMessageBuffers() {
  try {
    if (depthBinaryMessageBuffer && depthExpectedMessageLength) {
      const now = Date.now();
      const bufferAge = now - depthLastMessageTime;
      
      // If buffer is older than 30 seconds, clean it up
      if (bufferAge > 30000) {
        logger.warn(`Cleaning up stale Market Depth message buffer (age: ${bufferAge}ms)`);
        
        // Reset buffer state
        depthBinaryMessageBuffer = null;
        depthExpectedMessageLength = null;
        
        // Clear timeout
        if (depthMessageAssemblyTimeout) {
          clearTimeout(depthMessageAssemblyTimeout);
          depthMessageAssemblyTimeout = null;
        }
      }
    }
  } catch (error) {
    logger.error(`Error cleaning up Market Depth message buffers: ${error.message}`);
  }
}

/**
 * Check Market Depth connection health
 */
function checkMarketDepthConnectionHealth() {
  try {
    if (!wsMarketDepth) {
      logger.warn('No Market Depth WebSocket connection to check');
      return;
    }
    
    const now = Date.now();
    const timeSinceLastMessage = now - depthLastMessageTime;
    const timeSinceLastPong = now - depthLastPongTime;
    
    logger.debug(`Market Depth connection health: Last message ${timeSinceLastMessage}ms ago, last pong ${timeSinceLastPong}ms ago`);
    
    // If no message for 5 minutes, reconnect
    if (timeSinceLastMessage > 5 * 60 * 1000) {
      logger.warn(`No Market Depth messages received for ${Math.floor(timeSinceLastMessage/1000)}s, reconnecting...`);
      reconnectMarketDepthWebSocket();
    }
    
    // If no pong for 2 minutes, reconnect
    if (timeSinceLastPong > 2 * 60 * 1000) {
      logger.warn(`No Market Depth pong received for ${Math.floor(timeSinceLastPong/1000)}s, reconnecting...`);
      reconnectMarketDepthWebSocket();
    }
  } catch (error) {
    logger.error(`Error checking Market Depth connection health: ${error.message}`);
  }
}

/**
 * Reconnect Market Depth WebSocket
 */
function reconnectMarketDepthWebSocket() {
  try {
    logger.info('Initiating Market Depth WebSocket reconnection');
    
    // Close existing connection
    closeMarketDepthConnection();
    
    // Update state
    depthConnectionState = ConnectionState.RECONNECTING;
    logger.info('Market Depth WebSocket connection state:', depthConnectionState);
    
    // Re-authenticate
    authenticate()
      .then(newSession => connectMarketDepthWebSocket(newSession))
      .then(() => {
        logger.info('Market Depth WebSocket reconnection successful');
        
        // Resubscribe to tokens
        processMarketDepthSubscriptions();
      })
      .catch(error => {
        logger.error('Market Depth WebSocket reconnection failed:', error);
        
        // Schedule another reconnect attempt if within limits
        if (depthReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          handleMarketDepthReconnect();
        } else {
          logger.error('Maximum Market Depth reconnection attempts reached. Giving up.');
        }
      });
  } catch (error) {
    logger.error('Error in reconnectMarketDepthWebSocket:', error);
  }
}


/**
 * Request Market Depth data for all subscribed tokens
 */
function requestMarketDepthData() {
  try {
    if (depthConnectionState !== ConnectionState.READY || !wsMarketDepth || wsMarketDepth.readyState !== WebSocket.OPEN) {
      logger.warn(`Cannot request Market Depth data: WebSocket not ready (state: ${depthConnectionState})`);
      return;
    }
    
    // Group tokens by exchange
    const tokensByExchange = new Map(); // exchangeType -> array of tokens
    
    // Process all subscriptions
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.size === 0) continue;
      
      // Get symbol for the token
      const symbol = tokenToSymbolMap.get(token);
      if (!symbol) {
        logger.warn(`Cannot find symbol for token ${token}`);
        continue;
      }
      
      // Get exchange type for this symbol
      const plan = findOrderPlanBySymbolSync(symbol);
      if (!plan) {
        logger.warn(`Cannot find order plan for symbol ${symbol}`);
        continue;
      }
      
      const exchangeType = mapExchangeToType(plan.exchange);
      
      // Add to tokens by exchange map
      if (!tokensByExchange.has(exchangeType)) {
        tokensByExchange.set(exchangeType, []);
      }
      
      tokensByExchange.get(exchangeType).push(parseInt(token, 10));
    }
    
    // If no tokens to request, return
    if (tokensByExchange.size === 0) {
      logger.debug('No tokens to request Market Depth data for');
      return;
    }
    
    // Build token list for request
    const tokenList = [];
    for (const [exchangeType, tokens] of tokensByExchange.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    // Create request message
    const requestMessage = {
      correlationID: "marketdepth_request_" + Date.now(),
      action: 2, // MARKET_DATA_REQUEST
      params: {
        mode: 3, // Snap Quote mode
        tokenList
      }
    };
    
    // Log request summary
    const tokenCount = tokenList.reduce((sum, item) => sum + item.tokens.length, 0);
    logger.info(`Requesting Market Depth data for ${tokenCount} tokens across ${tokenList.length} exchanges`);
    
    // Send request
    wsMarketDepth.send(JSON.stringify(requestMessage));
  } catch (error) {
    logger.error('Error requesting Market Depth data:', error);
  }
}

/**
 * Process subscriptions for Market Depth WebSocket
 */
function processMarketDepthSubscriptions() {
  try {
    logger.info('Processing Market Depth subscriptions');
    
    // Check connection state
    if (depthConnectionState !== ConnectionState.READY) {
      logger.warn(`Cannot process Market Depth subscriptions: WebSocket not ready (state: ${depthConnectionState})`);
      return;
    }
    
    // Group tokens by exchange for subscription
    const tokensByExchange = new Map(); // exchangeType -> array of tokens
    
    // Process all subscriptions
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.size === 0) continue;
      
      // Get symbol for the token
      const symbol = tokenToSymbolMap.get(token);
      if (!symbol) {
        logger.warn(`Cannot find symbol for token ${token}`);
        continue;
      }
      
      // Get exchange type for this symbol
      const plan = findOrderPlanBySymbolSync(symbol);
      if (!plan) {
        logger.warn(`Cannot find order plan for symbol ${symbol}`);
        continue;
      }
      
      const exchangeType = mapExchangeToType(plan.exchange);
      
      // Add to tokens by exchange map
      if (!tokensByExchange.has(exchangeType)) {
        tokensByExchange.set(exchangeType, []);
      }
      
      tokensByExchange.get(exchangeType).push(parseInt(token, 10));
    }
    
    // If no tokens to subscribe, return
    if (tokensByExchange.size === 0) {
      logger.info('No tokens to subscribe to for Market Depth');
      return;
    }
    
    // Build token list for subscription
    const tokenList = [];
    for (const [exchangeType, tokens] of tokensByExchange.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    // Create subscription message
    const subscriptionMessage = {
      correlationID: "marketdepth_subscription_" + Date.now(),
      action: 1, // SUBSCRIBE
      params: {
        mode: 3, // Snap Quote mode
        tokenList
      }
    };
    
    // Log subscription summary
    const tokenCount = tokenList.reduce((sum, item) => sum + item.tokens.length, 0);
    logger.info(`Subscribing to ${tokenCount} tokens across ${tokenList.length} exchanges for Market Depth`);
    
    // Send subscription
    if (wsMarketDepth && wsMarketDepth.readyState === WebSocket.OPEN) {
      wsMarketDepth.send(JSON.stringify(subscriptionMessage));
      logger.info(`Subscribed to ${tokensByExchange.size} exchanges with ${tokenCount} tokens for Market Depth`);
    } else {
      logger.error('Market Depth WebSocket not connected, cannot subscribe');
    }
  } catch (error) {
    logger.error('Error processing Market Depth subscriptions:', error);
  }
}

/**
 * Update handlers for pong messages to track pong receipt time
 */
function setupLTPPongHandler() {
  wsLTP.on('pong', () => {
    logger.debug('Received pong from LTP server');
    ltpLastPongTime = Date.now();
  });
}

function setupMarketDepthPongHandler() {
  wsMarketDepth.on('pong', () => {
    logger.debug('Received pong from Market Depth server');
    depthLastPongTime = Date.now();
  });
}

/**
 * Handle Market Depth WebSocket messages
 * @param {any} data - Message data
 */
function handleMarketDepthWebSocketMessage(data) {
  try {
    // Update last message time
    depthLastMessageTime = Date.now();
    // Handle binary data
    if (data instanceof Buffer || data instanceof ArrayBuffer) {
      try {
        // Get size for logging
        const size = data instanceof Buffer ? data.length : data.byteLength;
        logger.debug(`Received Market Depth WebSocket message: ${data instanceof Buffer ? 'Buffer' : 'ArrayBuffer'}, size: ${size} bytes`);
        
        // Get first byte for message type detection
        const bytes = data instanceof Buffer ? 
          new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : 
          new Uint8Array(data);
        
        if (bytes.length === 0) {
          logger.warn('Received empty binary message');
          return;
        }
        
        const firstByte = bytes[0];
        
        // Check message type
        if (firstByte === 3) {  // Snap Quote mode
          const buffer = data instanceof Buffer ? data : Buffer.from(data);
          
          // Parse the Snap Quote data
          const snapQuoteData = parseSnapQuoteData(buffer);
          if (!snapQuoteData) {
            logger.warn('parseSnapQuoteData returned null or undefined');
            return;
          }
          
          const token = snapQuoteData.token;
          
          // Update symbol price in Redis and order plans
          const symbol = tokenToSymbolMap.get(token);
          if (symbol) {
            logger.info(`Received Snap Quote with Best Five Data for ${symbol}`);
            
            // Update market depth
            updateMarketDepth(symbol, token, snapQuoteData)
              .catch(err => logger.error(`Error updating market depth: ${err.message}`));
          } else {
            logger.debug(`No symbol mapping found for token ${token} in Snap Quote`);
          }
        }
        else if (bytes.length > 2 && bytes[2] === 0x37) {
          // This looks like an acknowledgment message
          handleAcknowledgmentMessage(data);
        }
      } catch (binaryError) {
        logger.error(`Error processing Market Depth binary message: ${binaryError.message}`);
        console.error('Stack trace:', binaryError.stack);
      }
      return;
    }
    
    // Handle text messages (could be acknowledgments, errors, etc.)
    try {
      const message = JSON.parse(data.toString());
      logger.debug(`Received Market Depth text message: ${JSON.stringify(message)}`);
      
      // Handle authentication response
      if (message.action === 1 && message.response) {
        if (message.response.status) {
          logger.info('Market Depth WebSocket authentication successful');
          depthConnectionState = ConnectionState.AUTHENTICATED;
        } else {
          logger.error(`Market Depth WebSocket authentication failed: ${message.response.message || 'Unknown error'}`);
          depthConnectionState = ConnectionState.DISCONNECTED;
          if (wsMarketDepth) wsMarketDepth.close();
        }
      }
    } catch (textError) {
      logger.error(`Error processing Market Depth text message: ${textError.message}`);
    }
    
  } catch (error) {
    logger.error(`Error handling Market Depth WebSocket message: ${error.message}`);
    if (error.stack) {
      logger.debug(`Stack trace: ${error.stack}`);
    }
  }
}

/**
 * Send authentication message for Market Depth WebSocket
 * @param {Object} session - Session information
 */
function sendMarketDepthAuthenticationMessage(session) {
  try {
    const authMessage = {
      correlationID: "marketdepth_websocket_" + Date.now(),
      action: 1, // AUTHENTICATE
      params: {
        clientCode: config.angelOneWebSocket.clientId,
        authorization: session.jwtToken
      }
    };
    
    logger.info('Sending authentication message to Market Depth WebSocket');
    wsMarketDepth.send(JSON.stringify(authMessage));
    depthConnectionState = ConnectionState.AUTHENTICATING;
  } catch (error) {
    logger.error('Error sending authentication message to Market Depth WebSocket:', error);
    if (wsMarketDepth) wsMarketDepth.close();
  }
}

/**
 * Close Market Depth WebSocket connection and clear intervals
 */
function closeMarketDepthConnection() {
  if (wsMarketDepth) {
    try {
      wsMarketDepth.terminate();
    } catch (err) {
      logger.warn(`Error terminating existing Market Depth WebSocket: ${err.message}`);
    }
    wsMarketDepth = null;
  }
  
  // Clear intervals specific to Market Depth connection
  clearMarketDepthIntervals();
}

/**
 * Clear intervals for Market Depth WebSocket
 */
function clearMarketDepthIntervals() {
  try {
    // Clear ping interval
    if (depthPingInterval) {
      clearInterval(depthPingInterval);
      depthPingInterval = null;
    }
    
    // Clear market data request interval
    if (depthMarketDataRequestInterval) {
      clearInterval(depthMarketDataRequestInterval);
      depthMarketDataRequestInterval = null;
    }
    
    // Clear connection health check interval
    if (depthConnectionHealthCheckInterval) {
      clearInterval(depthConnectionHealthCheckInterval);
      depthConnectionHealthCheckInterval = null;
    }
    
    // Clear buffer cleanup interval
    if (depthBufferCleanupInterval) {
      clearInterval(depthBufferCleanupInterval);
      depthBufferCleanupInterval = null;
    }
    
    // Clear message assembly timeout
    if (depthMessageAssemblyTimeout) {
      clearTimeout(depthMessageAssemblyTimeout);
      depthMessageAssemblyTimeout = null;
    }
    
    // Reset buffer
    depthBinaryMessageBuffer = null;
    depthExpectedMessageLength = null;
    
    logger.info('Market Depth WebSocket intervals cleared');
  } catch (error) {
    logger.error(`Error clearing Market Depth intervals: ${error.message}`);
  }
}

/**
 * Initialize WebSocket connection and subscriptions
 */

export async function initializeWebSocket() {
  try {
    // Authenticate with Angel Broking
    const session = await authenticate();
    
    // Connect to WebSocket
    await connectWebSocket(session);
    
    // Set up subscription refresh interval
    setInterval(refreshSubscriptions, 60 * 60 * 1000); // Every hour
    
    // Initial subscription setup
    await setupInitialSubscriptions();
    
    // Listen for new order plans using the subscriber connection
    redisSubscriber.subscribe('orderplan:new');
    redisSubscriber.subscribe('orderplan:delete');
    
    redisSubscriber.on('message', async (channel, message) => {
      if (channel === 'orderplan:new') {
        const planId = message;
        await subscribeToOrderPlan(planId);
      } else if (channel === 'orderplan:delete') {
        const planId = message;
        await unsubscribeFromOrderPlan(planId);
      }
    });
    
    logger.info('WebSocket service initialized');
  } catch (error) {
    logger.error('Failed to initialize WebSocket service:', error);
    throw error;
  }
}

/**
 * Authenticate with Angel Broking
 * @returns {Promise<Object>} Session details
 */
async function authenticate() {
  try {
    logger.info('Authenticating with Angel Broking');
    const totp = generateTOTP(config.angelOneWebSocket.totp);
    const loginResponse = await smartApi.generateSession(
      config.angelOneWebSocket.clientId,
      config.angelOneWebSocket.password,
      totp
    );
    
    if (!loginResponse.data || !loginResponse.data.jwtToken) {
      throw new Error('Authentication failed: Invalid response');
    }
    
    logger.info('Authentication successful');
    
    return {
      jwtToken: loginResponse.data.jwtToken,
      refreshToken: loginResponse.data.refreshToken,
      feedToken: loginResponse.data.feedToken
    };
  } catch (error) {
    logger.error('Authentication error:', error);
    throw new Error(`Authentication failed: ${error.message}`);
  }
}

/**
 * Connect WebSocket with proper state management
 * @param {Object} session - Authentication session with jwtToken and feedToken
 * @returns {Promise<void>}
 */
function connectWebSocket(session) {
  return new Promise((resolve, reject) => {
    try {
      // Validate session
      if (!session || !session.jwtToken) {
        throw new Error('Invalid session: missing JWT token');
      }
      
      // Close existing connection if any
      closeExistingConnection();
      
      // Update connection state
      connectionState = ConnectionState.CONNECTING;
      logger.info('WebSocket connection state:', connectionState);
      
      // Store current session for later use
      currentSession = session;
      
      // Reset variables
      resetState();
      
      // Create new WebSocket connection
      const wsUrl = config.angelOne.wsUrl || 'wss://smartapisocket.angelone.in/smart-stream';
      logger.info(`Connecting to WebSocket at ${wsUrl}`);
      
      // Create headers
      const headers = {
        'Authorization': `Bearer ${session.jwtToken}`,
        'x-api-key': config.angelOne.apiKey,
        'x-client-code': config.angelOneWebSocket.clientId,
        'x-feed-token': session.feedToken || session.jwtToken
      };
      
      ws = new WebSocket(wsUrl, { headers });
      
      // Set binary type
      ws.binaryType = 'arraybuffer';
      
      // Set connection timeout
      const connectionTimeout = setTimeout(() => {
        if (connectionState !== ConnectionState.READY) {
          logger.error('WebSocket connection timeout after 30 seconds');
          connectionState = ConnectionState.DISCONNECTED;
          
          if (ws) {
            try {
              ws.terminate();
            } catch (err) {
              logger.warn(`Error terminating WebSocket: ${err.message}`);
            }
            ws = null;
          }
          
          reject(new Error('WebSocket connection timeout'));
        }
      }, 30000);
      
      // Set up event handlers
      setupEventHandlers(resolve, reject, connectionTimeout);
      
    } catch (error) {
      logger.error('Error connecting to WebSocket:', error);
      
      // Update state
      connectionState = ConnectionState.DISCONNECTED;
      logger.info('WebSocket connection state:', connectionState);
      
      reject(error);
    }
  });
}

/**
 * Close existing WebSocket connection and clear intervals
 */
function closeExistingConnection() {
  if (ws) {
    try {
      ws.terminate();
    } catch (err) {
      logger.warn(`Error terminating existing WebSocket: ${err.message}`);
    }
    ws = null;
  }
  
  // Clear intervals
  clearIntervals();
}

/**
 * Reset connection state variables
 */
function resetState() {
  binaryMessageBuffer = null;
  expectedMessageLength = null;
  
  if (messageAssemblyTimeout) {
    clearTimeout(messageAssemblyTimeout);
    messageAssemblyTimeout = null;
  }
  
  // Reset connection tracking
  lastMessageTime = Date.now();
  lastPongTime = Date.now();
  reconnectAttempts = 0;
}

/**
 * Clear all intervals
 */
function clearIntervals() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
  
  if (marketDataRequestInterval) {
    clearInterval(marketDataRequestInterval);
    marketDataRequestInterval = null;
  }
  
  if (connectionHealthCheckInterval) {
    clearInterval(connectionHealthCheckInterval);
    connectionHealthCheckInterval = null;
  }
  
  if (bufferCleanupInterval) {
    clearInterval(bufferCleanupInterval);
    bufferCleanupInterval = null;
  }
}

/**
 * Set up WebSocket event handlers
 * @param {Function} resolve - Promise resolve function
 * @param {Function} reject - Promise reject function
 * @param {Timeout} connectionTimeout - Connection timeout
 */
function setupEventHandlers(resolve, reject, connectionTimeout) {
  ws.on('open', () => {
    logger.info('WebSocket connection established');
    
    // Set up intervals
    setupIntervals();
    
    // Send authentication message
    sendAuthenticationMessage();
  });
  
  ws.on('message', (data) => {
    try {
      handleWebSocketMessage(data);
    } catch (err) {
      logger.error(`Error in message handler: ${err.message}`);
      if (err.stack) logger.debug(`Stack trace: ${err.stack}`);
    }
  });
  
  ws.on('error', (error) => {
    logger.error('WebSocket error:', error);
    
    // Clear connection timeout
    clearTimeout(connectionTimeout);
    
    // Update state
    connectionState = ConnectionState.DISCONNECTED;
    logger.info('WebSocket connection state:', connectionState);
    
    // Reject the promise if we're still connecting
    if (reconnectAttempts === 0) {
      reject(error);
    }
  });
  
  ws.on('close', (code, reason) => {
    logger.warn(`WebSocket connection closed: ${code} ${reason}`);
    
    // Clear connection timeout
    clearTimeout(connectionTimeout);
    
    // Update state
    connectionState = ConnectionState.DISCONNECTED;
    logger.info('WebSocket connection state:', connectionState);
    
    // Clear intervals
    clearIntervals();
    
    // Reconnect with exponential backoff
    handleReconnect();
  });
  
  ws.on('pong', () => {
    logger.debug('Received pong from server');
    lastPongTime = Date.now();
  });
  
  // Set up a special handler to update connection state to READY after authentication
  // This is important to avoid a race condition
  setTimeout(() => {
    if (connectionState === ConnectionState.AUTHENTICATED || connectionState === ConnectionState.CONNECTING) {
      logger.info('Setting connection state to READY after timeout');
      connectionState = ConnectionState.READY;
      
      // Process pending subscriptions
      try {
        processPendingSubscriptions();
      } catch (err) {
        logger.error(`Error processing pending subscriptions: ${err.message}`);
      }
      
      // Request market data
      try {
        requestMarketData();
      } catch (err) {
        logger.error(`Error requesting market data: ${err.message}`);
      }
      
      // Resolve the promise
      resolve();
    }
  }, 5000); // 5 second timeout
}

/**
 * Set up maintenance intervals
 */
function setupIntervals() {
  // Set up ping interval
  pingInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      try {
        logger.debug('Sending ping to keep WebSocket alive');
        ws.ping();
      } catch (err) {
        logger.warn(`Error sending ping: ${err.message}`);
      }
    }
  }, 30000);
  
  // Set up market data request interval
  marketDataRequestInterval = setInterval(() => {
    if (connectionState === ConnectionState.READY && ws && ws.readyState === WebSocket.OPEN) {
      try {
        requestMarketData();
      } catch (err) {
        logger.warn(`Error requesting market data: ${err.message}`);
      }
    }
  }, 60000);
  
  // Set up connection health check
  connectionHealthCheckInterval = setInterval(() => {
    try {
      checkConnectionHealth();
    } catch (err) {
      logger.warn(`Error checking connection health: ${err.message}`);
    }
  }, 60000);
  
  // Set up buffer cleanup interval
  bufferCleanupInterval = setInterval(() => {
    try {
      cleanupMessageBuffers();
    } catch (err) {
      logger.warn(`Error cleaning up message buffers: ${err.message}`);
    }
  }, 10000);
}

/**
 * Send authentication message
 */
function sendAuthenticationMessage() {
  try {
    const authMessage = {
      correlationID: "websocket_connection_" + Date.now(),
      action: 1, // AUTHENTICATE
      params: {
        clientCode: config.angelOneWebSocket.clientId,
        authorization: currentSession.jwtToken
      }
    };
    
    logger.info('Sending authentication message to WebSocket');
    ws.send(JSON.stringify(authMessage));
  } catch (error) {
    logger.error('Error sending authentication message:', error);
    if (ws) ws.close();
  }
}

/**
 * Handle LTP WebSocket reconnection with exponential backoff
 */
function handleLTPReconnect() {
  if (ltpReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    ltpReconnectAttempts++;
    const delay = RECONNECT_DELAY * Math.pow(1.5, ltpReconnectAttempts - 1);
    
    logger.info(`Attempting to reconnect LTP WebSocket in ${delay}ms (attempt ${ltpReconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
    
    ltpConnectionState = ConnectionState.RECONNECTING;
    logger.info('LTP WebSocket connection state:', ltpConnectionState);
    
    setTimeout(() => {
      try {
        authenticate()
          .then(newSession => connectLTPWebSocket(newSession))
          .catch(err => logger.error('LTP reconnect failed:', err));
      } catch (err) {
        logger.error(`Error in LTP reconnect logic: ${err.message}`);
      }
    }, delay);
  } else {
    logger.error('Maximum LTP reconnection attempts reached. Giving up.');
  }
}



/**
 * Handle Market Depth WebSocket reconnection with exponential backoff
 */
function handleMarketDepthReconnect() {
  if (depthReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    depthReconnectAttempts++;
    const delay = RECONNECT_DELAY * Math.pow(1.5, depthReconnectAttempts - 1);
    
    logger.info(`Attempting to reconnect Market Depth WebSocket in ${delay}ms (attempt ${depthReconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
    
    depthConnectionState = ConnectionState.RECONNECTING;
    logger.info('Market Depth WebSocket connection state:', depthConnectionState);
    
    setTimeout(() => {
      try {
        authenticate()
          .then(newSession => connectMarketDepthWebSocket(newSession))
          .catch(err => logger.error('Market Depth reconnect failed:', err));
      } catch (err) {
        logger.error(`Error in Market Depth reconnect logic: ${err.message}`);
      }
    }, delay);
  } else {
    logger.error('Maximum Market Depth reconnection attempts reached. Giving up.');
  }
}

/**
 * Handle reconnection with exponential backoff
 */
function handleReconnect() {
  if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    reconnectAttempts++;
    const delay = RECONNECT_DELAY * Math.pow(1.5, reconnectAttempts - 1);
    
    logger.info(`Attempting to reconnect in ${delay}ms (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
    
    connectionState = ConnectionState.RECONNECTING;
    logger.info('WebSocket connection state:', connectionState);
    
    setTimeout(() => {
      try {
        authenticate()
          .then(newSession => connectWebSocket(newSession))
          .catch(err => logger.error('Reconnect failed:', err));
      } catch (err) {
        logger.error(`Error in reconnect logic: ${err.message}`);
      }
    }, delay);
  } else {
    logger.error('Maximum reconnection attempts reached. Giving up.');
  }
}

/**
 * Handle WebSocket messages
 * @param {any} data - Message data
 */
function handleWebSocketMessage(data) {
  try {
    // Update last message time
    lastMessageTime = Date.now();
    
    // Handle binary data
    if (data instanceof Buffer || data instanceof ArrayBuffer) {
      try {
        // Get size for logging
        const size = data instanceof Buffer ? data.length : data.byteLength;
        logger.debug(`Received WebSocket message: ${data instanceof Buffer ? 'Buffer' : 'ArrayBuffer'}, size: ${size} bytes`);
        
        // Get first byte for message type detection
        const bytes = data instanceof Buffer ? 
          new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : 
          new Uint8Array(data);
        
        if (bytes.length === 0) {
          logger.warn('Received empty binary message');
          return;
        }
        
        const firstByte = bytes[0];
        
        // Check message type
        if (size === 51 && firstByte === 1) {
          // This is an LTP mode market data message
          const tickData = handleDataTicks(data);
          
          if (!tickData) {
            logger.warn('handleDataTicks returned null or undefined');
            return;
          }
          
          if (tickData.error) {
            logger.warn(`Error in tick data: ${tickData.error}`);
            return;
          }
          
          if (!tickData.token) {
            logger.warn('Tick data missing token');
            return;
          }
          
          // Process the tick data
          const token = tickData.token;
          logger.debug(`Processed LTP market data for token ${token}`);
          
          // Create a simplified tick object for existing code
          const simpleTick = {
            token,
            ltp: tickData.lastTradedPrice || 0,
            open: tickData.openPrice || 0,
            high: tickData.highPrice || 0,
            low: tickData.lowPrice || 0,
            close: tickData.closePrice || 0,
            totalTradedQty: tickData.volumeTradedToday || 0,
            totBuyQty: tickData.totalBuyQuantity || 0,
            totSellQty: tickData.totalSellQuantity || 0,
            sequenceNumber: tickData.sequenceNumber,
            exchangeTimestamp: tickData.exchangeTimestamp
          };
          
          // Store last tick data
          lastTicks.set(token, simpleTick);
          
          // Update symbol price in Redis and order plans
          const symbol = tokenToSymbolMap.get(token);
          if (symbol) {
            updateSymbolPrice(symbol, token, simpleTick)
              .catch(err => logger.error(`Async error updating symbol price: ${err.message}`));
            
            // Update order plans that use this symbol
            if (subscriptions.has(token)) {
              const planIds = subscriptions.get(token);
              for (const planId of planIds) {
                if (planId) {
                  updateOrderPlanPrice(planId, simpleTick)
                    .catch(err => logger.error(`Async error updating order plan: ${err.message}`));
                }
              }
            }
            
            // Calculate price change for logging
            const priceChange = simpleTick.close > 0 ? 
              ((simpleTick.ltp - simpleTick.close) / simpleTick.close * 100).toFixed(2) + '%' : 
              'N/A';
            
            logger.info(`Market data for ${symbol}: LTP=${simpleTick.ltp}, Change=${priceChange}`);
          } else {
            logger.debug(`No symbol mapping found for token ${token}`);
          }
        } 
        else if (bytes.length > 2 && bytes[2] === 0x37) {
          // This looks like an acknowledgment message
          handleAcknowledgmentMessage(data);
        }
        else {
          // Process other binary messages
          processBinaryMessage(data);
        }
      } catch (binaryError) {
        logger.error(`Error processing binary message: ${binaryError.message}`);
        console.error('Stack trace:', binaryError.stack);
      }
      return;
    }
    
    // Handle text messages...
    // Rest of the code remains the same
  } catch (error) {
    logger.error(`Error handling WebSocket message: ${error.message}`);
    if (error.stack) {
      logger.debug(`Stack trace: ${error.stack}`);
    }
  }
}

/**
 * Handle acknowledgment messages
 * @param {Buffer|ArrayBuffer} data - The message data
 */
function handleAcknowledgmentMessage(data) {
  try {
    // Convert to ArrayBuffer if needed
    const buffer = data instanceof Buffer ? 
      data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : 
      data;
    
    // Create views for reading the data
    const bytes = new Uint8Array(buffer);
    const dataView = new DataView(buffer);
    
    // Verify message type (byte 2 should be 0x37 = 55)
    if (bytes[2] !== 0x37) {
      logger.warn(`Expected message type 0x37, got 0x${bytes[2].toString(16)}`);
      return;
    }
    
    // Extract message ID (bytes 3-6)
    let messageId = '';
    for (let i = 3; i < 7; i++) {
      if (bytes[i] >= 32 && bytes[i] <= 126) {
        messageId += String.fromCharCode(bytes[i]);
      }
    }
    
    // Extract status code (bytes 38-39, observed from your data)
    let statusCode = 0;
    if (buffer.byteLength >= 40) {
      statusCode = dataView.getUint16(38, true);
    }
    
    logger.debug(`Acknowledgment message: ID=${messageId}, status=${statusCode} (0x${statusCode.toString(16)})`);
    
    // Handle status code
    if (statusCode !== 0) {
      switch (statusCode) {
        case 307:
          logger.info('Received status code 307 (temporary redirect), refreshing subscriptions');
          // Schedule resubscription
          setTimeout(() => {
            resubscribeAll();
          }, 2000);
          break;
          
        case 4650: // 0x122A (observed in your logs)
          logger.debug(`Received status code 4650 (0x122A), possibly a heartbeat`);
          break;
          
        default:
          logger.debug(`Received status code ${statusCode} (0x${statusCode.toString(16)})`);
          break;
      }
    }
  } catch (error) {
    logger.error(`Error handling acknowledgment message: ${error.message}`);
  }
}


/**
 * Process general binary message
 * @param {Buffer|ArrayBuffer} data - The message data
 */
function processBinaryMessage(data) {
  const buffer = data instanceof Buffer ? data : Buffer.from(data);
  
  // Get message length and type for logging
  const messageLength = buffer.length;
  const messageType = buffer[0];
  
  logger.debug(`Binary message: length=${messageLength}, type=${messageType} (0x${messageType.toString(16)})`);
  
  // Handle different message types
  if (messageType === 1) {
    // LTP mode message
    const tickData = handleDataTicks(buffer);
    // Process tick data...
  } 
  else if (messageType === 3) {  // 0x3 = 3 in decimal - Snap Quote mode
    try {
      const snapQuoteData = parseSnapQuoteData(buffer);
      if (!snapQuoteData) {
        logger.warn('parseSnapQuoteData returned null or undefined');
        return;
      }
      
      const token = snapQuoteData.token;
      
      // Store last tick data
      lastTicks.set(token, snapQuoteData);
      
      // Update symbol price in Redis and order plans
      const symbol = tokenToSymbolMap.get(token);
      if (symbol) {
        logger.info(`Received Snap Quote with Best Five Data for ${symbol}`);
        
        // Create a simplified tick object for existing code
        const simpleTick = {
          token,
          ltp: snapQuoteData.lastTradedPrice || 0,
          open: snapQuoteData.openPrice || 0,
          high: snapQuoteData.highPrice || 0,
          low: snapQuoteData.lowPrice || 0,
          close: snapQuoteData.closePrice || 0,
          totalTradedQty: snapQuoteData.volumeTradedToday || 0,
          totBuyQty: snapQuoteData.totalBuyQuantity || 0,
          totSellQty: snapQuoteData.totalSellQuantity || 0,
          sequenceNumber: snapQuoteData.sequenceNumber,
          exchangeTimestamp: snapQuoteData.exchangeTimestamp,
          bestFive: snapQuoteData.bestFive  // Include Best Five data
        };
        
        // Update symbol price
        updateSymbolPrice(symbol, token, simpleTick)
          .catch(err => logger.error(`Async error updating symbol price: ${err.message}`));
        
        // Update market depth
        updateMarketDepth(symbol, token, snapQuoteData)
          .catch(err => logger.error(`Error updating market depth: ${err.message}`));
          
        // Update order plans that use this symbol
        if (subscriptions.has(token)) {
          const planIds = subscriptions.get(token);
          for (const planId of planIds) {
            if (planId) {
              updateOrderPlanPrice(planId, simpleTick)
                .catch(err => logger.error(`Async error updating order plan: ${err.message}`));
            }
          }
        }
      } else {
        logger.debug(`No symbol mapping found for token ${token} in Snap Quote`);
      }
    } catch (error) {
      logger.error(`Error processing Snap Quote data: ${error.message}`);
      if (error.stack) {
        logger.debug(`Stack trace: ${error.stack}`);
      }
    }
  }
  else {
    logger.warn(`Unknown binary message type: ${messageType} (0x${messageType.toString(16)})`);
  }
}

async function updateMarketDepth(symbol, token, snapQuoteData) {
  try {
    const depthKey = `marketdepth:${symbol}`;
    
    const depthData = {
      symbol,
      token,
      lastPrice: snapQuoteData.lastTradedPrice,
      bestFive: snapQuoteData.bestFive,
      lastUpdateTime: Date.now()
    };
    
    // Store in Redis
    await redisClient.set(depthKey, JSON.stringify(depthData));
    logger.debug(`Successfully updated market depth for ${symbol} in Redis`);
    
    // Publish update for subscribers
    await redisPublisher.publish(`marketdepth:update:${symbol}`, JSON.stringify(depthData));
    logger.debug(`Successfully published market depth update for ${symbol}`);
    
  } catch (error) {
    logger.error(`Error updating market depth for ${symbol}: ${error.message}`);
  }
}

function parseSnapQuoteData(buffer) {
  try {
    // Log buffer details for debugging
    logger.debug(`Parsing Snap Quote buffer of length ${buffer.length}`);
    
    // Dump the first 40 bytes for analysis
    const headerBytes = buffer.slice(0, 40).toString('hex');
    logger.debug(`First 40 bytes of snap quote: ${headerBytes}`);
    
    // Extract token as string from position 2
    let tokenStr = '';
    for (let i = 2; i < 20; i++) {
      const byte = buffer[i];
      if (byte === 0) break;
      tokenStr += String.fromCharCode(byte);
    }
    
    logger.debug(`Extracted token as string: "${tokenStr}"`);
    
    // Try to convert to number if it's numeric
    const tokenNum = parseInt(tokenStr, 10);
    const token = isNaN(tokenNum) ? tokenStr : tokenNum;
    
    logger.debug(`Final token value: ${token} (${typeof token})`);
    
    // Look for the offset where price data begins
    // First, let's find the first buy entry position
    const bestFiveStartPosition = 147; // From previous logs
    
    // Let's read some data at potential price positions and log them
    // Try different positions and formats for the prices
    const possiblePositions = [30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100];
    
    for (const pos of possiblePositions) {
      if (pos + 8 <= buffer.length) {
        const valueAsDouble = buffer.readDoubleLE(pos);
        const valueAsFloat = buffer.readFloatLE(pos);
        const valueAsInt32 = buffer.readInt32LE(pos);
        const valueAsInt32Div100 = valueAsInt32 / 100;
        
        // Only log if values are reasonable (not extremely small or large)
        if (Math.abs(valueAsDouble) > 1e-100 && Math.abs(valueAsDouble) < 1e100) {
          logger.debug(`Position ${pos} as double: ${valueAsDouble}`);
        }
        
        if (Math.abs(valueAsFloat) > 1e-20 && Math.abs(valueAsFloat) < 1e20) {
          logger.debug(`Position ${pos} as float: ${valueAsFloat}`);
        }
        
        if (Math.abs(valueAsInt32) < 1000000) {
          logger.debug(`Position ${pos} as int32: ${valueAsInt32}, divided by 100: ${valueAsInt32Div100}`);
        }
      }
    }
    
    // Assuming the price fields are stored as 4-byte integers divided by 100
    // Based on the best five prices which seem reasonable (around 145-150)
    // Let's try these positions
    const ltpPos = 35;
    const openPos = 39;
    const highPos = 43;
    const lowPos = 47;
    const closePos = 51;
    const volumePos = 55;
    const totalBuyQuantityPos = 59;
    const totalSellQuantityPos = 63;
    
    // Read prices as integers divided by 100
    const ltp = buffer.readInt32LE(ltpPos) / 100 || 0;
    const openPrice = buffer.readInt32LE(openPos) / 100 || 0;
    const highPrice = buffer.readInt32LE(highPos) / 100 || 0;
    const lowPrice = buffer.readInt32LE(lowPos) / 100 || 0;
    const closePrice = buffer.readInt32LE(closePos) / 100 || 0;
    const volume = buffer.readInt32LE(volumePos) || 0;
    const totalBuyQuantity = buffer.readInt32LE(totalBuyQuantityPos) || 0;
    const totalSellQuantity = buffer.readInt32LE(totalSellQuantityPos) || 0;
    
    // Log the extracted price data
    logger.debug(`Extracted prices: LTP=${ltp}, Open=${openPrice}, High=${highPrice}, Low=${lowPrice}, Close=${closePrice}`);
    logger.debug(`Extracted volume: ${volume}, Buy Qty: ${totalBuyQuantity}, Sell Qty: ${totalSellQuantity}`);
    
    // Extract best five data
    let bestFiveData = null;
    if (buffer.length >= bestFiveStartPosition + 10*20) {
        bestFiveData = extractBestFiveData(buffer, bestFiveStartPosition);
    } else {
        logger.warn(`Buffer too short for Best Five Data extraction: ${buffer.length}`);
    }
    
    const result = {
      token: token.toString(),
      lastTradedPrice: ltp,
      openPrice,
      highPrice,
      lowPrice,
      closePrice,
      volumeTradedToday: volume,
      totalBuyQuantity,
      totalSellQuantity,
      bestFive: bestFiveData
    };
    
    return result;
  } catch (error) {
    logger.error(`Error parsing Snap Quote data: ${error.message}`);
    logger.debug(`Buffer length: ${buffer.length}, First 20 bytes: ${buffer.slice(0, 20).toString('hex')}`);
    throw error;
  }
}

/**
 * Check connection health
 */
function checkConnectionHealth() {
  try {
    if (!ws) {
      logger.warn('No WebSocket connection to check');
      return;
    }
    
    const now = Date.now();
    const timeSinceLastMessage = now - lastMessageTime;
    const timeSinceLastPong = now - lastPongTime;
    
    logger.debug(`Connection health: Last message ${timeSinceLastMessage}ms ago, last pong ${timeSinceLastPong}ms ago`);
    
    // If no message for 5 minutes, reconnect
    if (timeSinceLastMessage > 5 * 60 * 1000) {
      logger.warn(`No messages received for ${Math.floor(timeSinceLastMessage/1000)}s, reconnecting...`);
      reconnectWebSocket();
    }
    
    // If no pong for 2 minutes, reconnect
    if (timeSinceLastPong > 2 * 60 * 1000) {
      logger.warn(`No pong received for ${Math.floor(timeSinceLastPong/1000)}s, reconnecting...`);
      reconnectWebSocket();
    }
  } catch (error) {
    logger.error(`Error checking connection health: ${error.message}`);
  }
}

/**
 * Clean up message buffers
 */
function cleanupMessageBuffers() {
  try {
    if (binaryMessageBuffer && expectedMessageLength) {
      const now = Date.now();
      const bufferAge = now - lastMessageTime;
      
      // If buffer is older than 30 seconds, clean it up
      if (bufferAge > 30000) {
        logger.warn(`Cleaning up stale message buffer (age: ${bufferAge}ms)`);
        
        // Reset buffer state
        binaryMessageBuffer = null;
        expectedMessageLength = null;
        
        // Clear timeout
        if (messageAssemblyTimeout) {
          clearTimeout(messageAssemblyTimeout);
          messageAssemblyTimeout = null;
        }
      }
    }
  } catch (error) {
    logger.error(`Error cleaning up message buffers: ${error.message}`);
  }
}

/**
 * Reconnect WebSocket after failure
 */
function reconnectWebSocket() {
  try {
    logger.info('Initiating WebSocket reconnection');
    
    // Close existing connection
    closeExistingConnection();
    
    // Update state
    connectionState = ConnectionState.RECONNECTING;
    logger.info('WebSocket connection state:', connectionState);
    
    // Re-authenticate and reconnect
    authenticate()
      .then(newSession => connectWebSocket(newSession))
      .then(() => {
        logger.info('WebSocket reconnection successful');
      })
      .catch(error => {
        logger.error('WebSocket reconnection failed:', error);
        
        // Schedule another reconnect attempt if within limits
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          handleReconnect();
        } else {
          logger.error('Maximum reconnection attempts reached. Giving up.');
        }
      });
  } catch (error) {
    logger.error('Error in reconnectWebSocket:', error);
  }
}

/**
 * Process pending subscriptions
 */
function processPendingSubscriptions() {
  try {
    logger.info('Processing pending subscriptions');
    
    // Check connection state
    if (connectionState !== ConnectionState.READY) {
      logger.warn(`Cannot process subscriptions: WebSocket not ready (state: ${connectionState})`);
      return;
    }
    
    // If no pending subscriptions, return
    if (pendingSubscriptions.size === 0) {
      logger.info('No pending subscriptions to process');
      return;
    }
    
    // Group tokens by exchange for subscription
    const tokensByExchange = new Map(); // exchangeType -> array of tokens
    
    // Process all pending subscriptions
    for (const [token, details] of pendingSubscriptions.entries()) {
      // Get exchange type
      const exchangeType = mapExchangeToType(details.exchange);
      
      // Add to tokens by exchange map
      if (!tokensByExchange.has(exchangeType)) {
        tokensByExchange.set(exchangeType, []);
      }
      
      tokensByExchange.get(exchangeType).push(parseInt(token, 10));
      
      // Add to subscriptions map
      if (!subscriptions.has(token)) {
        subscriptions.set(token, new Set());
      }
      
      // Add plan ID to the token's subscriptions
      subscriptions.get(token).add(details.planId);
      
      // Map token to symbol
      if (details.symbol) {
        tokenToSymbolMap.set(token, details.symbol);
        symbolToTokenMap.set(details.symbol, token);
      }
    }
    
    // If no tokens to subscribe, return
    if (tokensByExchange.size === 0) {
      logger.info('No tokens to subscribe to');
      pendingSubscriptions.clear();
      return;
    }
    
    // Build token list for subscription
    const tokenList = [];
    for (const [exchangeType, tokens] of tokensByExchange.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    // Create subscription message
    const subscriptionMessage = {
      correlationID: "subscription_" + Date.now(),
      action: 1, // SUBSCRIBE
      params: {
        mode: priceMode, // FULL mode
        tokenList
      }
    };
    
    // Log subscription summary
    const tokenCount = tokenList.reduce((sum, item) => sum + item.tokens.length, 0);
    logger.info(`Subscribing to ${tokenCount} tokens across ${tokenList.length} exchanges`);
    
    // Send subscription
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(subscriptionMessage));
      
      // Clear pending subscriptions
      pendingSubscriptions.clear();
      
      // Log subscribed tokens
      logger.info(`Resubscribed to ${tokensByExchange.size} exchanges with ${tokenCount} tokens`);
    } else {
      logger.error('WebSocket not connected, cannot subscribe');
    }
  } catch (error) {
    logger.error('Error processing pending subscriptions:', error);
  }
}

/**
 * Parse market data message
 * @param {ArrayBuffer} buffer - The message buffer
 */
function parseMarketData(buffer) {
  try {
    const dataView = new DataView(buffer);
    let position = 3; // Skip length (2 bytes) and type (1 byte)
    
    // Parse exchange type (1 byte)
    const exchangeType = dataView.getUint8(position);
    position += 1;
    
    // Parse number of tokens (2 bytes)
    const tokenCount = dataView.getUint16(position, true);
    position += 2;
    
    logger.debug(`Parsing market data: exchange type ${exchangeType}, token count ${tokenCount}`);
    
    // Prepare ticks array
    const ticks = [];
    
    // Parse each token data
    for (let i = 0; i < tokenCount; i++) {
      // Parse token (4 bytes)
      const token = dataView.getUint32(position, true).toString();
      position += 4;
      
      // Parse sequence number (4 bytes)
      const sequenceNumber = dataView.getUint32(position, true);
      position += 4;
      
      // Parse exchange timestamp (8 bytes)
      const exchangeTimestampLow = dataView.getUint32(position, true);
      position += 4;
      const exchangeTimestampHigh = dataView.getUint32(position, true);
      position += 4;
      const exchangeTimestamp = (BigInt(exchangeTimestampHigh) << BigInt(32) | BigInt(exchangeTimestampLow)).toString();
      
      // Parse last traded price (8 bytes)
      const lastTradedPrice = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse last traded quantity (8 bytes)
      const lastTradedQuantity = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse average traded price (8 bytes)
      const averageTradedPrice = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse volume (8 bytes)
      const volume = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse total buy quantity (8 bytes)
      const totalBuyQuantity = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse total sell quantity (8 bytes)
      const totalSellQuantity = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse open price (8 bytes)
      const openPrice = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse high price (8 bytes)
      const highPrice = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse low price (8 bytes)
      const lowPrice = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse close price (8 bytes)
      const closePrice = dataView.getFloat64(position, true);
      position += 8;
      
      // Add to ticks array
      ticks.push({
        token,
        ltp: lastTradedPrice,
        open: openPrice,
        high: highPrice,
        low: lowPrice,
        close: closePrice,
        totalTradedQty: volume,
        totBuyQty: totalBuyQuantity,
        totSellQty: totalSellQuantity,
        sequenceNumber,
        exchangeTimestamp,
        lastTradedQuantity,
        averageTradedPrice
      });
    }
    
    // Process the ticks
    if (ticks.length > 0) {
      handleDataTicks(ticks);
    }
  } catch (error) {
    logger.error(`Error parsing market data: ${error.message}`);
  }
}

/**
 * Parse index data message
 * @param {ArrayBuffer} buffer - The message buffer
 */
function parseIndexData(buffer) {
  try {
    const dataView = new DataView(buffer);
    let position = 3; // Skip length (2 bytes) and type (1 byte)
    
    // Parse exchange type (1 byte)
    const exchangeType = dataView.getUint8(position);
    position += 1;
    
    // Parse number of indices (2 bytes)
    const indexCount = dataView.getUint16(position, true);
    position += 2;
    
    logger.debug(`Parsing index data: exchange type ${exchangeType}, index count ${indexCount}`);
    
    // Prepare ticks array
    const ticks = [];
    
    // Parse each index
    for (let i = 0; i < indexCount; i++) {
      // Parse index token (4 bytes)
      const token = dataView.getUint32(position, true).toString();
      position += 4;
      
      // Parse sequence number (4 bytes)
      const sequenceNumber = dataView.getUint32(position, true);
      position += 4;
      
      // Parse timestamp (8 bytes)
      const timestampLow = dataView.getUint32(position, true);
      position += 4;
      const timestampHigh = dataView.getUint32(position, true);
      position += 4;
      const timestamp = (BigInt(timestampHigh) << BigInt(32) | BigInt(timestampLow)).toString();
      
      // Parse index value (8 bytes)
      const indexValue = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse open value (8 bytes)
      const openValue = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse high value (8 bytes)
      const highValue = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse low value (8 bytes)
      const lowValue = dataView.getFloat64(position, true);
      position += 8;
      
      // Parse close value (8 bytes)
      const closeValue = dataView.getFloat64(position, true);
      position += 8;
      
      // Add to ticks array
      ticks.push({
        token,
        ltp: indexValue,
        open: openValue,
        high: highValue,
        low: lowValue,
        close: closeValue,
        sequenceNumber,
        exchangeTimestamp: timestamp,
        isIndex: true
      });
    }
    
    // Process the ticks
    if (ticks.length > 0) {
      handleDataTicks(ticks);
    }
  } catch (error) {
    logger.error(`Error parsing index data: ${error.message}`);
  }
}

/**
 * Handle data ticks
 * @param {ArrayBuffer|Buffer} buffer - Binary data received from WebSocket
 * @returns {Object|null} Decoded market data object or null if error
 */
function handleDataTicks(buffer) {
  try {
    // Ensure we have a proper ArrayBuffer to work with
    const arrayBuffer = buffer instanceof Buffer ? 
      buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) : 
      buffer;
    
    const dataView = new DataView(arrayBuffer);
    const bytes = new Uint8Array(arrayBuffer);
    
    // Extract basic header information
    const mode = bytes[0]; // Subscription mode (1=LTP, 2=Quote, 3=Snap Quote)
    const exchangeType = bytes[1]; // Exchange type code
    
    // Log the raw data for debugging
    logger.debug(`Decoding market data: mode=${mode}, exchangeType=${exchangeType}, message size=${arrayBuffer.byteLength}`);
    
    // Extract token (string terminated by null byte)
    let token = '';
    for (let i = 2; i < 27; i++) {
      if (bytes[i] === 0) {
        break;
      }
      token += String.fromCharCode(bytes[i]);
    }
    
    // Safety check on token
    if (!token) {
      logger.warn(`Received market data with empty token, skipping`);
      return null;
    }
    
    // Extract sequence number (8 bytes, little-endian)
    const sequenceNumber = Number(dataView.getBigUint64(27, true));
    
    // Extract exchange timestamp (8 bytes, little-endian)
    const exchangeTimestamp = Number(dataView.getBigUint64(35, true));
    
    // Get exchange name safely
    let exchangeName = getExchangeName(exchangeType);
    
    // Create base response object with common fields
    const response = {
      mode,
      exchangeTypeCode: exchangeType,
      exchangeType: exchangeName,
      token,
      sequenceNumber,
      exchangeTimestamp: new Date(exchangeTimestamp).toISOString(),
      rawExchangeTimestamp: exchangeTimestamp
    };
    
    // Get the trading symbol if available
    const symbol = tokenToSymbolMap.get(token);
    if (symbol) {
      response.tradingSymbol = symbol;
    }
    
    // Create a tick data object for compatibility with existing code
    const tickData = {
      token,
      sequenceNumber,
      exchangeTimestamp: response.exchangeTimestamp
    };
    
    // Extract mode-specific data
    if (mode >= 1) { // LTP mode and higher
      try {
        // LTP is in position 43 (4 bytes, little-endian)
        const ltpRaw = dataView.getInt32(43, true);
        
        // Apply price conversion based on exchange type
        response.lastTradedPrice = convertPrice(ltpRaw, exchangeType);
        tickData.ltp = response.lastTradedPrice;
        
        if (mode === 1) {
          // For LTP mode, we're done here (packet size = 51 bytes)
          logger.debug(`Decoded LTP data for ${symbol || token}: ${response.lastTradedPrice}`);
          
          // Process the tick data
          processTickData(token, symbol, tickData);
          
          return response;
        }
      } catch (ltp_error) {
        logger.error(`Error extracting LTP data: ${ltp_error.message}`);
        response.error = `Failed to extract LTP: ${ltp_error.message}`;
        return response;
      }
    }
    
    if (mode >= 2) { // Quote mode and higher
      try {
        // Last traded quantity - position 51, 8 bytes
        response.lastTradedQuantity = Number(dataView.getBigUint64(51, true));
        tickData.lastTradedQuantity = response.lastTradedQuantity;
        
        // Average traded price - position 59, 8 bytes
        response.averageTradedPrice = convertPrice(Number(dataView.getBigUint64(59, true)), exchangeType);
        tickData.averageTradedPrice = response.averageTradedPrice;
        
        // Volume traded for the day - position 67, 8 bytes
        response.volumeTradedToday = Number(dataView.getBigUint64(67, true));
        tickData.totalTradedQty = response.volumeTradedToday;
        tickData.volume = response.volumeTradedToday;
        
        // Total buy quantity - position 75, 8 bytes (double)
        response.totalBuyQuantity = dataView.getFloat64(75, true);
        tickData.totBuyQty = response.totalBuyQuantity;
        
        // Total sell quantity - position 83, 8 bytes (double)
        response.totalSellQuantity = dataView.getFloat64(83, true);
        tickData.totSellQty = response.totalSellQuantity;
        
        // Open price - position 91, 8 bytes
        response.openPrice = convertPrice(Number(dataView.getBigUint64(91, true)), exchangeType);
        tickData.open = response.openPrice;
        
        // High price - position 99, 8 bytes
        response.highPrice = convertPrice(Number(dataView.getBigUint64(99, true)), exchangeType);
        tickData.high = response.highPrice;
        
        // Low price - position 107, 8 bytes
        response.lowPrice = convertPrice(Number(dataView.getBigUint64(107, true)), exchangeType);
        tickData.low = response.lowPrice;
        
        // Close price - position 115, 8 bytes
        response.closePrice = convertPrice(Number(dataView.getBigUint64(115, true)), exchangeType);
        tickData.close = response.closePrice;
        
        if (mode === 2) {
          // For Quote mode, we're done here (packet size = 123 bytes)
          logger.debug(`Decoded Quote data for ${symbol || token}`);
          
          // Process the tick data
          processTickData(token, symbol, tickData);
          
          return response;
        }
      } catch (quote_error) {
        logger.error(`Error extracting Quote data: ${quote_error.message}`);
        response.error = `Failed to extract Quote data: ${quote_error.message}`;
        
        // Process the partial tick data we have so far
        processTickData(token, symbol, tickData);
        
        return response;
      }
    }
    
    if (mode === 3) { // Snap Quote mode
      try {
        // Last traded timestamp - position 123, 8 bytes
        response.lastTradedTimestamp = Number(dataView.getBigUint64(123, true));
        response.lastTradedTime = new Date(response.lastTradedTimestamp).toISOString();
        
        // Open Interest - position 131, 8 bytes
        response.openInterest = Number(dataView.getBigUint64(131, true));
        tickData.openInterest = response.openInterest;
        
        // Open Interest change % - position 139, 8 bytes (double)
        response.openInterestChangePercent = dataView.getFloat64(139, true);
        tickData.openInterestChangePercent = response.openInterestChangePercent;
        
        // Best Five Data - position 147, 200 bytes (10 packets × 20 bytes)
        response.bestFiveData = extractBestFiveData(arrayBuffer, 147, exchangeType);
        
        // Upper circuit limit - position 347, 8 bytes
        response.upperCircuitLimit = convertPrice(Number(dataView.getBigUint64(347, true)), exchangeType);
        tickData.upperCircuitLimit = response.upperCircuitLimit;
        
        // Lower circuit limit - position 355, 8 bytes
        response.lowerCircuitLimit = convertPrice(Number(dataView.getBigUint64(355, true)), exchangeType);
        tickData.lowerCircuitLimit = response.lowerCircuitLimit;
        
        // 52 week high price - position 363, 8 bytes
        response.fiftyTwoWeekHighPrice = convertPrice(Number(dataView.getBigUint64(363, true)), exchangeType);
        tickData.fiftyTwoWeekHighPrice = response.fiftyTwoWeekHighPrice;
        
        // 52 week low price - position 371, 8 bytes
        response.fiftyTwoWeekLowPrice = convertPrice(Number(dataView.getBigUint64(371, true)), exchangeType);
        tickData.fiftyTwoWeekLowPrice = response.fiftyTwoWeekLowPrice;
        
        logger.debug(`Decoded Snap Quote data for ${symbol || token}`);
        
        // Process the tick data
        processTickData(token, symbol, tickData);
        
        return response;
      } catch (snap_quote_error) {
        logger.error(`Error extracting Snap Quote data: ${snap_quote_error.message}`);
        response.error = `Failed to extract Snap Quote data: ${snap_quote_error.message}`;
        
        // Process the partial tick data we have so far
        processTickData(token, symbol, tickData);
        
        return response;
      }
    }
    
    // Process the tick data for the mode we have
    processTickData(token, symbol, tickData);
    
    return response;
  } catch (error) {
    logger.error(`Error handling data tick: ${error.message}`);
    console.error('Error details:', error.stack);
    return {
      error: 'Failed to parse market data',
      errorDetails: error.message
    };
  }
}

/**
 * Process tick data and update related systems
 * @param {string} token - Security token
 * @param {string|undefined} symbol - Trading symbol
 * @param {Object} tickData - Tick data
 */
function processTickData(token, symbol, tickData) {
  try {
    // Store last tick data
    if (token) {
      lastTicks.set(token, tickData);
    }
    
    // Update symbol price in Redis and order plans
    if (symbol) {
      updateSymbolPrice(symbol, token, tickData);
      
      // Update order plans that use this symbol
      if (subscriptions.has(token)) {
        const planIds = subscriptions.get(token);
        for (const planId of planIds) {
          updateOrderPlanPrice(planId, tickData);
        }
      }
      
      // Log a summary of the price update
      const priceChange = tickData.close > 0 ? 
        ((tickData.ltp - tickData.close) / tickData.close * 100).toFixed(2) + '%' : 
        'N/A';
      
      logger.info(`Market data for ${symbol}: LTP=${tickData.ltp}, Change=${priceChange}`);
    }
  } catch (error) {
    logger.error(`Error processing tick data: ${error.message}`);
  }
}

/**
 * Extract best five buy/sell data from buffer
 * @param {ArrayBuffer} buffer - The binary data
 * @param {number} startOffset - Starting position in buffer
 * @param {number} exchangeType - Exchange type code
 * @returns {Object} Best five buy and sell data
 */
function extractBestFiveData(buffer, startPosition) {
  try {
    // Extract best five data (10 entries, 5 buy and 5 sell)
    const bestFive = {
      buy: [],
      sell: []
    };
    
    // The size of each entry is 20 bytes:
    // 2 (buy/sell flag) + 8 (quantity) + 8 (price) + 2 (number of orders)
    const entrySize = 20;
    
    // Dump raw bytes for the first buy and sell entries for analysis
    const firstBuyEntry = buffer.slice(startPosition, startPosition + entrySize);
    const firstSellEntry = buffer.slice(startPosition + (5 * entrySize), startPosition + (6 * entrySize));
    
    logger.debug(`First buy entry raw bytes: ${firstBuyEntry.toString('hex')}`);
    logger.debug(`First sell entry raw bytes: ${firstSellEntry.toString('hex')}`);
    
    // Process buy orders (first 5 entries)
    for (let i = 0; i < 5; i++) {
      const pos = startPosition + (i * entrySize);
      
      if (pos + entrySize > buffer.length) {
        logger.warn(`Buffer too short for order ${i+1}`);
        break;
      }
      
      // Read according to the specification
      const buyFlag = buffer.readInt16LE(pos);
      const quantity = Number(buffer.readBigInt64LE(pos + 2));
      const rawPrice = Number(buffer.readBigInt64LE(pos + 10));
      const price = rawPrice / 100; // Price is divided by 100 as per docs
      const orders = buffer.readInt16LE(pos + 18);
      
      // Skip entries with wrong buy/sell flag
      if (buyFlag !== 1 && buyFlag !== 0) {
        logger.warn(`Invalid buy/sell flag ${buyFlag} at position ${pos}`);
        continue;
      }
      
      // Add only buy orders to the buy array
      if (buyFlag === 1) {
        bestFive.buy.push({
          quantity,
          price: parseFloat(price.toFixed(2)),
          orders
        });
      } else if (buyFlag === 0) {
        bestFive.sell.push({
          quantity,
          price: parseFloat(price.toFixed(2)),
          orders
        });
      }
    }
    
    // Process next 5 entries (could be either buy or sell)
    for (let i = 5; i < 10; i++) {
      const pos = startPosition + (i * entrySize);
      
      if (pos + entrySize > buffer.length) {
        logger.warn(`Buffer too short for order ${i+1}`);
        break;
      }
      
      // Read according to the specification
      const buyFlag = buffer.readInt16LE(pos);
      const quantity = Number(buffer.readBigInt64LE(pos + 2));
      const rawPrice = Number(buffer.readBigInt64LE(pos + 10));
      const price = rawPrice / 100; // Price is divided by 100 as per docs
      const orders = buffer.readInt16LE(pos + 18);
      
      // Skip entries with wrong buy/sell flag
      if (buyFlag !== 1 && buyFlag !== 0) {
        logger.warn(`Invalid buy/sell flag ${buyFlag} at position ${pos}`);
        continue;
      }
      
      // Add to the appropriate array based on the flag
      if (buyFlag === 1) {
        bestFive.buy.push({
          quantity,
          price: parseFloat(price.toFixed(2)),
          orders
        });
      } else if (buyFlag === 0) {
        bestFive.sell.push({
          quantity,
          price: parseFloat(price.toFixed(2)),
          orders
        });
      }
    }
    
    // Sort buy orders by price in descending order
    bestFive.buy.sort((a, b) => b.price - a.price);
    
    // Sort sell orders by price in ascending order
    bestFive.sell.sort((a, b) => a.price - b.price);
    
    // Truncate to 5 entries if we have more
    if (bestFive.buy.length > 5) bestFive.buy = bestFive.buy.slice(0, 5);
    if (bestFive.sell.length > 5) bestFive.sell = bestFive.sell.slice(0, 5);
    
    // Log the prices for debugging
    logger.debug(`Buy prices (descending): ${bestFive.buy.map(b => b.price).join(', ')}`);
    logger.debug(`Sell prices (ascending): ${bestFive.sell.map(s => s.price).join(', ')}`);
    
    return bestFive;
  } catch (error) {
    logger.error(`Error extracting Best Five Data: ${error.message}`);
    logger.error(`Error stack: ${error.stack}`);
    
    // Return an empty structure on error to avoid crashing
    return {
      buy: [],
      sell: []
    };
  }
}

/**
 * Convert raw price to proper decimal value
 * @param {number} rawPrice - Raw price value
 * @param {number} exchangeType - Exchange type code
 * @returns {number} Converted price value
 */
function convertPrice(rawPrice, exchangeType) {
  try {
    // For currencies (CDE_FO - 13), divide by 10000000.0
    if (exchangeType === 13) {
      return rawPrice / 10000000.0;
    }
    
    // For everything else, divide by 100
    return rawPrice / 100.0;
  } catch (error) {
    logger.error(`Error converting price: ${error.message}`);
    return rawPrice; // Return raw price as fallback
  }
}


/**
 * Update symbol price in Redis
 * @param {string} symbol - Trading symbol
 * @param {string} token - Symbol token
 * @param {Object} tickData - Tick data
 */
async function updateSymbolPrice(symbol, token, tickData) {
  try {
    if (!symbol || !token) {
      logger.warn(`Cannot update price: Missing symbol or token`);
      return;
    }
    
    const priceKey = `price:${symbol}`;
    
    // Create a simplified price object with default values to prevent undefined
    const priceData = {
      symbol,
      token,
      lastPrice: tickData?.ltp || 0,
      open: tickData?.open || 0,
      high: tickData?.high || 0,
      low: tickData?.low || 0,
      close: tickData?.close || 0,
      totalBuyQty: tickData?.totBuyQty || tickData?.totalBuyQuantity || 0,
      totalSellQty: tickData?.totSellQty || tickData?.totalSellQuantity || 0,
      volume: tickData?.totalTradedQty || tickData?.volume || 0,
      lastUpdateTime: Date.now()
    };
    
    // Log the price data for debugging
    logger.debug(`Updating price for ${symbol}: ${JSON.stringify(priceData)}`);
    
    // Store in Redis (with try/catch)
    try {
      await redisClient.set(priceKey, JSON.stringify(priceData));
      logger.debug(`Successfully updated price for ${symbol} in Redis`);
    } catch (redisError) {
      logger.error(`Redis error updating price for ${symbol}: ${redisError.message}`);
      return; // Exit early if Redis store fails
    }
    
    // Publish update for any real-time subscribers (with try/catch)
    // Use redisPublisher instead of redisPubSub
    try {
      await redisPublisher.publish(`price:update:${symbol}`, JSON.stringify(priceData));
      logger.debug(`Successfully published price update for ${symbol}`);
    } catch (publishError) {
      logger.error(`Redis publish error for ${symbol}: ${publishError.message}`);
    }
    
  } catch (error) {
    logger.error(`Error updating price for ${symbol}: ${error.message}`);
    if (error.stack) {
      logger.debug(`Stack trace: ${error.stack}`);
    }
  }
}


/**
 * Update order plan price
 * @param {string} planId - Order plan ID
 * @param {Object} tickData - Tick data
 */
async function updateOrderPlanPrice(planId, tickData) {
  try {
    if (!planId) {
      logger.warn(`Cannot update order plan: Missing planId`);
      return;
    }
    
    if (!tickData || typeof tickData.ltp !== 'number') {
      logger.warn(`Cannot update order plan ${planId}: Invalid tick data`);
      return;
    }
    
    // Get the order plan
    let plan;
    try {
      plan = await orderPlanRedisService.getOrderPlan(planId);
    } catch (getPlanError) {
      logger.error(`Error getting order plan ${planId}: ${getPlanError.message}`);
      return;
    }
    
    if (!plan) {
      logger.warn(`Order plan ${planId} not found, removing from subscriptions`);
      // Plan doesn't exist anymore, remove from subscriptions
      try {
        await unsubscribeFromOrderPlan(planId);
      } catch (unsubError) {
        logger.error(`Error unsubscribing deleted plan ${planId}: ${unsubError.message}`);
      }
      return;
    }
    
    // Update last price
    const updates = {
      currentPrice: tickData.ltp || 0,
      lastUpdated: new Date().toISOString()
    };
    
    // Log updates for debugging
    logger.debug(`Updating order plan ${planId} with: ${JSON.stringify(updates)}`);
    
    // Update the plan in Redis
    try {
      await orderPlanRedisService.updateOrderPlan(planId, updates);
      logger.info(`Order plan ${planId} updated successfully`);
    } catch (updateError) {
      logger.error(`Error updating order plan ${planId} in Redis: ${updateError.message}`);
      return; // Exit early if update fails
    }
    
    // Publish an update event - use redisPublisher instead of redisPubSub
    try {
      await redisPublisher.publish(`orderplan:update:${planId}`, JSON.stringify({
        planId,
        updates
      }));
      logger.debug(`Successfully published update for order plan ${planId}`);
    } catch (publishError) {
      logger.error(`Error publishing update for plan ${planId}: ${publishError.message}`);
    }
    
  } catch (error) {
    logger.error(`Error updating order plan ${planId}: ${error.message}`);
    if (error.stack) {
      logger.debug(`Stack trace: ${error.stack}`);
    }
  }
}
/**
 * Request market data for all subscribed tokens
 */
function requestMarketData() {
  try {
    if (connectionState !== ConnectionState.READY || !ws || ws.readyState !== WebSocket.OPEN) {
      logger.warn(`Cannot request market data: WebSocket not ready (state: ${connectionState})`);
      return;
    }
    
    // Group tokens by exchange
    const tokensByExchange = new Map(); // exchangeType -> array of tokens
    
    // Process all subscriptions
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.size === 0) continue;
      
      // Get symbol for the token
      const symbol = tokenToSymbolMap.get(token);
      if (!symbol) {
        logger.warn(`Cannot find symbol for token ${token}`);
        continue;
      }
      
      // Get exchange type for this symbol
      const plan = findOrderPlanBySymbolSync(symbol);
      if (!plan) {
        logger.warn(`Cannot find order plan for symbol ${symbol}`);
        continue;
      }
      
      const exchangeType = mapExchangeToType(plan.exchange);
      
      // Add to tokens by exchange map
      if (!tokensByExchange.has(exchangeType)) {
        tokensByExchange.set(exchangeType, []);
      }
      
      tokensByExchange.get(exchangeType).push(parseInt(token, 10));
    }
    
    // If no tokens to request, return
    if (tokensByExchange.size === 0) {
      logger.debug('No tokens to request market data for');
      return;
    }
    
    // Build token list for request
    const tokenList = [];
    for (const [exchangeType, tokens] of tokensByExchange.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    // Create request message
    const requestMessage = {
      correlationID: "marketdata_" + Date.now(),
      action: 2, // MARKET_DATA_REQUEST
      params: {
        mode: priceMode, // FULL mode
        tokenList
      }
    };
    console.log(requestMessage);
    // Log request summary
    const tokenCount = tokenList.reduce((sum, item) => sum + item.tokens.length, 0);
    logger.info(`Requesting market data for ${tokenCount} tokens across ${tokenList.length} exchanges`);
    
    // Send request
    ws.send(JSON.stringify(requestMessage));
  } catch (error) {
    logger.error('Error requesting market data:', error);
  }
}

/**
 * Subscribe to an order plan on both WebSockets
 * @param {string} planId - Order plan ID
 */
export async function subscribeToOrderPlan(planId) {
  try {
    const plan = await orderPlanRedisService.getOrderPlan(planId);
    
    if (!plan || !plan.symboltoken || !plan.tradingsymbol) {
      logger.warn(`Cannot subscribe to plan ${planId}: Invalid plan data`);
      return;
    }
    
    const token = plan.symboltoken;
    const symbol = plan.tradingsymbol;
    const exchange = plan.exchange;
    
    // Add to maps
    tokenToSymbolMap.set(token, symbol);
    symbolToTokenMap.set(symbol, token);
    
    // Initialize subscription set if needed
    if (!subscriptions.has(token)) {
      subscriptions.set(token, new Set());
    }
    
    // Add plan to subscription set
    subscriptions.get(token).add(planId);
    
    // Subscribe to both WebSockets if they're ready
    if (ltpConnectionState === ConnectionState.READY && wsLTP && wsLTP.readyState === WebSocket.OPEN) {
      subscribeLTP(token, exchange);
    }
    
    if (depthConnectionState === ConnectionState.READY && wsMarketDepth && wsMarketDepth.readyState === WebSocket.OPEN) {
      subscribeMarketDepth(token, exchange);
    }
    
    logger.info(`Subscribed plan ${planId} to symbol ${symbol} on exchange ${exchange} (token ${token})`);
  } catch (error) {
    logger.error(`Error subscribing to order plan ${planId}:`, error);
  }
}

/**
 * Subscribe to LTP data for a token
 * @param {string} token - Token to subscribe to
 * @param {string} exchange - Exchange name
 */
function subscribeLTP(token, exchange) {
  try {
    const exchangeType = mapExchangeToType(exchange);
    
    // Create subscription message
    const subscriptionMessage = {
      correlationID: "ltp_subscription_" + Date.now(),
      action: 1, // SUBSCRIBE
      params: {
        mode: 1, // LTP mode
        tokenList: [
          {
            exchangeType,
            tokens: [parseInt(token, 10)]
          }
        ]
      }
    };
    
    // Send subscription
    if (wsLTP && wsLTP.readyState === WebSocket.OPEN) {
      wsLTP.send(JSON.stringify(subscriptionMessage));
      logger.info(`Subscribed to LTP data for token ${token} on exchange type ${exchangeType}`);
    } else {
      logger.error('LTP WebSocket not connected, cannot subscribe');
    }
  } catch (error) {
    logger.error(`Error subscribing to LTP data for token ${token}:`, error);
  }
}

/**
 * Subscribe to Market Depth data for a token
 * @param {string} token - Token to subscribe to
 * @param {string} exchange - Exchange name
 */
function subscribeMarketDepth(token, exchange) {
  try {
    const exchangeType = mapExchangeToType(exchange);
    
    // Create subscription message
    const subscriptionMessage = {
      correlationID: "marketdepth_subscription_" + Date.now(),
      action: 1, // SUBSCRIBE
      params: {
        mode: 3, // Snap Quote mode
        tokenList: [
          {
            exchangeType,
            tokens: [parseInt(token, 10)]
          }
        ]
      }
    };
    
    // Send subscription
    if (wsMarketDepth && wsMarketDepth.readyState === WebSocket.OPEN) {
      wsMarketDepth.send(JSON.stringify(subscriptionMessage));
      logger.info(`Subscribed to Market Depth data for token ${token} on exchange type ${exchangeType}`);
    } else {
      logger.error('Market Depth WebSocket not connected, cannot subscribe');
    }
  } catch (error) {
    logger.error(`Error subscribing to Market Depth data for token ${token}:`, error);
  }
}

/**
 * Unsubscribe from an order plan
 * @param {string} planId - Order plan ID
 */
export async function unsubscribeFromOrderPlan(planId) {
  try {
    // Find which tokens this plan is subscribed to
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.has(planId)) {
        // Remove plan from this token's subscriptions
        planIds.delete(planId);
        
        // If no more plans for this token, unsubscribe
        if (planIds.size === 0) {
          await unsubscribeFromToken(token);
          subscriptions.delete(token);
          
          const symbol = tokenToSymbolMap.get(token);
          if (symbol) {
            symbolToTokenMap.delete(symbol);
          }
          tokenToSymbolMap.delete(token);
        }
        
        logger.info(`Unsubscribed plan ${planId} from token ${token}`);
      }
    }
  } catch (error) {
    logger.error(`Error unsubscribing from order plan ${planId}:`, error);
  }
}

/**
 * Unsubscribe from a token
 * @param {string} token - Symbol token
 */
async function unsubscribeFromToken(token) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    logger.warn(`Cannot unsubscribe from token ${token}: WebSocket not connected`);
    return;
  }
  
  try {
    // Find symbol and exchange for the token
    const symbol = tokenToSymbolMap.get(token);
    if (!symbol) {
      logger.warn(`Cannot find symbol for token ${token}`);
      return;
    }
    
    // Get exchange type for this token
    const exchangeType = await getExchangeTypeForSymbol(symbol);
    
    // Create unsubscribe message
    const unsubscribeMessage = {
      correlationID: "unsubscribe_" + Date.now(),
      action: 0, // Unsubscribe
      params: {
        mode: priceMode,
        tokenList: [
          {
            exchangeType,
            tokens: [parseInt(token, 10)]
          }
        ]
      }
    };
    
    // Send unsubscription request
    ws.send(JSON.stringify(unsubscribeMessage));
    
    logger.info(`Unsubscribed from token ${token} on exchange type ${exchangeType}`);
  } catch (error) {
    logger.error(`Error unsubscribing from token ${token}:`, error);
  }
}

/**
 * Get exchange type for a symbol
 * @param {string} symbol - Trading symbol
 * @returns {Promise<number>} Exchange type code
 */
async function getExchangeTypeForSymbol(symbol) {
  try {
    // Get plan from Redis
    const plans = await orderPlanRedisService.getAllOrderPlans({ tradingsymbol: symbol }, 1, 0);
    
    if (plans.length > 0) {
      return mapExchangeToType(plans[0].exchange);
    }
    
    // Try to determine exchange from symbol name
    if (symbol.includes('NIFTY') || symbol.includes('BANKNIFTY')) {
      if (symbol.includes('FUT')) {
        return 2; // NSE_FO (NFO)
      } else if (symbol.includes('PE') || symbol.includes('CE')) {
        return 2; // NSE_FO (NFO)
      } else {
        return 1; // NSE_CM
      }
    } else if (symbol.includes('SENSEX') || symbol.includes('BSE')) {
      if (symbol.includes('FUT') || symbol.includes('PE') || symbol.includes('CE')) {
        return 4; // BSE_FO
      } else {
        return 3; // BSE_CM
      }
    } else if (symbol.includes('USDINR') || symbol.includes('EURINR') || 
               symbol.includes('GBPINR') || symbol.includes('JPYINR')) {
      return 13; // CDE_FO (Currency Derivatives)
    } else if (symbol.includes('GOLD') || symbol.includes('SILVER') || 
               symbol.includes('CRUDE') || symbol.includes('COPPER') || 
               symbol.includes('NATURAL')) {
      return 5; // MCX_FO
    } else if (symbol.includes('BARLEY') || symbol.includes('WHEAT') || 
               symbol.includes('COTTON') || symbol.includes('SOYBEAN')) {
      return 7; // NCX_FO (NCDEX)
    }
    
    // Detect based on symbol format patterns
    
    // Options pattern: e.g., NIFTY28AUG2524000PE
    if (/[A-Z]+\d{2}[A-Z]{3}\d{2}\d+(?:CE|PE)/.test(symbol)) {
      return 2; // NSE_FO
    }
    
    // Futures pattern: e.g., NIFTY28AUG25FUT
    if (/[A-Z]+\d{2}[A-Z]{3}\d{2}FUT/.test(symbol)) {
      return 2; // NSE_FO
    }
    
    // BSE stocks usually have a suffix
    if (symbol.endsWith('-BE') || symbol.endsWith('.BO')) {
      return 3; // BSE_CM
    }
    
    // Default to NSE_CM for regular stocks
    return 1;
  } catch (error) {
    logger.error(`Error getting exchange type for ${symbol}:`, error);
    return 1; // Default to NSE_CM
  }
}

/**
 * Find order plan by symbol from cache
 * @param {string} symbol - Trading symbol
 * @returns {Object|null} Order plan or null if not found
 */
function findOrderPlanBySymbolSync(symbol) {
  try {
    if (!symbol) return null;
    
    // Parse the symbol to detect exchange and token
    const exchange = detectExchangeFromSymbol(symbol);
    
    // Return minimal info
    return {
      tradingsymbol: symbol,
      exchange: exchange
    };
  } catch (error) {
    logger.error(`Error finding order plan for symbol ${symbol}:`, error);
    return null;
  }
}

/**
 * Detect exchange from symbol name
 * @param {string} symbol - Trading symbol
 * @returns {string} Exchange name
 */
function detectExchangeFromSymbol(symbol) {
  // Futures and options on NSE
  if ((symbol.includes('NIFTY') || symbol.includes('BANKNIFTY')) && 
      (symbol.includes('FUT') || symbol.includes('PE') || symbol.includes('CE'))) {
    return 'NFO';
  }
  
  // Other NSE futures and options
  if (/[A-Z]+\d{2}[A-Z]{3}\d{2}\d+(?:CE|PE)/.test(symbol) || 
      /[A-Z]+\d{2}[A-Z]{3}\d{2}FUT/.test(symbol)) {
    return 'NFO';
  }
  
  // BSE stocks and derivatives
  if (symbol.endsWith('-BE') || symbol.endsWith('.BO') || 
      symbol.includes('SENSEX')) {
    if (symbol.includes('FUT') || symbol.includes('PE') || symbol.includes('CE')) {
      return 'BSE_FO';
    } else {
      return 'BSE';
    }
  }
  
  // Currency symbols
  if (symbol.includes('USDINR') || symbol.includes('EURINR') || 
      symbol.includes('GBPINR') || symbol.includes('JPYINR') ||
      (symbol.length === 6 && /[A-Z]{3}[A-Z]{3}/.test(symbol))) {
    return 'CDS';
  }
  
  // Commodity symbols on MCX
  if (symbol.includes('MCX') || symbol.includes('GOLD') || symbol.includes('SILVER') || 
      symbol.includes('CRUDE') || symbol.includes('COPPER') || symbol.includes('NATURAL')) {
    return 'MCX';
  }
  
  // Commodity symbols on NCDEX
  if (symbol.includes('NCDEX') || symbol.includes('BARLEY') || symbol.includes('WHEAT') || 
      symbol.includes('COTTON') || symbol.includes('SOYBEAN')) {
    return 'NCDEX';
  }
  
  // Default to NSE for stocks
  return 'NSE';
}

/**
 * Map exchange name to exchange type code
 * @param {string} exchange - Exchange name
 * @returns {number} Exchange type code
 */
function mapExchangeToType(exchange) {
  try {
    if (!exchange) return 1; // Default to NSE_CM
    
    if (typeof exchange === 'number') {
      return exchange; // Already a type code
    }
    
    const exchangeStr = String(exchange).toUpperCase();
    
    switch (exchangeStr) {
      case 'NSE':
      case 'NSE_CM':
      case 'NSE_CAPITAL':
        return 1; // NSE Capital Market
        
      case 'NFO':
      case 'NSE_FO':
      case 'NSE_FUTURES':
      case 'NSE_OPTIONS':
        return 2; // NSE Futures & Options
        
      case 'BSE':
      case 'BSE_CM':
      case 'BSE_CAPITAL':
        return 3; // BSE Capital Market
        
      case 'BFO':
      case 'BSE_FO':
      case 'BSE_FUTURES':
      case 'BSE_OPTIONS':
        return 4; // BSE Futures & Options
        
      case 'MCX':
      case 'MCX_FO':
      case 'MCX_FUTURES':
        return 5; // MCX Futures & Options
        
      case 'NCDEX':
      case 'NCX_FO':
      case 'NCDEX_FUTURES':
        return 7; // NCDEX Futures
        
      case 'CDS':
      case 'NSE_CDS':
      case 'CURRENCY':
      case 'CDE_FO':
        return 13; // Currency Derivatives
        
      default:
        logger.warn(`Unknown exchange: ${exchange}, defaulting to NSE_CM (1)`);
        return 1; // Default to NSE Capital Market
    }
  } catch (error) {
    logger.error(`Error mapping exchange to type: ${error.message}`);
    return 1; // Default to NSE_CM in case of error
  }
}

/**
 * Resubscribe to all active subscriptions
 */
async function resubscribeAll() {
  try {
    logger.info('Resubscribing to all active subscriptions');
    
    // If no subscriptions, return
    if (subscriptions.size === 0) {
      logger.info('No subscriptions to resubscribe to');
      return;
    }
    
    // Add all subscriptions to pending list
    for (const [token, planIds] of subscriptions.entries()) {
      // Skip if no plan IDs
      if (planIds.size === 0) continue;
      
      // Get symbol for the token
      const symbol = tokenToSymbolMap.get(token);
      if (!symbol) {
        logger.warn(`Cannot find symbol for token ${token}`);
        continue;
      }
      
      // Get exchange for the symbol
      const exchange = detectExchangeFromSymbol(symbol);
      
      // Add to pending subscriptions
      pendingSubscriptions.set(token, {
        symbol,
        exchange,
        planId: planIds.values().next().value // Use first plan ID
      });
    }
    
    // Process pending subscriptions
    processPendingSubscriptions();
  } catch (error) {
    logger.error('Error resubscribing to all active subscriptions:', error);
  }
}

/**
 * Set up initial subscriptions
 */
async function setupInitialSubscriptions() {
  try {
    // Get all active order plans
    const plans = await orderPlanRedisService.getAllOrderPlans({}, 1000, 0);
    
    logger.info(`Setting up initial subscriptions for ${plans.length} order plans`);
    
    // Subscribe to each plan
    for (const plan of plans) {
      await subscribeToOrderPlan(plan.id);
    }
  } catch (error) {
    logger.error('Error setting up initial subscriptions:', error);
  }
}

/**
 * Refresh subscriptions periodically
 */
async function refreshSubscriptions() {
  try {
    logger.info('Refreshing subscriptions');
    
    // Re-authenticate
    const session = await authenticate();
    
   // Reconnect both WebSockets with new session
    await Promise.all([
      connectLTPWebSocket(session),
      connectMarketDepthWebSocket(session)
    ]);
    
    logger.info('WebSocket connections refreshed successfully');
  } catch (error) {
    logger.error('Error refreshing WebSocket subscriptions:', error);
  }
}

/**
 * Get last tick for a symbol
 * @param {string} symbol - Trading symbol
 * @returns {Promise<Object|null>} Last tick data or null
 */
export async function getLastTick(symbol) {
  try {
    const token = symbolToTokenMap.get(symbol);
    
    if (!token) {
      return null;
    }
    
    const tickData = lastTicks.get(token);
    
    if (!tickData) {
      return null;
    }
    
    return {
      symbol,
      token,
      lastPrice: tickData.ltp || 0,
      open: tickData.open || 0,
      high: tickData.high || 0,
      low: tickData.low || 0,
      close: tickData.close || 0,
      volume: tickData.totalTradedQty || 0,
      lastUpdateTime: Date.now()
    };
  } catch (error) {
    logger.error(`Error getting last tick for ${symbol}:`, error);
    return null;
  }
}


/**
 * Get exchange name from exchange type code
 * @param {number} exchangeType - Exchange type code
 * @returns {string} Exchange name
 */
function getExchangeName(exchangeType) {
  try {
    switch (exchangeType) {
      case 1: return 'NSE_CM';
      case 2: return 'NSE_FO';
      case 3: return 'BSE_CM';
      case 4: return 'BSE_FO';
      case 5: return 'MCX_FO';
      case 7: return 'NCX_FO';
      case 13: return 'CDE_FO';
      default: return `UNKNOWN(${exchangeType})`;
    }
  } catch (error) {
    logger.error(`Error getting exchange name: ${error.message}`);
    return `UNKNOWN(${exchangeType})`;
  }
}

/**
 * Get all current subscriptions
 * @returns {Object} Current subscription data
 */
export function getCurrentSubscriptions() {
  const result = {
    tokens: [],
    symbols: [],
    planCount: 0
  };
  
  for (const [token, planIds] of subscriptions.entries()) {
    const symbol = tokenToSymbolMap.get(token);
    
    result.tokens.push(token);
    if (symbol) {
      result.symbols.push(symbol);
    }
    
    result.planCount += planIds.size;
  }
  
  return result;
}

// Export orderPlanService with WebSocket integration
export const orderPlanService = {
  // Create order plan with WebSocket subscription
  async storeOrderPlan(orderPlan) {
    try {
      // Store in Redis
      const storedPlan = await orderPlanRedisService.storeOrderPlan(orderPlan);
      
      // Publish event for WebSocket subscription - use redisPublisher
      await redisPublisher.publish('orderplan:new', storedPlan.id);
      
      return storedPlan;
    } catch (error) {
      logger.error('Error storing order plan:', error);
      throw error;
    }
  },
  
  // Delete order plan with WebSocket unsubscription
  async deleteOrderPlan(planId) {
    try {
      // Delete from Redis
      const deleted = await orderPlanRedisService.deleteOrderPlan(planId);
      
      if (deleted) {
        // Publish event for WebSocket unsubscription - use redisPublisher
        await redisPublisher.publish('orderplan:delete', planId);
      }
      
      return deleted;
    } catch (error) {
      logger.error(`Error deleting order plan ${planId}:`, error);
      throw error;
    }
  },
  
  // Pass through other methods
  getOrderPlan: orderPlanRedisService.getOrderPlan,
  getAllOrderPlans: orderPlanRedisService.getAllOrderPlans,
  updateOrderPlan: orderPlanRedisService.updateOrderPlan,
  updateOrderPlanStatus: orderPlanRedisService.updateOrderPlanStatus,
  getOrderPlanCount: orderPlanRedisService.getOrderPlanCount
};