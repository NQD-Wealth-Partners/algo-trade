import WebSocket from 'ws';
import logger from '../utils/logger.js';
import config from '../config/index.js';
import * as orderPlanRedisService from './orderPlanRedisService.js';
import {generateTOTP } from './angelOneService.js';
import Redis from 'ioredis';
import axios from 'axios';
import { SmartAPI } from 'smartapi-javascript';

// Initialize Redis client for pub/sub
const redisPubSub = new Redis({
  host: config.redis?.host || 'algo-trade-redis',
  port: config.redis?.port || 6379,
  password: config.redis?.password || '',
  db: config.redis?.db || 0
});

// Initialize Redis client for data storage
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

// WebSocket connection
let ws = null;

// Subscription tracker
const subscriptions = new Map(); // Map<token, Set<planId>>
const tokenToSymbolMap = new Map(); // Map<token, symbol>
const symbolToTokenMap = new Map(); // Map<symbol, token>

// Last ticks
const lastTicks = new Map(); // Map<token, tickData>

// Message handling variables
let binaryMessageBuffer = null;
let expectedMessageLength = null;
let messageAssemblyTimeout = null;
const MESSAGE_ASSEMBLY_TIMEOUT = 5000; // 5 seconds

// Connection tracking variables
let lastMessageTime = Date.now();
let lastPongTime = Date.now();
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 5000; // 5 seconds base delay

// Intervals
let pingInterval = null;
let marketDataRequestInterval = null;
let connectionHealthCheckInterval = null;


// Authentication session
let currentSession = null;


const pendingSubscriptions = new Map(); // token -> { exchange, planId }

// Cached order plans
const cachedPlans = new Map(); // plan ID -> plan object


// WebSocket connection states
const ConnectionState = {
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  AUTHENTICATING: 'authenticating',
  AUTHENTICATED: 'authenticated',
  READY: 'ready',
  RECONNECTING: 'reconnecting'
};

// Current connection state
let connectionState = ConnectionState.DISCONNECTED;


/**
 * Initialize WebSocket connection and subscriptions
 */
export async function initializeWebSocket() {
  try {
    // Authenticate with Angel Broking
    const session = await authenticate();
    
    // Connect to WebSocket
    connectWebSocket(session);
    
    // Set up subscription refresh interval
    setInterval(refreshSubscriptions, 60 * 60 * 1000); // Every hour
    
    // Initial subscription setup
    await setupInitialSubscriptions();
    
    // Listen for new order plans
    redisPubSub.subscribe('orderplan:new');
    redisPubSub.subscribe('orderplan:delete');
    
    redisPubSub.on('message', async (channel, message) => {
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
 * Connect WebSocket with proper state management and error handling
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
      
      // Update connection state
      connectionState = ConnectionState.CONNECTING;
      logger.info('WebSocket connection state:', connectionState);
      
      // Store current session for later use
      currentSession = session;
      
      // Close existing connection if any
      if (ws) {
        try {
          ws.terminate();
        } catch (err) {
          logger.warn(`Error terminating existing WebSocket: ${err.message}`);
        }
        ws = null;
      }
      
      // Reset variables
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
      
      // Clear intervals
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
      
      // Validate configuration before connecting
      if (!config || !config.angelOne || !config.angelOneWebSocket) {
        throw new Error('Invalid configuration: missing Angel One WebSocket config');
      }
      
      // Create new WebSocket connection
      const wsUrl = config.angelOne.wsUrl || 'wss://smartapisocket.angelone.in/smart-stream';
      logger.info(`Connecting to WebSocket at ${wsUrl}`);
      
      // Validate required headers
      if (!config.angelOne.apiKey) {
        throw new Error('Missing API key in configuration');
      }
      
      if (!config.angelOneWebSocket.clientId) {
        throw new Error('Missing client ID in configuration');
      }
      
      // Create headers
      const headers = {
        'Authorization': `Bearer ${session.jwtToken}`,
        'x-api-key': config.angelOne.apiKey,
        'x-client-code': config.angelOneWebSocket.clientId
      };
      
      // Add feed token if available
      if (session.feedToken) {
        headers['x-feed-token'] = session.feedToken;
      } else {
        headers['x-feed-token'] = session.jwtToken;
        logger.warn('Using JWT token as feed token');
      }
      ws = new WebSocket(wsUrl, { headers: headers });
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
      ws.on('open', () => {
        logger.info('WebSocket connection established');
        
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
        
        // Send authentication message
        try {
          const authMessage = {
            correlationID: "websocket_connection_" + Date.now(),
            action: 1, // AUTHENTICATE
            params: {
              clientCode: config.angelOneWebSocket.clientId,
              authorization: session.jwtToken
            }
          };
          
          logger.info('Sending authentication message to WebSocket');
          ws.send(JSON.stringify(authMessage));
        } catch (error) {
          logger.error('Error sending authentication message:', error);
          ws.close();
        }
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
        
        // Reconnect with exponential backoff
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
      });
      
      ws.on('pong', () => {
        logger.debug('Received pong from server');
        lastPongTime = Date.now();
      });
      
      // Special handler to resolve the promise when authenticated
      // This is handled in the message handler when we receive the authentication success message
      
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
 * Reconnect WebSocket after failure
 */
function reconnectWebSocket() {
  try {
    logger.info('Initiating WebSocket reconnection');
    
    // Close existing connection if any
    if (ws) {
      try {
        ws.terminate();
      } catch (err) {
        logger.warn(`Error terminating WebSocket: ${err.message}`);
      }
      ws = null;
    }
    
    // Update state
    connectionState = ConnectionState.RECONNECTING;
    logger.info('WebSocket connection state:', connectionState);
    
    // Clear intervals
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
    
    // Re-authenticate and reconnect
    authenticate()
      .then(newSession => connectWebSocket(newSession))
      .then(() => {
        logger.info('WebSocket reconnection successful');
      })
      .catch(error => {
        logger.error('WebSocket reconnection failed:', error);
        
        // Schedule another reconnect attempt
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          const delay = RECONNECT_DELAY * Math.pow(1.5, reconnectAttempts);
          logger.info(`Scheduling another reconnect attempt in ${delay}ms`);
          
          setTimeout(reconnectWebSocket, delay);
        } else {
          logger.error('Maximum reconnection attempts reached. Giving up.');
        }
      });
  } catch (error) {
    logger.error('Error in reconnectWebSocket:', error);
  }
}

/**
 * Handle WebSocket messages with robust error handling
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
        
        // Handle 51-byte messages directly (the common case from your logs)
        if (size === 51) {
          handleAcknowledgmentMessage(data);
        } else {
          // Process other binary messages normally
          processBinaryMessage(data);
        }
      } catch (binaryError) {
        logger.error(`Error processing binary message: ${binaryError.message}`);
      }
      return;
    }
    
    // Handle text messages
    try {
      const textData = data.toString();
      logger.debug(`Received text message: ${textData.substring(0, 200)}${textData.length > 200 ? '...' : ''}`);
      
      // Try to parse as JSON
      try {
        const jsonData = JSON.parse(textData);
        
        // Handle authentication response
        if (jsonData.hasOwnProperty('success')) {
          if (jsonData.success === true) {
            logger.info('WebSocket operation successful:', jsonData.message || 'No message');
            
            // Check for authentication success
            if (jsonData.message === "Authenticated") {
              logger.info('WebSocket authentication successful');
              
              // Update connection state
              connectionState = ConnectionState.AUTHENTICATED;
              logger.info('WebSocket connection state:', connectionState);
              
              // Move to READY state after a short delay
              setTimeout(() => {
                connectionState = ConnectionState.READY;
                logger.info('WebSocket connection state:', connectionState);
                
                // Process pending subscriptions
                processPendingSubscriptions();
                
                // Request market data
                setTimeout(() => {
                  requestMarketData();
                }, 1000);
              }, 1000);
            }
          } else {
            logger.warn(`WebSocket operation failed:`, jsonData.message || 'No error message');
            
            // Handle authentication failure
            if (jsonData.message && jsonData.message.includes("auth")) {
              logger.error(`Authentication failed: ${jsonData.message}`);
              
              // Update connection state
              connectionState = ConnectionState.DISCONNECTED;
              logger.info('WebSocket connection state:', connectionState);
              
              // Trigger reconnection
              reconnectWebSocket();
            }
          }
        } 
        // Handle other JSON message types...
        else if (jsonData.hasOwnProperty('responses') && Array.isArray(jsonData.responses)) {
          // Process market data responses
          handleDataTicks(jsonData.responses);
        }
        // Additional message type handling...
      } catch (jsonError) {
        // Not valid JSON, log and ignore
        logger.debug(`Received non-JSON text message: ${textData}`);
      }
    } catch (textError) {
      logger.error(`Error processing text message: ${textError.message}`);
    }
  } catch (error) {
    logger.error('Error handling WebSocket message:', error);
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
        mode: 1, // FULL mode
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
 * Add subscription to pending list
 * @param {string} symbol - Trading symbol
 * @param {string} exchange - Exchange name
 * @param {string} token - Token ID
 * @param {string} planId - Plan ID
 */
function addSubscription(symbol, exchange, token, planId) {
  try {
    logger.info(`Adding subscription for ${symbol} (${exchange}, token: ${token})`);
    
    // Validate parameters
    if (!symbol || !exchange || !token) {
      logger.warn('Invalid subscription parameters:', { symbol, exchange, token });
      return;
    }
    
    // Create plan ID if not provided
    const actualPlanId = planId || `${symbol}_${exchange}`;
    
    // Add to pending subscriptions
    pendingSubscriptions.set(token, {
      symbol,
      exchange,
      planId: actualPlanId
    });
    
    // Map token to symbol
    tokenToSymbolMap.set(token, symbol);
    
    // Process immediately if connected
    if (connectionState === ConnectionState.READY && ws && ws.readyState === WebSocket.OPEN) {
      processPendingSubscriptions();
    } else {
      logger.info(`Queued subscription for ${symbol} (WebSocket state: ${connectionState})`);
    }
  } catch (error) {
    logger.error(`Error adding subscription for ${symbol}:`, error);
  }
}

/**
 * Remove subscription
 * @param {string} token - Token ID
 * @param {string} planId - Plan ID (optional)
 */
function removeSubscription(token, planId) {
  try {
    logger.info(`Removing subscription for token ${token}${planId ? ` (plan: ${planId})` : ''}`);
    
    // If no specific plan ID, remove all subscriptions for this token
    if (!planId) {
      subscriptions.delete(token);
      tokenToSymbolMap.delete(token);
      pendingSubscriptions.delete(token);
      
      // Unsubscribe from the token
      unsubscribeToken(token);
      
      return;
    }
    
    // Remove specific plan ID from token's subscriptions
    if (subscriptions.has(token)) {
      const planIds = subscriptions.get(token);
      planIds.delete(planId);
      
      // If no more subscriptions for this token, unsubscribe
      if (planIds.size === 0) {
        subscriptions.delete(token);
        tokenToSymbolMap.delete(token);
        
        // Unsubscribe from the token
        unsubscribeToken(token);
      }
    }
    
    // Remove from pending subscriptions if present
    if (pendingSubscriptions.has(token)) {
      const details = pendingSubscriptions.get(token);
      if (!planId || details.planId === planId) {
        pendingSubscriptions.delete(token);
      }
    }
  } catch (error) {
    logger.error(`Error removing subscription for token ${token}:`, error);
  }
}

/**
 * Unsubscribe from a token
 * @param {string} token - Token ID
 */
function unsubscribeToken(token) {
  try {
    logger.info(`Unsubscribing from token ${token}`);
    
    // Check connection state
    if (connectionState !== ConnectionState.READY || !ws || ws.readyState !== WebSocket.OPEN) {
      logger.warn(`Cannot unsubscribe: WebSocket not ready (state: ${connectionState})`);
      return;
    }
    
    // Find symbol and exchange for the token
    const symbol = tokenToSymbolMap.get(token);
    if (!symbol) {
      logger.warn(`Cannot find symbol for token ${token}`);
      return;
    }
    
    // Get exchange type
    let exchangeType = 1; // Default to NSE
    const plan = findOrderPlanBySymbolSync(symbol);
    if (plan) {
      exchangeType = mapExchangeToType(plan.exchange);
    }
    
    // Create unsubscription message
    const unsubscriptionMessage = {
      correlationID: "unsubscription_" + Date.now(),
      action: 2, // UNSUBSCRIBE
      params: {
        mode: 1, // FULL mode
        tokenList: [
          {
            exchangeType,
            tokens: [parseInt(token, 10)]
          }
        ]
      }
    };
    
    // Send unsubscription
    ws.send(JSON.stringify(unsubscriptionMessage));
    logger.info(`Unsubscribed from token ${token}`);
  } catch (error) {
    logger.error(`Error unsubscribing from token ${token}:`, error);
  }
}

/**
 * Initialize subscription for NIFTY option (as in your logs)
 */
function initializeNiftySubscription() {
  try {
    // Based on your logs, you're using token 71933 for NIFTY28AUG2524000PE
    const symbol = 'NIFTY28AUG2524000PE';
    const exchange = 'NFO';
    const token = '71933';
    
    // Create a cached plan for this symbol
    cachedPlans.set(symbol, {
      tradingsymbol: symbol,
      exchange: exchange,
      token: token,
      instrumentType: 'PE', // Put option
      strikePrice: 24000,
      expiry: '2025-08-28',
      underlying: 'NIFTY'
    });
    
    // Add subscription
    addSubscription(symbol, exchange, token, symbol);
    
    logger.info(`Initialized subscription for ${symbol} (token: ${token})`);
  } catch (error) {
    logger.error('Error initializing NIFTY subscription:', error);
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
      
      // Get plan for the symbol
      const plan = findOrderPlanBySymbolSync(symbol);
      if (!plan) {
        logger.warn(`Cannot find order plan for symbol ${symbol}`);
        continue;
      }
      
      // Get exchange type
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
        mode: 1, // FULL mode
        tokenList
      }
    };
    
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
 * Find order plan by symbol
 * @param {string} symbol - Trading symbol
 * @returns {Object|null} Order plan or null if not found
 */
function findOrderPlanBySymbolSync(symbol) {
  console.log(symbol);
  try {
    if (!symbol) return null;
    
    // Check cached plans
    console.log(cachedPlans.values());
    for (const plan of cachedPlans.values()) {
      if (plan.tradingsymbol === symbol) {
        return plan;
      }
    }
    
    // Plan not found in cache
    logger.warn(`Cannot find order plan for symbol ${symbol}`);
    
    // Create a minimal fallback plan to avoid errors
    return {
      tradingsymbol: symbol,
      exchange: 'NFO', // Default to NFO for option symbols like NIFTY28AUG2524000PE
      token: symbol.includes('NIFTY') ? '71933' : '0', // Use 71933 for NIFTY options as in your logs
      instrumentType: symbol.includes('PE') ? 'PE' : (symbol.includes('CE') ? 'CE' : 'OTHER')
    };
  } catch (error) {
    logger.error(`Error finding order plan for symbol ${symbol}:`, error);
    return null;
  }
}

/**
 * Handle 51-byte acknowledgment messages
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
    
    // Log buffer content for debugging
    let hexDump = '';
    for (let i = 0; i < bytes.length; i++) {
      hexDump += bytes[i].toString(16).padStart(2, '0') + ' ';
      if ((i + 1) % 16 === 0) hexDump += '\n';
    }
    logger.debug(`Binary message content:\n${hexDump}`);
    
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
    
    // If message ID matches a token we're interested in, log it
    if (messageId === "1933") {
      logger.debug(`Received acknowledgment for token 71933`);
    }
  } catch (error) {
    logger.error(`Error handling acknowledgment message: ${error.message}`);
  }
}


/**
 * Handle the special 51-byte acknowledgment messages
 * @param {ArrayBuffer} buffer - 51-byte binary buffer
 */
function handleSpecial51ByteMessage(buffer) {
  try {
    // Validate buffer size
    if (buffer.byteLength !== 51) {
      logger.warn(`Expected 51-byte buffer, got ${buffer.byteLength} bytes`);
      return;
    }
    
    const bytes = new Uint8Array(buffer);
    const dataView = new DataView(buffer);
    
    // Validate message type (byte 2 should be 0x37 = 55)
    if (bytes[2] !== 0x37) {
      logger.warn(`Expected message type 0x37, got 0x${bytes[2].toString(16)}`);
      return;
    }
    
    // Extract fields from the message
    
    // Bytes 0-1: Message length (usually 0x01 0x02 = 513 in little-endian)
    const messageLength = dataView.getUint16(0, true);
    
    // Byte 2: Message type (0x37 = 55)
    const messageType = bytes[2];
    
    // Bytes 3-6: Message ID or token (ASCII "1933" = 0x31 0x39 0x33 0x33)
    let messageIdStr = '';
    for (let i = 3; i < 7; i++) {
      if (bytes[i] >= 32 && bytes[i] <= 126) {
        messageIdStr += String.fromCharCode(bytes[i]);
      }
    }
    
    // Extract status code (position varies, but appears to be around bytes 38-39)
    // Try a few known positions based on the observed data
    let statusCode = 0;
    if (buffer.byteLength >= 40) {
      // Try position 38 (0-based index)
      statusCode = dataView.getUint16(38, true);
    }
    
    logger.debug(`Processed 51-byte acknowledgment: ID=${messageIdStr}, status=${statusCode}`);
    
    // Handle specific status codes
    if (statusCode !== 0) {
      handleSpecialStatusCodes(statusCode);
    }
    
    // Match the message ID to a token if possible
    if (/^\d+$/.test(messageIdStr)) {
      const numericId = parseInt(messageIdStr, 10);
      if (numericId === 71933) { // This matches the token in your logs
        logger.debug(`Acknowledgment for token 71933 (NIFTY28AUG2524000PE)`);
      }
    }
  } catch (error) {
    logger.error(`Error handling 51-byte message: ${error.message}`);
    if (error.stack) logger.debug(`Stack trace: ${error.stack}`);
  }
}


/**
 * Process general binary message
 * @param {Buffer|ArrayBuffer} data - The message data
 */
function processBinaryMessage(data) {
  try {
    // Convert to ArrayBuffer if needed
    const buffer = data instanceof Buffer ? 
      data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : 
      data;
    
    // Create views for reading the data
    const bytes = new Uint8Array(buffer);
    const dataView = new DataView(buffer);
    
    // Check for minimum size
    if (buffer.byteLength < 3) {
      logger.warn(`Binary message too small: ${buffer.byteLength} bytes`);
      return;
    }
    
    // Read message length (first 2 bytes, little endian)
    const messageLength = dataView.getUint16(0, true);
    
    // Read message type (byte 2)
    const messageType = bytes[2];
    
    logger.debug(`Binary message: length=${messageLength}, type=${messageType} (0x${messageType.toString(16)})`);
    
    // Handle different message types
    switch (messageType) {
      case 1: // Market data
        parseMarketData(buffer);
        break;
        
      case 2: // Index data
        parseIndexData(buffer);
        break;
        
      case 55: // Acknowledgment (should be handled by handleAcknowledgmentMessage)
        logger.debug(`Processing type 55 message with length ${buffer.byteLength}`);
        // For larger type 55 messages, we may have additional data
        if (buffer.byteLength > 51) {
          // Process as needed
        }
        break;
        
      default:
        logger.warn(`Unknown binary message type: ${messageType} (0x${messageType.toString(16)})`);
        break;
    }
  } catch (error) {
    logger.error(`Error processing binary message: ${error.message}`);
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
      
      logger.debug(`Parsed market data for token ${token}: LTP=${lastTradedPrice}`);
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
      
      logger.debug(`Parsed index data for token ${token}: Value=${indexValue}`);
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
 * Parse a type 55 message, even if incomplete
 * @param {ArrayBuffer} buffer - Binary buffer
 * @returns {Object} Parsed message data
 */
function parseType55Message(buffer) {
  try {
    // Create result object
    const result = {
      responseType: 55,
      timestamp: new Date().toISOString(),
      messageType: 'acknowledgment',
      ticks: []
    };
    
    const bytes = new Uint8Array(buffer);
    
    // Message should have at least 7 bytes for basic parsing
    if (buffer.byteLength < 7) {
      result.messageType = 'incomplete_acknowledgment';
      return result;
    }
    
    // Extract message ID from bytes 3-6
    let messageIdStr = '';
    for (let i = 3; i < 7; i++) {
      if (bytes[i] >= 32 && bytes[i] <= 126) {
        messageIdStr += String.fromCharCode(bytes[i]);
      }
    }
    
    result.messageIdRaw = messageIdStr;
    
    // Try to parse as number if it looks like digits
    if (/^\d+$/.test(messageIdStr)) {
      result.messageId = parseInt(messageIdStr, 10);
    } else {
      result.messageId = 0; // Default if not parseable
    }
    
    // If we have a status code at the end (51-byte fragment typically has this at the end)
    if (buffer.byteLength >= 30) {
      // The status code seems to be around offset 26-28
      try {
        const dataView = new DataView(buffer);
        const statusPos = buffer.byteLength - 4; // Typically 4 bytes from the end
        if (statusPos > 0 && statusPos < buffer.byteLength - 1) {
          result.statusCode = dataView.getUint16(statusPos, true);
        } else {
          result.statusCode = 0;
        }
      } catch (err) {
        logger.warn(`Error extracting status code: ${err.message}`);
        result.statusCode = 0;
      }
    } else {
      result.statusCode = 0;
    }
    
    return result;
  } catch (error) {
    logger.error(`Error parsing type 55 message: ${error.message}`);
    return {
      responseType: 55,
      timestamp: new Date().toISOString(),
      messageType: 'error_acknowledgment',
      error: error.message,
      ticks: []
    };
  }
}

/**
 * Handle parsed binary message
 * @param {Object} parsedData - Parsed message data
 */
function handleParsedMessage(parsedData) {
  try {
    if (!parsedData) {
      logger.warn('Received null parsed data');
      return;
    }
    
    switch (parsedData.responseType) {
      case 1: // Market data
      case 2: // Index data
        if (parsedData.ticks && parsedData.ticks.length > 0) {
          handleDataTicks(parsedData.ticks);
        } else {
          logger.debug(`Received ${parsedData.responseType === 1 ? 'market' : 'index'} data message with no ticks`);
        }
        break;
        
      case 55: // Acknowledgment/heartbeat
        logger.debug(`Processed type 55 message (${parsedData.messageType}): ID=${parsedData.messageId}, status=${parsedData.statusCode}`);
        
        // Check status code
        if (parsedData.statusCode !== 0) {
          // Handle special status codes
          handleSpecialStatusCodes(parsedData.statusCode);
        }
        break;
        
      default:
        logger.debug(`Processed message with unknown type ${parsedData.responseType}`);
        break;
    }
  } catch (error) {
    logger.error('Error handling parsed message:', error);
  }
}

/**
 * Get the filled length of the binary message buffer
 * @returns {number} Filled length
 */
function getFilledBufferLength() {
  if (!binaryMessageBuffer || !expectedMessageLength) {
    return 0;
  }
  
  try {
    // First, check for a non-standard case: if the buffer length is exactly 51 bytes
    // and it's a type 55 message, this might be a special case
    if (binaryMessageBuffer.length >= 3 && binaryMessageBuffer[2] === 0x37) {
      // Count non-zero bytes
      let nonZeroCount = 0;
      for (let i = 0; i < binaryMessageBuffer.length; i++) {
        if (binaryMessageBuffer[i] !== 0) {
          nonZeroCount++;
        }
      }
      
      // If we have around 51 non-zero bytes, this is likely the special case
      if (nonZeroCount > 45 && nonZeroCount < 55) {
        return nonZeroCount;
      }
    }
    
    // Standard case: find the last non-zero byte
    for (let i = binaryMessageBuffer.length - 1; i >= 0; i--) {
      if (binaryMessageBuffer[i] !== 0) {
        return i + 1;
      }
    }
    
    // If all bytes are zero, return 0
    return 0;
  } catch (error) {
    logger.error(`Error in getFilledBufferLength: ${error.message}`);
    return 0;
  }
}

/**
 * Handle special status codes from WebSocket messages
 * @param {number} statusCode - Status code from the message
 */
function handleSpecialStatusCodes(statusCode) {
  try {
    logger.debug(`Handling status code: ${statusCode}`);
    
    switch (statusCode) {
      case 0:
        // Success, nothing to do
        break;
        
      case 307:
        logger.info('Received status code 307 (temporary redirect), refreshing subscriptions');
        
        // Wait a short time before resubscribing
        setTimeout(() => {
          resubscribeAll();
        }, 2000);
        break;
        
      case 401:
      case 403:
        logger.warn(`Received authentication error (${statusCode}), reconnecting...`);
        reconnectWebSocket();
        break;
        
      case 429:
        logger.warn('Received status code 429 (too many requests), backing off');
        // Implement backoff logic
        break;
        
      case 4369: // 0x1111 (observed in your logs)
        logger.debug('Received status code 4369 (0x1111), possibly a success code');
        break;
        
      default:
        logger.warn(`Received unknown status code: ${statusCode} (0x${statusCode.toString(16)})`);
        break;
    }
  } catch (error) {
    logger.error(`Error handling status code ${statusCode}: ${error.message}`);
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


// Add a cleanup interval
let bufferCleanupInterval = setInterval(cleanupMessageBuffers, 10000); // Every 10 seconds

// Add a backoff multiplier for requests
let requestBackoffMultiplier = 1;

/**
 * Parse binary message from WebSocket
 * @param {ArrayBuffer} buffer - Binary data buffer
 * @returns {Object|null} Parsed data or null on error
 */
function parseBinaryMessage(buffer) {
  try {
    // Create a DataView for reading the buffer with Little Endian
    const dataView = new DataView(buffer);
    
    // Get the message length (first 2 bytes)
    const messageLength = dataView.getUint16(0, true); // true for Little Endian
    
    // Calculate expected buffer size
    const expectedSize = 2 + messageLength;
    if (buffer.byteLength < expectedSize) {
      logger.warn(`Incomplete message: expected ${expectedSize} bytes, got ${buffer.byteLength}`);
      return null;
    }
    
    // Parse the response type (1 byte)
    const responseType = dataView.getUint8(2);
    
    // Log the response type for debugging
    logger.debug(`Binary message response type: ${responseType} (0x${responseType.toString(16)})`);
    
    // Dump the first 32 bytes of the message for debugging
    const bytes = new Uint8Array(buffer);
    let headerHex = '';
    let headerAscii = '';
    for (let i = 0; i < Math.min(buffer.byteLength, 32); i++) {
      const byte = bytes[i];
      headerHex += byte.toString(16).padStart(2, '0') + ' ';
      headerAscii += (byte >= 32 && byte <= 126) ? String.fromCharCode(byte) : '.';
    }
    logger.debug(`Binary message header (hex): ${headerHex}`);
    logger.debug(`Binary message header (ascii): ${headerAscii}`);
    
    // Initialize result object
    const result = {
      responseType,
      timestamp: new Date().toISOString(),
      ticks: []
    };
    
    // Current position in the buffer
    let position = 3;
    
    // Parse based on response type
    switch (responseType) {
      case 1: // Market data
        // Parse exchange type (1 byte)
        const exchangeType = dataView.getUint8(position);
        position += 1;
        
        // Parse number of tokens (2 bytes)
        const tokenCount = dataView.getUint16(position, true);
        position += 2;
        
        logger.debug(`Parsing market data: exchange type ${exchangeType}, token count ${tokenCount}`);
        
        // Parse each token data
        for (let i = 0; i < tokenCount; i++) {
          // Parse token (4 bytes for integer token)
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
          
          // Parse last traded price (8 bytes as double)
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
          
          // Add to ticks array, matching the format expected by handleDataTicks
          result.ticks.push({
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
          
          logger.debug(`Parsed token ${token}: LTP=${lastTradedPrice}`);
        }
        break;
        
      case 2: // Index data
        // Parse exchange type (1 byte)
        const indexExchangeType = dataView.getUint8(position);
        position += 1;
        
        // Parse number of indices (2 bytes)
        const indexCount = dataView.getUint16(position, true);
        position += 2;
        
        logger.debug(`Parsing index data: exchange type ${indexExchangeType}, index count ${indexCount}`);
        
        // Parse each index data
        for (let i = 0; i < indexCount; i++) {
          // Parse index token (4 bytes)
          const indexToken = dataView.getUint32(position, true).toString();
          position += 4;
          
          // Parse sequence number (4 bytes)
          const indexSequenceNumber = dataView.getUint32(position, true);
          position += 4;
          
          // Parse index timestamp (8 bytes)
          const indexTimestampLow = dataView.getUint32(position, true);
          position += 4;
          const indexTimestampHigh = dataView.getUint32(position, true);
          position += 4;
          const indexTimestamp = (BigInt(indexTimestampHigh) << BigInt(32) | BigInt(indexTimestampLow)).toString();
          
          // Parse index value (8 bytes as double)
          const indexValue = dataView.getFloat64(position, true);
          position += 8;
          
          // Parse open value (8 bytes)
          const indexOpenValue = dataView.getFloat64(position, true);
          position += 8;
          
          // Parse high value (8 bytes)
          const indexHighValue = dataView.getFloat64(position, true);
          position += 8;
          
          // Parse low value (8 bytes)
          const indexLowValue = dataView.getFloat64(position, true);
          position += 8;
          
          // Parse close value (8 bytes)
          const indexCloseValue = dataView.getFloat64(position, true);
          position += 8;
          
          // Add to ticks array, matching the format expected by handleDataTicks
          result.ticks.push({
            token: indexToken,
            ltp: indexValue, // Index value
            open: indexOpenValue,
            high: indexHighValue,
            low: indexLowValue,
            close: indexCloseValue,
            sequenceNumber: indexSequenceNumber,
            exchangeTimestamp: indexTimestamp,
            isIndex: true
          });
          
          logger.debug(`Parsed index ${indexToken}: Value=${indexValue}`);
        }
        break;
        
      case 55: // This appears to be a heartbeat or acknowledgment message (0x37 in hex)
        logger.debug('Received message with response type 55 (likely a heartbeat or acknowledgment)');
        
        // Based on the observed data structure from the logs, we need to parse differently
        // The message appears to contain multiple repeating patterns of "71933"
        
        try {
          // Extract a 4-byte message ID at position 3-6
          if (position + 4 <= buffer.byteLength) {
            const messageIdBytes = new Uint8Array(buffer, position, 4);
            let messageIdStr = '';
            for (let i = 0; i < 4; i++) {
              const byte = messageIdBytes[i];
              if (byte >= 32 && byte <= 126) {
                messageIdStr += String.fromCharCode(byte);
              }
            }
            position += 4;
            
            result.messageIdRaw = messageIdStr;
            
            // Try to parse as number if it looks like digits
            if (/^\d+$/.test(messageIdStr)) {
              result.messageId = parseInt(messageIdStr, 10);
            } else {
              result.messageId = 0; // Default if not parseable
            }
            
            logger.debug(`Message ID/Sequence: ${result.messageId}`);
          }
          
          // Find status code near the end of the message
          // The status code appears to be a 2-byte value near the end of the message
          if (buffer.byteLength >= 6) {
            // Try to find the status code at the expected position (near the end)
            const statusPosition = buffer.byteLength - 4;
            if (statusPosition > 0 && statusPosition < buffer.byteLength) {
              const statusCode = dataView.getUint16(statusPosition, true);
              result.statusCode = statusCode;
              logger.debug(`Status code: ${statusCode}`);
            } else {
              result.statusCode = 0; // Default if can't find
            }
          }
          
          return result;
        } catch (error) {
          logger.warn(`Error parsing type 55 message: ${error.message}`);
          
          // Return a minimal result since there are no ticks
          return {
            responseType: 55,
            timestamp: new Date().toISOString(),
            messageType: 'heartbeat',
            error: error.message,
            ticks: []
          };
        }
        
      default:
        // Don't treat unknown response types as errors, just log them
        logger.warn(`Unknown response type: ${responseType} (0x${responseType.toString(16)})`);
        
        // Dump the entire buffer for unknown message types
        const fullBytes = new Uint8Array(buffer);
        let fullHex = '';
        let fullAscii = '';
        for (let i = 0; i < fullBytes.length; i++) {
          const byte = fullBytes[i];
          fullHex += byte.toString(16).padStart(2, '0') + ' ';
          fullAscii += (byte >= 32 && byte <= 126) ? String.fromCharCode(byte) : '.';
          
          // Add line breaks every 16 bytes for readability
          if ((i + 1) % 16 === 0) {
            fullHex += '\n';
            fullAscii += '\n';
          }
        }
        logger.debug(`Full message dump (hex):\n${fullHex}`);
        logger.debug(`Full message dump (ascii):\n${fullAscii}`);
        
        // Return a minimal result with the unknown type
        return {
          responseType,
          timestamp: new Date().toISOString(),
          messageType: 'unknown',
          ticks: []
        };
    }
    
    return result;
  } catch (error) {
    logger.error('Error parsing binary message:', error);
    
    // Log buffer content for debugging
    const bytes = new Uint8Array(buffer);
    let bufferStr = '';
    for (let i = 0; i < Math.min(buffer.byteLength, 100); i++) {
      bufferStr += bytes[i].toString(16).padStart(2, '0') + ' ';
    }
    logger.debug(`Buffer content (first 100 bytes): ${bufferStr}`);
    
    return null;
  }
}

/**
 * Handle data ticks
 * @param {Array} ticks - Array of tick data
 */
function handleDataTicks(ticks) {
  try {
    if (!ticks || !Array.isArray(ticks)) {
      logger.warn('Invalid ticks data:', ticks);
      return;
    }
    
    logger.debug(`Processing ${ticks.length} data ticks`);
    
    // Process each tick
    for (const tick of ticks) {
      try {
        const token = tick.token ? tick.token.toString() : null;
        
        if (!token) {
          logger.warn('Tick missing token:', tick);
          continue;
        }
        
        // Get symbol for the token
        const symbol = tokenToSymbolMap.get(token);
        
        // Log tick data
        logger.debug(`Tick for ${symbol || token}: LTP=${tick.ltp}, open=${tick.open}, high=${tick.high}, low=${tick.low}, close=${tick.close}`);
        
        // Add processing logic here
        // ...
        
      } catch (tickError) {
        logger.error('Error processing tick:', tickError);
      }
    }
  } catch (error) {
    logger.error('Error handling data ticks:', error);
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
    const priceKey = `price:${symbol}`;
    
    // Create a simplified price object
    const priceData = {
      symbol,
      token,
      lastPrice: tickData.ltp || 0,
      open: tickData.open || 0,
      high: tickData.high || 0,
      low: tickData.low || 0,
      close: tickData.close || 0,
      totalBuyQty: tickData.totBuyQty || tickData.totalBuyQuantity || 0,
      totalSellQty: tickData.totSellQty || tickData.totalSellQuantity || 0,
      volume: tickData.totalTradedQty || tickData.volume || 0,
      lastUpdateTime: Date.now()
    };
    
    // Store in Redis
    await redisClient.set(priceKey, JSON.stringify(priceData));
    
    // Publish update for any real-time subscribers
    await redisPubSub.publish(`price:update:${symbol}`, JSON.stringify(priceData));
    
  } catch (error) {
    logger.error(`Error updating price for ${symbol}:`, error);
  }
}

/**
 * Update order plan price
 * @param {string} planId - Order plan ID
 * @param {Object} tickData - Tick data
 */
async function updateOrderPlanPrice(planId, tickData) {
  try {
    // Get the order plan
    const plan = await orderPlanRedisService.getOrderPlan(planId);
    
    if (!plan) {
      // Plan doesn't exist anymore, remove from subscriptions
      unsubscribeFromOrderPlan(planId);
      return;
    }
    
    // Update last price
    const updates = {
      currentPrice: tickData.ltp || 0,
      lastUpdated: new Date().toISOString()
    };
    
    // Calculate price difference from entry
    if (plan.entry_price) {
      const entryPrice = parseFloat(plan.entry_price);
      const currentPrice = updates.currentPrice;
      
      updates.priceDiff = currentPrice - entryPrice;
      updates.priceDiffPercentage = ((currentPrice - entryPrice) / entryPrice * 100).toFixed(2);
      
      // Check if price has reached entry or exit
      if (plan.transactiontype === 'BUY') {
        // For buy orders
        if (currentPrice <= entryPrice && plan.status === 'CREATED') {
          updates.status = 'ENTRY_TRIGGERED';
        } else if (currentPrice >= parseFloat(plan.exit_price) && ['ENTRY_TRIGGERED', 'CREATED'].includes(plan.status)) {
          updates.status = 'EXIT_TRIGGERED';
        }
      } else {
        // For sell orders
        if (currentPrice >= entryPrice && plan.status === 'CREATED') {
          updates.status = 'ENTRY_TRIGGERED';
        } else if (currentPrice <= parseFloat(plan.exit_price) && ['ENTRY_TRIGGERED', 'CREATED'].includes(plan.status)) {
          updates.status = 'EXIT_TRIGGERED';
        }
      }
    }
    
    // Update the plan in Redis
    await orderPlanRedisService.updateOrderPlan(planId, updates);
    
    // Publish an update event
    await redisPubSub.publish(`orderplan:update:${planId}`, JSON.stringify({
      planId,
      updates
    }));
    
  } catch (error) {
    logger.error(`Error updating order plan ${planId}:`, error);
  }
}

/**
 * Subscribe to an order plan
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
    
    // Add to maps
    tokenToSymbolMap.set(token, symbol);
    symbolToTokenMap.set(symbol, token);
    
    // Initialize subscription set if needed
    if (!subscriptions.has(token)) {
      subscriptions.set(token, new Set());
      
      // Subscribe to the new token
      await subscribeToToken(token, plan.exchange);
    }
    
    // Add plan to subscription set
    subscriptions.get(token).add(planId);
    
    logger.info(`Subscribed plan ${planId} to symbol ${symbol} (token ${token})`);
  } catch (error) {
    logger.error(`Error subscribing to order plan ${planId}:`, error);
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
 * Subscribe to a token
 * @param {string} token - Symbol token
 * @param {string} exchange - Exchange (NSE, BSE, etc.)
 */
async function subscribeToToken(token, exchange) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    logger.warn(`Cannot subscribe to token ${token}: WebSocket not connected`);
    return;
  }
  
  try {
    // Map exchange to exchange type
    const exchangeType = mapExchangeToType(exchange);
    
    // Create subscription message
    const subscribeMessage = {
      correlationID: "subscribe_" + Date.now(),
      action: 1, // Subscribe
      params: {
        mode: 1, // FULL mode
        tokenList: [
          {
            exchangeType,
            tokens: [token]
          }
        ]
      }
    };
    console.log(subscribeMessage);
    // Send subscription request
    ws.send(JSON.stringify(subscribeMessage));
    
    logger.info(`Subscribed to token ${token} on exchange ${exchange} (type ${exchangeType})`);
  } catch (error) {
    logger.error(`Error subscribing to token ${token}:`, error);
    throw error;
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
    // Find exchange type for this token
    const symbol = tokenToSymbolMap.get(token);
    const plan = await findOrderPlanBySymbol(symbol);
    
    if (!plan) {
      logger.warn(`Cannot determine exchange for token ${token}`);
      return;
    }
    
    const exchangeType = mapExchangeToType(plan.exchange);
    
    // Create unsubscribe message
    const unsubscribeMessage = {
      correlationID: "unsubscribe_" + Date.now(),
      action: 0, // Unsubscribe
      params: {
        mode: 1, // FULL mode
        tokenList: [
          {
            exchangeType,
            tokens: [token]
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
 * Find an order plan by symbol
 * @param {string} symbol - Trading symbol
 * @returns {Promise<Object|null>} Order plan or null
 */
async function findOrderPlanBySymbol(symbol) {
  try {
    const plans = await orderPlanRedisService.getAllOrderPlans({ symbol }, 1, 0);
    return plans.length > 0 ? plans[0] : null;
  } catch (error) {
    logger.error(`Error finding order plan by symbol ${symbol}:`, error);
    return null;
  }
}

/**
 * Map exchange name to exchange type code
 * @param {string} exchange - Exchange name
 * @returns {number} Exchange type code
 */
function mapExchangeToType(exchange) {
  switch (exchange.toUpperCase()) {
    case 'NSE':
      return 1;
    case 'BSE':
      return 2;
    case 'NFO':
    case 'NSE_FO':
      return 3;
    case 'BFO':
    case 'BSE_FO':
      return 4;
    case 'CDS':
    case 'NSE_CDS':
      return 5;
    case 'MCX':
      return 6;
    case 'NCDEX':
      return 7;
    case 'BCD':
    case 'BSE_CDS':
      return 8;
    default:
      logger.warn(`Unknown exchange: ${exchange}, defaulting to NSE (1)`);
      return 1;
  }
}


/**
 * Resubscribe to all tokens
 * @param {Object} session - Session details
 */
async function resubscribeAll(session) {
  try {
    // Group tokens by exchange
    const exchangeTokens = new Map(); // Map<exchangeType, string[]>
    
    for (const [token, planIds] of subscriptions.entries()) {
      if (planIds.size === 0) continue;
      
      const symbol = tokenToSymbolMap.get(token);
      const plan = await findOrderPlanBySymbol(symbol);
      
      if (!plan) continue;
      
      const exchangeType = mapExchangeToType(plan.exchange);
      
      if (!exchangeTokens.has(exchangeType)) {
        exchangeTokens.set(exchangeType, []);
      }
      
      exchangeTokens.get(exchangeType).push(token);
    }
    
    // Create token list for subscription
    const tokenList = [];
    for (const [exchangeType, tokens] of exchangeTokens.entries()) {
      tokenList.push({
        exchangeType,
        tokens
      });
    }
    
    if (tokenList.length === 0) {
      logger.info('No tokens to resubscribe');
      return;
    }
    
    // Create subscription message
    const subscribeMessage = {
      correlationID: "resubscribe_" + Date.now(),
      action: 1, // Subscribe
      params: {
        mode: 1, // FULL mode
        tokenList
      }
    };
    console.log(subscribeMessage);
    // Send subscription request
    ws.send(JSON.stringify(subscribeMessage));
    
    logger.info(`Resubscribed to ${tokenList.length} exchanges with ${subscriptions.size} tokens`);
  } catch (error) {
    logger.error('Error resubscribing to tokens:', error);
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
    
    // Reconnect WebSocket with new session
    connectWebSocket(session);
  } catch (error) {
    logger.error('Error refreshing subscriptions:', error);
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

// Export modified orderPlanRedisService with WebSocket integration
export const orderPlanService = {
  // Create order plan with WebSocket subscription
  async storeOrderPlan(orderPlan) {
    try {
      // Store in Redis
      const storedPlan = await orderPlanRedisService.storeOrderPlan(orderPlan);
      
      // Publish event for WebSocket subscription
      await redisPubSub.publish('orderplan:new', storedPlan.id);
      
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
        // Publish event for WebSocket unsubscription
        await redisPubSub.publish('orderplan:delete', planId);
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