import Redis from 'ioredis';
import logger from '../utils/logger.js';
import config from '../config/index.js';

// Initialize Redis client
const redis = new Redis({
  host: config.redis.host || 'localhost',
  port: config.redis.port || 6379,
  password: config.redis.password,
  db: config.redis.db || 0
});

redis.on('error', (error) => {
  logger.error('Redis connection error:', error);
});

redis.on('connect', () => {
  logger.info('Connected to Redis server');
});

/**
 * Clear all script-related data from Redis
 * @returns {Promise<void>}
 */
export async function clearAllScriptData() {
  try {
    // Get all script-related keys
    const scriptKeys = await redis.keys('angel:script:*');
    const tokenKeys = await redis.keys('angel:token:*');
    const indexKeys = [
      'angel:scripts:all',
      'angel:scripts:by_name',
      'angel:scripts:by_symbol'
    ];
    
    // Get exchange and instrument type keys
    const exchangeKeys = await redis.keys('angel:scripts:exchange:*');
    const instrumentKeys = await redis.keys('angel:scripts:instrumenttype:*');
    
    // Combine all keys to delete
    const allKeys = [
      ...scriptKeys,
      ...tokenKeys,
      ...indexKeys,
      ...exchangeKeys,
      ...instrumentKeys
    ];
    
    if (allKeys.length > 0) {
      logger.info(`Clearing ${allKeys.length} script-related keys from Redis`);
      await redis.del(...allKeys);
    }
    
    logger.info('All script data cleared from Redis');
  } catch (error) {
    logger.error('Error clearing script data from Redis:', error);
    throw error;
  }
}

/**
 * Load scripts into Redis with proper indexing
 * @param {Array} scripts - Array of script objects
 * @returns {Promise<void>}
 */
export async function loadScriptsIntoRedis(scripts) {
  try {
    logger.info(`Loading ${scripts.length} scripts into Redis`);
    
    // Store the full list as a JSON string
    await redis.set('angel:scripts:all', JSON.stringify(scripts));
    
    // Use pipeline for better performance
    const pipeline = redis.pipeline();
    
    for (const script of scripts) {
      // Create a unique key for each script
      const key = `angel:script:${script.symbol}`;
      
      // Store the script details as a hash
      pipeline.hmset(key, {
        token: script.token || '',
        symbol: script.symbol || '',
        name: script.name || '',
        expiry: script.expiry || '',
        strike: script.strike || '',
        lotsize: script.lotsize || '',
        instrumenttype: script.instrumenttype || '',
        exch_seg: script.exch_seg || '',
        tick_size: script.tick_size || ''
      });
      
      // Index by token for reverse lookups
      if (script.token) {
        pipeline.set(`angel:token:${script.token}`, script.symbol);
      }
      
      // Create searchable indices by adding to sorted sets
      if (script.name) {
        pipeline.zadd('angel:scripts:by_name', 0, `${script.name.toLowerCase()}:${script.symbol}`);
      }
      
      pipeline.zadd('angel:scripts:by_symbol', 0, `${script.symbol.toLowerCase()}:${script.symbol}`);
      
      // Add to exchange segment sets for filtering
      if (script.exch_seg) {
        pipeline.sadd(`angel:scripts:exchange:${script.exch_seg}`, script.symbol);
      }
      
      // If it has an instrument type, add to that set too
      if (script.instrumenttype) {
        pipeline.sadd(`angel:scripts:instrumenttype:${script.instrumenttype}`, script.symbol);
      }
    }
    
    // Execute all commands in the pipeline
    await pipeline.exec();
    logger.info('All scripts loaded into Redis successfully');
  } catch (error) {
    logger.error('Error loading scripts into Redis:', error);
    throw error;
  }
}

/**
 * Search scripts by name or symbol
 * @param {string} searchTerm - Search term
 * @param {Object} filters - Optional filters
 * @param {string} filters.exchange - Filter by exchange
 * @param {string} filters.instrumentType - Filter by instrument type
 * @param {number} limit - Maximum number of results to return
 * @returns {Promise<Array>} Matching scripts
 */
export async function searchScripts(searchTerm, filters = {}, limit = 20) {
  try {
    searchTerm = searchTerm.toLowerCase();
    
    // Search by name
    const nameMatches = await redis.zrangebylex(
      'angel:scripts:by_name',
      `[${searchTerm}`, 
      `[${searchTerm}\xff`,
      'LIMIT', 0, limit
    );
    
    // Search by symbol
    const symbolMatches = await redis.zrangebylex(
      'angel:scripts:by_symbol',
      `[${searchTerm}`, 
      `[${searchTerm}\xff`,
      'LIMIT', 0, limit
    );
    
    // Combine and deduplicate results
    const allMatches = [...new Set([...nameMatches, ...symbolMatches])];
    
    // Extract just the symbol part from the index entries
    const symbols = allMatches.map(match => match.split(':')[1]);
    
    // Apply exchange filter if provided
    let filteredSymbols = symbols;
    if (filters.exchange) {
      const exchangeScripts = await redis.smembers(`angel:scripts:exchange:${filters.exchange}`);
      filteredSymbols = filteredSymbols.filter(symbol => exchangeScripts.includes(symbol));
    }
    
    // Apply instrument type filter if provided
    if (filters.instrumentType) {
      const instrumentScripts = await redis.smembers(`angel:scripts:instrumenttype:${filters.instrumentType}`);
      filteredSymbols = filteredSymbols.filter(symbol => instrumentScripts.includes(symbol));
    }
    
    // Get full details for each symbol
    const pipeline = redis.pipeline();
    for (const symbol of filteredSymbols.slice(0, limit)) {
      pipeline.hgetall(`angel:script:${symbol}`);
    }
    
    const results = await pipeline.exec();
    return results.map(result => result[1]).filter(Boolean);
  } catch (error) {
    logger.error('Error searching scripts:', error);
    throw new Error(`Script search failed: ${error.message}`);
  }
}

/**
 * Get script by token
 * @param {string} token - Script token
 * @returns {Promise<Object|null>} Script details or null if not found
 */
export async function getScriptByToken(token) {
  try {
    // Get the symbol for this token
    const symbol = await redis.get(`angel:token:${token}`);
    if (!symbol) return null;
    
    // Get the full script details
    return await redis.hgetall(`angel:script:${symbol}`);
  } catch (error) {
    logger.error(`Error getting script by token ${token}:`, error);
    throw new Error(`Failed to get script by token: ${error.message}`);
  }
}

/**
 * Get script by symbol
 * @param {string} symbol - Script symbol
 * @returns {Promise<Object|null>} Script details or null if not found
 */
export async function getScriptBySymbol(symbol) {
  try {
    const scriptDetails = await redis.hgetall(`angel:script:${symbol}`);
    return Object.keys(scriptDetails).length > 0 ? scriptDetails : null;
  } catch (error) {
    logger.error(`Error getting script by symbol ${symbol}:`, error);
    throw new Error(`Failed to get script by symbol: ${error.message}`);
  }
}

/**
 * Get all available exchanges
 * @returns {Promise<Array>} List of exchanges
 */
export async function getAllExchanges() {
  try {
    const exchangeKeys = await redis.keys('angel:scripts:exchange:*');
    return exchangeKeys.map(key => key.replace('angel:scripts:exchange:', ''));
  } catch (error) {
    logger.error('Error getting exchanges:', error);
    throw new Error(`Failed to get exchanges: ${error.message}`);
  }
}

/**
 * Get all instrument types
 * @returns {Promise<Array>} List of instrument types
 */
export async function getAllInstrumentTypes() {
  try {
    const instrumentKeys = await redis.keys('angel:scripts:instrumenttype:*');
    return instrumentKeys.map(key => key.replace('angel:scripts:instrumenttype:', ''));
  } catch (error) {
    logger.error('Error getting instrument types:', error);
    throw new Error(`Failed to get instrument types: ${error.message}`);
  }
}

/**
 * Get script count
 * @returns {Promise<number>} Number of scripts in the database
 */
export async function getScriptCount() {
  try {
    const scripts = await redis.get('angel:scripts:all');
    if (!scripts) return 0;
    return JSON.parse(scripts).length;
  } catch (error) {
    logger.error('Error getting script count:', error);
    throw new Error(`Failed to get script count: ${error.message}`);
  }
}