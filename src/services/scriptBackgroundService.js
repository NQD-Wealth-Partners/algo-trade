import axios from 'axios';
import cron from 'node-cron';
import logger from '../utils/logger.js';
import * as redisScriptService from './redisScriptService.js';
import config from '../config/index.js';

const SCRIPT_MASTER_URL = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json';

/**
 * Fetch script data from Angel Broking API
 * @returns {Promise<Array>} Array of script objects
 */
export async function fetchScriptData() {
  try {
    logger.info('Fetching script data from Angel Broking API');
    const response = await axios.get(SCRIPT_MASTER_URL);
    
    if (!response.data || !Array.isArray(response.data)) {
      throw new Error('Invalid script data received from API');
    }
    
    logger.info(`Successfully fetched ${response.data.length} scripts from Angel Broking API`);
    return response.data;
  } catch (error) {
    logger.error('Error fetching script data:', error);
    throw new Error(`Failed to fetch script data: ${error.message}`);
  }
}

/**
 * Process and load scripts into Redis
 * @returns {Promise<void>}
 */
export async function updateScriptsInRedis() {
  try {
    const scripts = await fetchScriptData();
    logger.info(`Loading ${scripts.length} scripts into Redis`);
    
    // Clear existing script data
    await redisScriptService.clearAllScriptData();
    
    // Load new script data
    await redisScriptService.loadScriptsIntoRedis(scripts);
    
    logger.info('Script data successfully updated in Redis');
  } catch (error) {
    logger.error('Error updating scripts in Redis:', error);
    throw new Error(`Failed to update scripts in Redis: ${error.message}`);
  }
}

/**
 * Initialize background script updates
 */
export function initScriptUpdates() {
  // Update scripts immediately on startup
  updateScriptsInRedis()
    .then(() => logger.info('Initial script data loaded successfully'))
    .catch(err => logger.error('Failed to load initial script data:', err));
  
  // Schedule periodic updates (default: every day at 1:00 AM)
  const cronSchedule = config.scriptUpdate?.cronSchedule || '0 1 * * *';
  
  cron.schedule(cronSchedule, async () => {
    logger.info(`Running scheduled script update (${cronSchedule})`);
    try {
      await updateScriptsInRedis();
      logger.info('Scheduled script update completed successfully');
    } catch (error) {
      logger.error('Scheduled script update failed:', error);
    }
  });
  
  logger.info(`Scheduled script updates configured with cron: ${cronSchedule}`);
}