import logger from '../utils/logger.js';

/**
 * Convert script details to trading symbol format
 * @param {Object} scriptDetails - Script details
 * @returns {string} Formatted trading symbol
 */
export function convertToTradingSymbol(scriptDetails) {
  try {
    if (!scriptDetails || !scriptDetails.symbol) {
      throw new Error('Invalid script details provided');
    }
    
    let tradingSymbol = scriptDetails.symbol;
    const exchange = scriptDetails.exch_seg;
    const instrumentType = scriptDetails.instrumenttype;
    
    // Handle equity instruments
    if (exchange === 'NSE' && !instrumentType) {
      tradingSymbol = `${scriptDetails.symbol}-EQ`;
    } 
    else if (exchange === 'BSE' && !instrumentType) {
      // BSE symbols remain unchanged
      tradingSymbol = scriptDetails.symbol;
    }
    // Handle futures
    else if (['NFO', 'CDS', 'MCX', 'BFO'].includes(exchange) && 
             ['FUTSTK', 'FUTIDX', 'FUTCUR', 'FUTCOM'].includes(instrumentType)) {
      
      if (!scriptDetails.expiry) {
        logger.warn(`Missing expiry date for future: ${scriptDetails.symbol}`);
        return tradingSymbol;
      }
      
      const expiryDate = new Date(scriptDetails.expiry);
      const monthCode = 'JFMAMJJASOND'.charAt(expiryDate.getMonth());
      const year = expiryDate.getFullYear().toString().slice(-2);
      
      if (exchange === 'NFO') {
        tradingSymbol = `${scriptDetails.symbol}${monthCode}${year}FUT`;
      } else if (exchange === 'CDS') {
        tradingSymbol = `${scriptDetails.symbol}${monthCode}${year}FUT`;
      } else if (exchange === 'MCX') {
        tradingSymbol = `${scriptDetails.symbol}${monthCode}${year}`;
      }
    }
    // Handle options
    else if (['NFO', 'CDS', 'BFO'].includes(exchange) &&
             ['OPTSTK', 'OPTIDX', 'OPTCUR'].includes(instrumentType)) {
      
      if (!scriptDetails.expiry || !scriptDetails.strike) {
        logger.warn(`Missing expiry or strike for option: ${scriptDetails.symbol}`);
        return tradingSymbol;
      }
      
      const expiryDate = new Date(scriptDetails.expiry);
      const day = expiryDate.getDate().toString().padStart(2, '0');
      const monthCode = 'JFMAMJJASOND'.charAt(expiryDate.getMonth());
      const year = expiryDate.getFullYear().toString().slice(-2);
      
      // Determine option type (CE/PE)
      let optionType = '';
      if (scriptDetails.symbol.includes('CE')) {
        optionType = 'CE';
      } else if (scriptDetails.symbol.includes('PE')) {
        optionType = 'PE';
      } else {
        logger.warn(`Could not determine option type for: ${scriptDetails.symbol}`);
        return tradingSymbol;
      }
      
      // Format strike price without decimal point
      const strikePrice = scriptDetails.strike.toString().replace('.', '');
      
      tradingSymbol = `${scriptDetails.symbol.replace(/CE|PE/g, '')}${day}${monthCode}${year}${strikePrice}${optionType}`;
    }
    
    logger.debug(`Converted ${scriptDetails.symbol} to trading symbol: ${tradingSymbol}`);
    return tradingSymbol;
  } catch (error) {
    logger.error('Error converting to trading symbol:', error);
    throw new Error(`Failed to convert to trading symbol: ${error.message}`);
  }
}