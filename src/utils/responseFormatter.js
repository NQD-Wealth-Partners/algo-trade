/**
 * Format successful response
 * @param {Object} res - Express response object
 * @param {*} data - Response data
 * @param {number} statusCode - HTTP status code
 * @returns {Object} Formatted response
 */
export const successResponse = (res, data, statusCode = 200) => {
  return res.status(statusCode).json({
    status: 'success',
    data
  });
};

/**
 * Format error response
 * @param {Object} res - Express response object
 * @param {string} message - Error message
 * @param {number} statusCode - HTTP status code
 * @returns {Object} Formatted error response
 */
export const errorResponse = (res, message, statusCode = 400) => {
  return res.status(statusCode).json({
    status: 'error',
    message
  });
};