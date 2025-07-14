/**
 * Async handler to avoid try/catch blocks in route handlers
 * @param {Function} fn - Express route handler
 * @returns {Function} - Wrapped route handler
 */
export default (fn) => {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
};