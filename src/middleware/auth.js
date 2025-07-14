/**
 * Authentication middleware
 * This is a placeholder - replace with your actual auth logic
 */
export default (req, res, next) => {
  const authHeader = req.headers.authorization;
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({
      status: 'error',
      message: 'Unauthorized - Authentication required'
    });
  }
  
  const token = authHeader.split(' ')[1];
  
  // This is a placeholder - implement your token validation logic
  if (token === 'fake-token') {
    // Set user info in request object
    req.user = {
      id: 'admin',
      role: 'admin'
    };
    return next();
  }
  
  return res.status(401).json({
    status: 'error',
    message: 'Unauthorized - Invalid token'
  });
};