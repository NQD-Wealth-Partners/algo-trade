const config = {
  nodeEnv: process.env.NODE_ENV || 'development',
  port: process.env.PORT || 3000,
  apiPrefix: process.env.API_PREFIX || '/api/v1',
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || '',
    db: parseInt(process.env.REDIS_DB) || 0
  },
  cache: {
    ttl: parseInt(process.env.CACHE_TTL) || 3600 // Default TTL: 1 hour
  },
  rateLimit: {
    windowMs: parseInt(process.env.RATE_LIMIT_WINDOW) * 60 * 1000 || 15 * 60 * 1000, // Default: 15 minutes
    max: parseInt(process.env.RATE_LIMIT_MAX) || 100 // Default: 100 requests per window
  },
  // Add Angel One configuration
  angelOne: {
    apiKey: process.env.ANGEL_ONE_API_KEY
  },
  jwt: {
    secret: process.env.JWT_SECRET || 'your-default-secret-key-for-development',
    expiresIn: process.env.JWT_EXPIRES_IN || '7d'
  }
};

export default config;