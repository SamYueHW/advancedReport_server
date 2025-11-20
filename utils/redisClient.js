const redis = require('redis');
const logger = require('./logger');

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

const redisClient = redis.createClient({ url: redisUrl });

redisClient.on('connect', async () => {
  logger.info(`Redis client connected (${redisUrl})`);
  await clearAdvancedReportOnlineKeys();
});

redisClient.on('reconnecting', () => {
  logger.warn('Redis client reconnecting...');
});

redisClient.on('error', (err) => {
  logger.error(`Redis client error: ${err.message}`);
});

async function connectRedis() {
  if (!redisClient.isOpen) {
    try {
      await redisClient.connect();
    } catch (err) {
      logger.error(`Failed to connect to Redis: ${err.message}`);
    }
  }
}

async function clearAdvancedReportOnlineKeys() {
  try {
    if (!redisClient.isOpen) return;
    const pattern = 'advancedreport:online:*';
    const iterator = redisClient.scanIterator({ MATCH: pattern, COUNT: 100 });
    for await (const key of iterator) {
      await redisClient.del(key);
      logger.info(`Cleared stale Redis key ${key}`);
    }
  } catch (err) {
    logger.warn(`Failed to clear Advanced Report online keys: ${err.message}`);
  }
}

connectRedis();

module.exports = redisClient;

