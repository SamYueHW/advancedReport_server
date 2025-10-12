const winston = require('winston');
const path = require('path');
const fs = require('fs');

// Configuration with defaults
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const LOG_FILE = process.env.LOG_FILE || './logs/server.log';
const LOG_MAX_SIZE = process.env.LOG_MAX_SIZE || 10485760; // 10MB
const LOG_MAX_FILES = process.env.LOG_MAX_FILES || 5;

// Ensure logs directory exists
const logsDir = path.dirname(LOG_FILE);
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

const logger = winston.createLogger({
    level: LOG_LEVEL,
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    defaultMeta: { service: 'syncdb-server' },
    transports: [
        new winston.transports.File({
            filename: LOG_FILE,
            maxsize: LOG_MAX_SIZE,
            maxFiles: LOG_MAX_FILES
        }),
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple()
            )
        })
    ]
});

module.exports = logger; 