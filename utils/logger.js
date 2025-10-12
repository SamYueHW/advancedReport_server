const winston = require('winston');
const config = require('config');
const path = require('path');
const fs = require('fs');

// Ensure logs directory exists
const logsDir = path.dirname(config.get('logging.file'));
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

const logger = winston.createLogger({
    level: config.get('logging.level'),
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    defaultMeta: { service: 'syncdb-server' },
    transports: [
        new winston.transports.File({
            filename: config.get('logging.file'),
            maxsize: config.get('logging.maxSize'),
            maxFiles: config.get('logging.maxFiles')
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