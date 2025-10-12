const mysql = require('mysql2/promise');
const logger = require('../utils/logger');

class LicenseService {
    constructor() {
        this.connectionPool = null;
        this.config = {
            host: process.env.ONLINE_REPORT_DB_HOST || 'localhost',
            port: parseInt(process.env.ONLINE_REPORT_DB_PORT) || 3306,
            database: process.env.ONLINE_REPORT_DB_NAME || 'online_report',
            user: process.env.ONLINE_REPORT_DB_USER || 'root',
            password: process.env.ONLINE_REPORT_DB_PASSWORD || '123123',
            waitForConnections: true,
            connectionLimit: 3,
            queueLimit: 0
        };
    }

    /**
     * Initialize database connection
     */
    async initialize() {
        try {
            if (!this.connectionPool) {
                this.connectionPool = mysql.createPool(this.config);
                // Test the connection
                const connection = await this.connectionPool.getConnection();
                await connection.ping();
                connection.release();
                logger.info('License service database connection established');
            }
            return true;
        } catch (error) {
            logger.error('Failed to initialize license service database connection:', error);
            return false;
        }
    }

    /**
     * Close database connection
     */
    async close() {
        try {
            if (this.connectionPool) {
                await this.connectionPool.end();
                this.connectionPool = null;
                logger.info('License service database connection closed');
            }
        } catch (error) {
            logger.error('Error closing license service database connection:', error);
        }
    }

    /**
     * Validate Advanced Report license
     * @param {string} storeId - Store ID from client
     * @param {string} appId - Advanced Report App ID from client
     * @returns {Object} - Validation result
     */
    async validateAdvancedReportLicense(storeId, appId) {
        try {
            // Ensure connection is established
            if (!this.connectionPool) {
                const initialized = await this.initialize();
                if (!initialized) {
                    return {
                        isValid: false,
                        isExpired: true,
                        error: 'Database connection failed'
                    };
                }
            }

            const query = `
                SELECT 
                    StoreId,
                    AdvancedReportAppId,
                    AdvancedReportLicenseExpire,
                    StoreName
                FROM stores 
                WHERE StoreId = ? 
                AND AdvancedReportAppId = ?
            `;

            const [rows] = await this.connectionPool.execute(query, [storeId, appId]);

            if (rows.length === 0) {
                logger.warn(`Store not found or AppId mismatch: StoreId=${storeId}, AppId=${appId}`);
                return {
                    isValid: false,
                    isExpired: true,
                    error: 'Store not found or invalid AppId'
                };
            }

            const store = rows[0];
            console.log(store);
            const expireDate = new Date(store.AdvancedReportLicenseExpire);
            const currentDate = new Date();
            const isExpired = expireDate <= currentDate;

            logger.info(`License check for Store ${storeId}: ExpireDate=${expireDate.toISOString()}, IsExpired=${isExpired}`);

            return {
                isValid: !isExpired,
                isExpired: isExpired,
                storeInfo: {
                    storeId: store.StoreId,
                    storeName: store.StoreName,
                    appId: store.AdvancedReportAppId,
                    expireDate: expireDate.toISOString(),
                    daysRemaining: isExpired ? 0 : Math.ceil((expireDate - currentDate) / (1000 * 60 * 60 * 24))
                }
            };

        } catch (error) {
            logger.error('Error validating Advanced Report license:', error);
            return {
                isValid: false,
                isExpired: true,
                error: error.message
            };
        }
    }

    /**
     * Get all stores with Advanced Report licenses
     * @returns {Array} - List of stores
     */
    async getAllAdvancedReportStores() {
        try {
            if (!this.connectionPool) {
                await this.initialize();
            }

            const query = `
                SELECT 
                    StoreId,
                    StoreName,
                    AdvancedReportAppId,
                    AdvancedReportLicenseExpire
                FROM stores 
                WHERE AdvancedReportAppId IS NOT NULL 
                AND AdvancedReportAppId != ''
                ORDER BY StoreId
            `;

            const [rows] = await this.connectionPool.execute(query);
            
            return rows.map(store => ({
                storeId: store.StoreId,
                storeName: store.StoreName,
                appId: store.AdvancedReportAppId,
                expireDate: store.AdvancedReportLicenseExpire,
                isExpired: new Date(store.AdvancedReportLicenseExpire) <= new Date()
            }));

        } catch (error) {
            logger.error('Error getting Advanced Report stores:', error);
            return [];
        }
    }

    /**
     * Get database name for a given storeId and appId combination
     * Validates that the storeId and appId combination exists in stores table
     * @param {string} storeId - Store ID
     * @param {string} appId - Application ID  
     * @returns {Promise<string|null>} Returns appId as database name if valid, null if not found
     */
    async getDatabaseByStoreAndApp(storeId, appId) {
        try {
            if (!this.connectionPool) {
                throw new Error('License service not initialized');
            }

            const query = `
                SELECT StoreId
                FROM stores 
                WHERE StoreId = ? AND AdvancedReportAppId = ?
            `;

            const [rows] = await this.connectionPool.execute(query, [storeId, appId]);
            
            if (rows.length > 0) {
                // Return appId as database name directly
                return appId;
            }
            
            return null;

        } catch (error) {
            logger.error('Error getting database by store and app:', error);
            return null;
        }
    }

    /**
     * Health check for license service
     */
    async healthCheck() {
        try {
            if (!this.connectionPool) {
                await this.initialize();
            }

            const [rows] = await this.connectionPool.execute('SELECT 1 as test');
            return {
                status: 'healthy',
                database: 'connected',
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            return {
                status: 'unhealthy',
                database: 'disconnected',
                error: error.message,
                timestamp: new Date().toISOString()
            };
        }
    }
}

module.exports = new LicenseService();
