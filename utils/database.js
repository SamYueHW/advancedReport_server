const mysql = require('mysql2/promise');
const logger = require('./logger');
const dbConfig = require('../config/database.json');

class DatabaseManager {
    constructor() {
        this.pools = new Map();
        this.config = dbConfig.mysql;
    }

    // Get or create connection pool for specific database
    async getPool(database) {
        if (this.pools.has(database)) {
            const pool = this.pools.get(database);
            try {
                // Test the connection
                const connection = await pool.getConnection();
                connection.release();
                return pool;
            } catch (error) {
                // Pool is invalid, remove it
                this.pools.delete(database);
            }
        }

        try {
            const poolConfig = {
                ...this.config,
                database: database
            };

            const pool = mysql.createPool(poolConfig);
            
            // Test the connection
            const connection = await pool.getConnection();
            connection.release();
            
            this.pools.set(database, pool);
            logger.info(`Connected to MySQL database: ${database}`);
            
            return pool;
        } catch (error) {
            logger.error(`Failed to connect to MySQL database ${database}:`, error);
            throw error;
        }
    }

    // Get database name by app ID (previously machine name)
    getDatabaseByMachine(appId) {
        const mapping = dbConfig.machineMapping[appId];
        if (!mapping) {
            throw new Error(`App ID ${appId} not found in configuration`);
        }
        return mapping.database;
    }

    // Execute SQL query
    async executeQuery(database, query, parameters = []) {
        try {
            const pool = await this.getPool(database);
            const [rows, fields] = await pool.execute(query, parameters);
            
            return {
                rows: rows,
                fields: fields,
                affectedRows: rows.affectedRows || 0,
                insertId: rows.insertId || 0
            };
        } catch (error) {
            logger.error(`Query execution failed for database ${database}:`, error);
            throw error;
        }
    }

    // Get a connection from the pool
    async getConnection(database) {
        try {
            const pool = await this.getPool(database);
            const connection = await pool.getConnection();
            return connection;
        } catch (error) {
            logger.error(`Failed to get connection for database ${database}:`, error);
            throw error;
        }
    }

    // Execute LOAD DATA INFILE query (not supported in prepared statements)
    async executeLoadDataQuery(database, query) {
        try {
            logger.info(`=== executeLoadDataQuery Debug Start ===`);
            logger.info(`Database: ${database}`);
            logger.info(`Query length: ${query.length} characters`);
            
            const pool = await this.getPool(database);
            logger.info(`Got connection pool for database: ${database}`);
            
            const connection = await pool.getConnection();
            logger.info(`Got database connection`);
            
            try {
                // Check current connection settings
                logger.info(`Checking connection settings...`);
                
                try {
                    const [localInfileResult] = await connection.query("SELECT @@local_infile as local_infile");
                    logger.info(`Connection local_infile setting: ${localInfileResult[0].local_infile}`);
                } catch (checkError) {
                    logger.warn(`Could not check local_infile setting: ${checkError.message}`);
                }
                
                // Try to enable local_infile for this connection
                logger.info(`Attempting to enable local_infile...`);
                try {
                    await connection.query('SET GLOBAL local_infile = 1');
                    logger.info(`Successfully set GLOBAL local_infile = 1`);
                } catch (setError) {
                    logger.warn(`Could not set GLOBAL local_infile: ${setError.message}`);
                }
                
                // Execute the LOAD DATA query
                logger.info(`Executing LOAD DATA query...`);
                const startTime = Date.now();
                
                let result;
                
                // Check if this is a LOCAL INFILE query
                if (query.includes('LOAD DATA LOCAL INFILE')) {
                    logger.info(`Detected LOCAL INFILE query, setting up streamFactory...`);
                    
                    // Extract file path from query
                    const pathMatch = query.match(/LOAD DATA LOCAL INFILE '([^']+)'/);
                    if (!pathMatch) {
                        throw new Error('Could not extract file path from LOCAL INFILE query');
                    }
                    const filePath = pathMatch[1];
                    logger.info(`File path extracted: ${filePath}`);
                    
                    const fs = require('fs');
                    
                    // Execute with streamFactory
                    const [rows, fields] = await connection.query({
                        sql: query,
                        infileStreamFactory: () => {
                            logger.info(`Creating read stream for file: ${filePath}`);
                            return fs.createReadStream(filePath);
                        }
                    });
                    
                    result = [rows, fields];
                } else {
                    // Regular LOAD DATA INFILE (not LOCAL)
                    logger.info(`Executing regular LOAD DATA INFILE...`);
                    const [rows, fields] = await connection.query(query);
                    result = [rows, fields];
                }
                
                const duration = Date.now() - startTime;
                logger.info(`Query executed in ${duration}ms`);
                logger.info(`Rows result type: ${typeof result[0]}`);
                logger.info(`Rows result:`, JSON.stringify(result[0], null, 2));
                logger.info(`Fields count: ${result[1] ? result[1].length : 'N/A'}`);
                
                const finalResult = {
                    rows: result[0],
                    fields: result[1],
                    affectedRows: result[0].affectedRows || 0,
                    insertId: result[0].insertId || 0
                };
                
                logger.info(`Final result object:`, JSON.stringify(finalResult, null, 2));
                logger.info(`=== executeLoadDataQuery Debug End ===`);
                
                return finalResult;
            } finally {
                connection.release();
                logger.info(`Database connection released`);
            }
        } catch (error) {
            logger.error(`=== executeLoadDataQuery Error ===`);
            logger.error(`Error message: ${error.message}`);
            logger.error(`Error code: ${error.code}`);
            logger.error(`Error errno: ${error.errno}`);
            logger.error(`Error sqlState: ${error.sqlState}`);
            logger.error(`Error sqlMessage: ${error.sqlMessage}`);
            logger.error(`Error stack:`, error.stack);
            logger.error(`Full error object:`, JSON.stringify(error, null, 2));
            throw error;
        }
    }

    // Close all connections
    async closeAll() {
        for (const [database, pool] of this.pools) {
            try {
                await pool.end();
                logger.info(`Closed connection to MySQL database: ${database}`);
            } catch (error) {
                logger.error(`Error closing connection to ${database}:`, error);
            }
        }
        this.pools.clear();
    }

    // Check if table exists
    async tableExists(database, tableName) {
        const query = `
            SELECT COUNT(*) as count 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE UPPER(TABLE_NAME) = UPPER(?) AND TABLE_SCHEMA = ?
        `;
        const result = await this.executeQuery(database, query, [tableName, database]);
        return result.rows[0].count > 0;
    }

    // Get table schema
    async getTableSchema(database, tableName) {
        const query = `
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE UPPER(TABLE_NAME) = UPPER(?) AND TABLE_SCHEMA = ?
            ORDER BY ORDINAL_POSITION
        `;
        const result = await this.executeQuery(database, query, [tableName, database]);
        return result.rows;
    }
}

module.exports = new DatabaseManager(); 