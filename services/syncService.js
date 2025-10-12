const logger = require('../utils/logger');
const dbManager = require('../utils/database');
const Joi = require('joi');
const fs = require('fs').promises;
const path = require('path');

// Validation schema for sync data
const syncDataSchema = Joi.object({
    MachineName: Joi.string().required(),
    TableName: Joi.string().valid('SalesDetail', 'StockItems', 'MenuItem', 'SubMenuLinkDetail', 'Sales').required(),
    Operation: Joi.string().valid('INSERT', 'UPDATE', 'DELETE', 'SCHEMA_CHANGE').required(),
    Data: Joi.object().required(),
    Timestamp: Joi.date().required(),
    SyncId: Joi.string().required(),
    DatabaseType: Joi.string().valid('retail', 'hospitality').optional()
});

class SyncService {
    constructor() {
        this.processingQueue = new Map();
        this.csvUploadQueue = new Map(); // Track CSV uploads per machine/table
    }

    // Get WHERE fields for a specific table
    getWhereFields(tableName) {
        // Table-specific WHERE fields mapping
        const tableWhereFields = {
            'SalesDetail': ['InvoiceNo', 'StockId'],
            'StockItems': ['StockId'],
            'Sales': ['InvoiceNo', 'TransactionDate'],
            'MenuItem': ['ItemCode'],
            'SubMenuLinkDetail': ['ItemCode', 'SubItemCode']
        };
        
        return tableWhereFields[tableName] || ['ID'];
    }

    // Build WHERE condition based on table configuration
    buildWhereCondition(tableName, data, isOldData = false) {
        const whereFields = this.getWhereFields(tableName);
        const whereCondition = {};
        
        // Extract data source (for UPDATE operations with new/old data)
        let sourceData = data;
        if (isOldData && data.old) {
            sourceData = data.old.deleted || data.old;
        } else if (!isOldData && data.new) {
            sourceData = data.new.inserted || data.new;
        } else if (data.new && !data.old) {
            // If only new data is available, use it
            sourceData = data.new.inserted || data.new;
        }
        
        // Build WHERE condition using configured fields
        for (const field of whereFields) {
            if (sourceData[field] !== undefined && sourceData[field] !== null) {
                whereCondition[field] = sourceData[field];
            }
        }
        
        // If no WHERE fields found, log warning but continue
        if (Object.keys(whereCondition).length === 0) {
            logger.warn(`No WHERE condition fields found for ${tableName}. Required fields: ${whereFields.join(', ')}`);
            // Try to use any available ID field as fallback
            if (sourceData.ID !== undefined) {
                whereCondition.ID = sourceData.ID;
                logger.info(`Using ID as fallback WHERE condition for ${tableName}`);
            }
        }
        
        return whereCondition;
    }

    // Validate sync data
    validateSyncData(data) {
        const { error, value } = syncDataSchema.validate(data);
        if (error) {
            throw new Error(`Validation failed: ${error.details[0].message}`);
        }
        return value;
    }

    // Process sync data from client
    async processSyncData(syncData) {
        try {
            // Validate input data
            const validatedData = this.validateSyncData(syncData);
            const { MachineName, TableName, Operation, Data, SyncId } = validatedData;

            logger.info(`Processing sync data: ${SyncId} - ${MachineName}/${TableName}/${Operation}`);

            // Check if already processing this sync ID
            if (this.processingQueue.has(SyncId)) {
                return { success: false, error: 'Sync already in progress' };
            }

            // Add to processing queue
            this.processingQueue.set(SyncId, true);

            try {
                // Get database configuration for this machine
                const database = dbManager.getDatabaseByMachine(MachineName);
                
                // Process based on operation type
                let result;
                switch (Operation) {
                    case 'INSERT':
                        result = await this.handleInsert(database, TableName, Data);
                        break;
                    case 'UPDATE':
                        result = await this.handleUpdate(database, TableName, Data);
                        break;
                    case 'DELETE':
                        result = await this.handleDelete(database, TableName, Data);
                        break;
                    case 'SCHEMA_CHANGE':
                        // Schema change functionality has been removed
                        logger.info(`Schema change operation received but functionality is disabled. SyncId: ${SyncId}`);
                        result = { success: true, message: 'Schema change functionality disabled' };
                        break;
                    default:
                        throw new Error(`Unsupported operation: ${Operation}`);
                }

                logger.info(`Sync completed successfully: ${SyncId}`);
                return { success: true, result };

            } finally {
                this.processingQueue.delete(SyncId);
            }

        } catch (error) {
            // Re-throw TABLE_NOT_EXISTS errors so app.js can handle them
            if (error.message.startsWith('TABLE_NOT_EXISTS:')) {
                throw error;
            }
            
            logger.error(`Sync processing failed: ${error.message}`, error);
            return { success: false, error: error.message };
        }
    }

    // Handle INSERT operation
    async handleInsert(database, TableName, Data, isFullSync = false) {
        try {
            // Check if table exists
            const tableExists = await dbManager.tableExists(database, TableName);
            if (!tableExists) {
                throw new Error(`TABLE_NOT_EXISTS:${TableName}`);
            }

            // Build INSERT query for MySQL
            const columns = Object.keys(Data);
            const values = Object.values(Data);
            const placeholders = columns.map(() => '?').join(', ');
            
            // Always use regular INSERT - let errors be handled by catch block
            const query = `
                INSERT INTO \`${TableName}\` (\`${columns.join('`, `')}\`)
                VALUES (${placeholders})
            `;

            const result = await dbManager.executeQuery(database, query, values);
            
            logger.debug(`INSERT completed for ${database}.${TableName}, rows affected: ${result.affectedRows}`);
            return { 
                rowsAffected: result.affectedRows,
                operation: 'INSERT',
                wasUpdate: false
            };

        } catch (error) {
            if (error.code === 'ER_DUP_ENTRY') { // MySQL duplicate entry error
                if (isFullSync) {
                    // For full sync, if duplicate exists, just skip it (it means data already exists)
                    logger.debug(`Duplicate key detected during full sync, skipping row for ${TableName}`);
                    return { 
                        rowsAffected: 0,
                        operation: 'SKIP',
                        wasUpdate: false
                    };
                } else {
                    // For incremental sync, attempt UPDATE
                    logger.warn(`Duplicate key detected, attempting UPDATE instead`);
                    const updateResult = await this.handleUpdate(database, TableName, Data);
                    return {
                        ...updateResult,
                        operation: 'UPDATE',
                        wasUpdate: true
                    };
                }
            }
            throw error;
        }
    }

    // Handle UPDATE operation
    async handleUpdate(database, TableName, Data) {
        try {
            // Check if table exists first
            const tableExists = await dbManager.tableExists(database, TableName);
            if (!tableExists) {
                throw new Error(`TABLE_NOT_EXISTS:${TableName}`);
            }

            // Use new WHERE condition building logic
            const whereCondition = this.buildWhereCondition(TableName, Data, true); // true for old data
            const updateData = this.getUpdateData(TableName, Data);
            
            if (Object.keys(whereCondition).length === 0) {
                throw new Error(`WHERE condition is required for UPDATE operation on ${TableName}. Required fields: ${this.getWhereFields(TableName).join(', ')}`);
            }
            
            if (Object.keys(updateData).length === 0) {
                logger.warn(`No update data found for ${TableName}, skipping UPDATE`);
                return { rowsAffected: 0 };
            }
            
            const setClause = Object.keys(updateData)
                .map((col, index) => `\`${col}\` = ?`)
                .join(', ');
            
            const whereClause = Object.keys(whereCondition)
                .map((col, index) => `\`${col}\` = ?`)
                .join(' AND ');

            const query = `
                UPDATE \`${TableName}\` 
                SET ${setClause}
                WHERE ${whereClause}
            `;

            const parameters = [...Object.values(updateData), ...Object.values(whereCondition)];
            const result = await dbManager.executeQuery(database, query, parameters);
            
            logger.info(`UPDATE completed for ${database}.${TableName}, rows affected: ${result.affectedRows}, WHERE: ${JSON.stringify(whereCondition)}`);
            return { rowsAffected: result.affectedRows };

        } catch (error) {
            throw error;
        }
    }

    // Extract update data from the Data object
    getUpdateData(tableName, data) {
        let updateData = {};
        
        // Handle the format from client: { new: { inserted: {...} }, old: { deleted: {...} } }
        if (data.new) {
            updateData = data.new.inserted || data.new;
        } else if (data.ID) {
            // Handle direct row data (from full sync duplicate handling)
            updateData = { ...data };
            delete updateData.ID; // Don't update the ID field
        } else {
            // Fallback for other formats
            updateData = { ...data };
            delete updateData._key;
        }
        
        // Remove WHERE condition fields from update data to avoid updating them
        const whereFields = this.getWhereFields(tableName);
        for (const field of whereFields) {
            delete updateData[field];
        }
        
        return updateData;
    }

    // Handle DELETE operation
    async handleDelete(database, TableName, Data) {
        try {
            // Check if table exists first
            const tableExists = await dbManager.tableExists(database, TableName);
            if (!tableExists) {
                throw new Error(`TABLE_NOT_EXISTS:${TableName}`);
            }

            // Use new WHERE condition building logic
            const whereCondition = this.buildWhereCondition(TableName, Data, false);
            
            if (Object.keys(whereCondition).length === 0) {
                throw new Error(`WHERE condition is required for DELETE operation on ${TableName}. Required fields: ${this.getWhereFields(TableName).join(', ')}`);
            }

            const whereClause = Object.keys(whereCondition)
                .map((col, index) => `\`${col}\` = ?`)
                .join(' AND ');

            const query = `
                DELETE FROM \`${TableName}\`
                WHERE ${whereClause}
            `;

            const parameters = Object.values(whereCondition);
            const result = await dbManager.executeQuery(database, query, parameters);
            
            logger.info(`DELETE completed for ${database}.${TableName}, rows affected: ${result.affectedRows}, WHERE: ${JSON.stringify(whereCondition)}`);
            return { rowsAffected: result.affectedRows };

        } catch (error) {
            throw error;
        }
    }

    // Schema change functionality has been removed
    // All schema-related methods have been disabled for security and stability

    // Handle schema changes - DISABLED
    async handleSchemaChange(database, TableName, Data) {
        logger.info(`Schema change request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Schema change functionality disabled' };
    }

    // Handle table alteration based on DDL trigger data - DISABLED
    async handleTableAlteration(database, TableName, Data) {
        logger.info(`Table alteration request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Table alteration functionality disabled' };
    }

    // Handle OTHER_ALTER type operations - DISABLED
    async handleOtherAlterOperation(database, TableName, tsqlCommand) {
        logger.info(`Other alter operation request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Other alter functionality disabled' };
    }

    // Attempt direct conversion - DISABLED
    attemptDirectConversion(tableName, tsqlCommand) {
        logger.info(`Direct conversion request received but functionality is disabled for table: ${tableName}`);
        return null;
    }

    // Parse ALTER TABLE ADD COLUMN command - DISABLED
    async parseAndAddColumn(database, TableName, tsqlCommand, columnDetails = null) {
        logger.info(`Add column request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Add column functionality disabled' };
    }

    // Parse column definition - DISABLED
    parseColumnDefinition(definition) {
        logger.info(`Column definition parsing requested but functionality is disabled`);
        return {};
    }

    // Build MySQL column definition - DISABLED
    buildMySQLColumnDefinition(parsedDef) {
        logger.info(`MySQL column definition building requested but functionality is disabled`);
        return 'TEXT';
    }

    // Convert SQL Server data type to MySQL - DISABLED
    convertDataTypeToMySQL(sqlServerType) {
        logger.info(`Data type conversion requested but functionality is disabled`);
        return 'TEXT';
    }

    // Convert SQL Server default value to MySQL - DISABLED
    convertDefaultValueToMySQL(defaultValue, dataType) {
        logger.info(`Default value conversion requested but functionality is disabled`);
        return null;
    }

    // Parse ALTER TABLE DROP COLUMN command - DISABLED
    async parseAndDropColumn(database, TableName, tsqlCommand) {
        logger.info(`Drop column request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Drop column functionality disabled' };
    }

    // Parse ALTER TABLE ALTER COLUMN command - DISABLED
    async parseAndModifyColumn(database, TableName, tsqlCommand) {
        logger.info(`Modify column request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Modify column functionality disabled' };
    }

    // Force client schema on server - DISABLED
    async forceClientSchema(database, TableName, Data) {
        logger.info(`Force client schema request received but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Force client schema functionality disabled' };
    }

    // Log schema conflict - DISABLED
    async logSchemaConflict(database, TableName, Data) {
        logger.info(`Schema conflict logging requested but functionality is disabled: ${database}.${TableName}`);
        return { success: true, message: 'Schema conflict logging disabled' };
    }

    // Request table schema from client - DISABLED
    async requestTableSchema(appId, tableName) {
        logger.info(`Table schema request received but functionality is disabled: App ${appId}/${tableName}`);
        throw new Error(`Table ${tableName} does not exist. Schema functionality is disabled.`);
    }

    // Create table with provided schema - RESTORED
    async createTableWithSchema(database, tableName, schema, databaseType = null) {
        try {
            // Extract data from new schema format
            const columns = schema.columns || schema; // Support both old and new format
            const primaryKeys = schema.primaryKeys || [];
            const indexes = schema.indexes || [];
            
            let columnDefinitions = [];
            let primaryKeyColumns = [];
            
            // Process each column from schema
            columns.forEach(column => {
                let colDef = `\`${column.COLUMN_NAME}\` ${this.convertToMySQLType(column)}`;
                
                // Handle default values first
                let hasDefault = false;
                if (column.COLUMN_DEFAULT !== null && column.COLUMN_DEFAULT !== undefined) {
                    const defaultValue = this.formatDefaultValue(column.COLUMN_DEFAULT, column.DATA_TYPE);
                    if (defaultValue !== null) {
                        colDef += ` DEFAULT ${defaultValue}`;
                        hasDefault = true;
                    }
                }
                
                // Only enforce NOT NULL if there's a default value or it's a primary key/identity column
                if (column.IS_NULLABLE === 'NO' && (hasDefault || column.IS_IDENTITY === 1 || column.COLUMN_KEY === 'PRI')) {
                    colDef += ' NOT NULL';
                } else {
                    // For columns without defaults, allow NULL to prevent import errors
                    colDef += ' NULL DEFAULT NULL';
                }
                
                // Handle identity columns (AUTO_INCREMENT)
                if (column.IS_IDENTITY === 1) {
                    colDef += ' AUTO_INCREMENT';
                }
                
                // Collect primary key columns - ONLY based on COLUMN_KEY='PRI'
                if (column.COLUMN_KEY === 'PRI') {
                    primaryKeyColumns.push(column.COLUMN_NAME);
                }
                
                columnDefinitions.push(colDef);
            });

            // Add primary key constraint if any primary key columns exist
            if (primaryKeyColumns.length > 0) {
                const pkConstraint = `PRIMARY KEY (\`${primaryKeyColumns.join('`, `')}\`)`;
                columnDefinitions.push(pkConstraint);
            }

            // Create the table
            const createQuery = `
                CREATE TABLE \`${tableName}\` (
                    ${columnDefinitions.join(',\n                    ')}
                )
            `;

            await dbManager.executeQuery(database, createQuery);
            logger.info(`Created table ${tableName} in database ${database} from client schema`);
            logger.debug(`Table structure: ${columnDefinitions.join(', ')}`);
            
            // Create indexes if any exist
            if (indexes && indexes.length > 0) {
                await this.createTableIndexes(database, tableName, indexes);
            }
            
            // Add industry-specific indexes and constraints based on database type
            if (databaseType === 'hospitality') {
                await this.addHospitalitySpecificIndexes(database, tableName);
            } else if (databaseType === 'retail') {
                await this.addRetailSpecificIndexes(database, tableName);
            } else {
                logger.info(`No database type specified for ${tableName}, skipping industry-specific indexes`);
            }
            
        } catch (error) {
            logger.error(`Failed to create table ${tableName}: ${error.message}`);
            throw error;
        }
    }

    // Add hospitality-specific indexes and constraints
    async addHospitalitySpecificIndexes(database, tableName) {
        try {
            const hospitalityIndexes = {
                'MenuItem': [
                    'ALTER TABLE `MenuItem` ADD PRIMARY KEY (ItemCode)',  // 主键索引
                    'ALTER TABLE `MenuItem` ADD INDEX idx_category (Category)',
                    'ALTER TABLE `MenuItem` ADD FULLTEXT INDEX idx_desc_ngram (Description1, Description2) WITH PARSER ngram',
                    // 删除以下冗余索引：
                    // 'ALTER TABLE `MenuItem` ADD INDEX idx_itemcode (ItemCode)',  // 与主键重复
                    // 'ALTER TABLE `MenuItem` ADD INDEX idx_itemcode_category (ItemCode, Category)',  // 使用率低
                    // 'ALTER TABLE `MenuItem` ADD FULLTEXT INDEX idx_desc_default (Description1, Description2)'  // 与ngram重复
                ],
                'SalesDetail': [
                    'ALTER TABLE `SalesDetail` ADD INDEX idx_item_orderno (ItemCode, OrderNo)',
                    'ALTER TABLE `SalesDetail` ADD INDEX idx_orderno (OrderNo)'
                    // 当前配置合理，无需修改
                ],
                'Sales': [
                    'ALTER TABLE `Sales` ADD PRIMARY KEY (OrderNo)',  // 主键索引
                    'ALTER TABLE `Sales` ADD INDEX idx_orderdate (OrderDate)',
                    'ALTER TABLE `Sales` ADD INDEX idx_orderdate_orderno (OrderDate, OrderNo)',  // 新增复合索引
                    // 删除以下冗余索引：
                    // 'ALTER TABLE `Sales` ADD INDEX idx_orderdate (OrderDate)'  // 已在上方添加
                ]
            };
            if (hospitalityIndexes[tableName]) {
                logger.info(`Adding hospitality-specific indexes for table ${tableName}`);
                
                for (const indexQuery of hospitalityIndexes[tableName]) {
                    try {
                        await dbManager.executeQuery(database, indexQuery);
                        logger.info(`✅ Added index: ${indexQuery}`);
                    } catch (indexError) {
                        // Log warning but don't fail the entire process
                        // Some indexes might already exist or have conflicts
                        logger.warn(`⚠️ Failed to add index for ${tableName}: ${indexError.message}`);
                        logger.warn(`Query: ${indexQuery}`);
                    }
                }
                
                logger.info(`Completed adding hospitality indexes for ${tableName}`);
            }
        } catch (error) {
            logger.warn(`Error adding hospitality indexes for ${tableName}: ${error.message}`);
            // Don't throw error to avoid breaking table creation
        }
    }

    // Add retail-specific indexes and constraints
    async addRetailSpecificIndexes(database, tableName) {
        try {
            const retailIndexes = {
               'StockItems': [
                    'ALTER TABLE `StockItems` ADD PRIMARY KEY (StockId)',  
                    'ALTER TABLE `StockItems` ADD INDEX idx_category (Category)',
                    'ALTER TABLE `StockItems` ADD INDEX idx_category_stockid (Category, StockId)',
                    'ALTER TABLE `StockItems` ADD FULLTEXT INDEX idx_desc_ngram (Description, Description1, Description2, Description3) WITH PARSER ngram',
                    // -- 删除以下冗余索引：
                    // -- 'ALTER TABLE `StockItems` ADD INDEX idx_stockid (StockId)',  -- 与主键重复
                    // -- 'ALTER TABLE `StockItems` ADD FULLTEXT INDEX idx_desc_default (Description, Description1, Description2, Description3)',  -- 与ngram重复
                    // -- 'ALTER TABLE `StockItems` ADD INDEX idx_stockid_desc (StockId, Description)',  -- 使用率低
                    // -- 'ALTER TABLE `StockItems` ADD INDEX idx_stockid_category (StockId, Category)',  -- 与idx_category_stockid功能重叠
                ],
                'SalesDetail': [
                    'ALTER TABLE `SalesDetail` ADD INDEX idx_invoiceno_stockid (InvoiceNo, StockId)',
                    'ALTER TABLE `SalesDetail` ADD INDEX idx_stockid (StockId)',
                    'ALTER TABLE `SalesDetail` ADD INDEX idx_invoiceno (InvoiceNo)'
                    
                   
                ],
                'Sales': [
                    'ALTER TABLE `Sales` ADD PRIMARY KEY (InvoiceNo)', 
                    'ALTER TABLE `Sales` ADD INDEX idx_transactiondate (TransactionDate)',
                    'ALTER TABLE `Sales` ADD INDEX idx_transactiondate_invoiceno (TransactionDate, InvoiceNo)', 
                    // -- 删除以下冗余索引：
                    // -- 'ALTER TABLE `Sales` ADD INDEX idx_invoiceno (InvoiceNo)',  -- 与主键重复
                    // -- 'ALTER TABLE `Sales` ADD INDEX idx_invoiceno_transactiondate (InvoiceNo, TransactionDate)',  -- 顺序不佳
                ],
            };

            if (retailIndexes[tableName]) {
                logger.info(`Adding retail-specific indexes for table ${tableName}`);
                
                for (const indexQuery of retailIndexes[tableName]) {
                    try {
                        await dbManager.executeQuery(database, indexQuery);
                        logger.info(`✅ Added retail index: ${indexQuery}`);
                    } catch (indexError) {
                        // Log warning but don't fail the entire process
                        // Some indexes might already exist or have conflicts
                        logger.warn(`⚠️ Failed to add retail index for ${tableName}: ${indexError.message}`);
                        logger.warn(`Query: ${indexQuery}`);
                    }
                }
                
                logger.info(`Completed adding retail indexes for ${tableName}`);
            }
        } catch (error) {
            logger.warn(`Error adding retail indexes for ${tableName}: ${error.message}`);
            // Don't throw error to avoid breaking table creation
        }
    }

    // Create indexes for the table - RESTORED
    async createTableIndexes(database, tableName, indexes) {
        try {
            for (const index of indexes) {
                const indexName = index.INDEX_NAME;
                const isUnique = index.IS_UNIQUE;
                const columns = index.COLUMNS || [];
                
                if (columns.length === 0) {
                    logger.warn(`Skipping index ${indexName} - no columns defined`);
                    continue;
                }
                
                // Build column list with sort order
                const columnList = columns.map(col => {
                    const direction = col.IS_DESCENDING ? 'DESC' : 'ASC';
                    return `\`${col.COLUMN_NAME}\` ${direction}`;
                }).join(', ');
                
                // Create index query
                const uniqueKeyword = isUnique ? 'UNIQUE' : '';
                const createIndexQuery = `
                    CREATE ${uniqueKeyword} INDEX \`${indexName}\` 
                    ON \`${tableName}\` (${columnList})
                `;
                
                await dbManager.executeQuery(database, createIndexQuery);
                logger.info(`Created ${isUnique ? 'unique ' : ''}index ${indexName} on table ${tableName}`);
            }
        } catch (error) {
            logger.error(`Failed to create indexes for table ${tableName}: ${error.message}`);
            // Don't throw here - table creation succeeded, index creation is secondary
            logger.warn(`Continuing without some indexes for table ${tableName}`);
        }
    }

    // Format default value for MySQL - RESTORED
    formatDefaultValue(defaultValue, dataType) {
        if (defaultValue === null || defaultValue === undefined) {
            return null;
        }
        
        // Handle object types that might come from C#
        if (typeof defaultValue === 'object') {
            return null; // Skip complex default values
        }
        
        const lowerDataType = dataType.toLowerCase();
        const valueStr = String(defaultValue);
        
        // Handle special SQL Server defaults
        if (valueStr.includes('getdate()') || valueStr.includes('GETDATE()')) {
            return 'CURRENT_TIMESTAMP';
        }
        
        if (valueStr.includes('newid()') || valueStr.includes('NEWID()')) {
            return null; // MySQL doesn't have direct equivalent, skip
        }
        
        // For string types, wrap in quotes
        if (lowerDataType.includes('varchar') || lowerDataType.includes('char') || lowerDataType.includes('text')) {
            return `'${valueStr.replace(/'/g, "''")}'`; // Escape single quotes
        }
        
        // For numeric types, return as-is if it's a valid number
        if (lowerDataType.includes('int') || lowerDataType.includes('decimal') || lowerDataType.includes('float')) {
            if (/^-?\d+(\.\d+)?$/.test(valueStr)) {
                return valueStr;
            }
            return null;
        }
        
        // For boolean types
        if (lowerDataType === 'bit') {
            return valueStr === '1' || valueStr.toLowerCase() === 'true' ? '1' : '0';
        }
        
        return null; // Skip unknown default values
    }

    // Convert SQL Server/client data types to MySQL types - RESTORED
    convertToMySQLType(column) {
        const dataType = column.DATA_TYPE.toLowerCase();
        const maxLength = column.CHARACTER_MAXIMUM_LENGTH;
        const precision = column.NUMERIC_PRECISION;
        const scale = column.NUMERIC_SCALE;
        
        switch (dataType) {
            case 'int':
            case 'integer':
                return 'INT';
            case 'bigint':
                return 'BIGINT';
            case 'smallint':
                return 'SMALLINT';
            case 'tinyint':
                return 'TINYINT';
            case 'decimal':
            case 'numeric':
                return `DECIMAL(${precision || 18},${scale || 0})`;
            case 'float':
                return 'FLOAT';
            case 'real':
                return 'DOUBLE';
            case 'varchar':
            case 'nvarchar':
                return `VARCHAR(${maxLength || 255})`;
            case 'char':
            case 'nchar':
                return `CHAR(${maxLength || 1})`;
            case 'text':
            case 'ntext':
                return 'TEXT';
            case 'datetime':
            case 'datetime2':
                return 'DATETIME';
            case 'date':
                return 'DATE';
            case 'time':
                return 'TIME';
            case 'timestamp':
                return 'TIMESTAMP';
            case 'bit':
                return 'BOOLEAN';
            default:
                return 'TEXT';
        }
    }

    // Convert SQL Server ALTER TABLE commands to MySQL equivalents - DISABLED
    convertSqlServerToMySQL(tableName, tsqlCommand) {
        logger.info(`SQL Server to MySQL conversion requested but functionality is disabled for table: ${tableName}`);
        return null;
    }

    // CSV Bulk Import Methods
    async handleCSVBulkUpload(appId, tableName, fileName, fileContent, fileSizeBytes, rowCount) {
        try {
            logger.info(`Receiving CSV file: ${fileName} for App ${appId}/${tableName} (${fileSizeBytes} bytes, ${rowCount} rows)`);

            // Create uploads directory if it doesn't exist
            const uploadsDir = path.join(__dirname, '../uploads');
            await fs.mkdir(uploadsDir, { recursive: true });

            // Save file to disk
            const filePath = path.join(uploadsDir, fileName);
            const fileBuffer = Buffer.from(fileContent, 'base64');
            await fs.writeFile(filePath, fileBuffer);

            logger.info(`CSV file saved: ${filePath}`);
            
            // Verify actual file size
            const stats = await fs.stat(filePath);
            logger.info(`Actual file size on disk: ${stats.size} bytes (expected: ${fileSizeBytes} bytes)`);
            if (stats.size !== fileSizeBytes) {
                logger.warn(`File size mismatch! Actual: ${stats.size}, Expected: ${fileSizeBytes}`);
            }

            // Initialize or update CSV upload tracking for this app/table
            const uploadKey = `${appId}_${tableName}`;
            if (!this.csvUploadQueue.has(uploadKey)) {
                this.csvUploadQueue.set(uploadKey, {
                    appId,
                    tableName,
                    files: [],
                    totalRows: 0,
                    totalFiles: 0,
                    expectedFiles: 0, // Will be set based on file naming pattern
                    startTime: new Date()
                });
            }

            const uploadInfo = this.csvUploadQueue.get(uploadKey);
            uploadInfo.files.push({
                fileName,
                filePath,
                rowCount,
                fileSizeBytes
            });
            uploadInfo.totalRows += rowCount;
            uploadInfo.totalFiles = uploadInfo.files.length;

            // Check if this is a batch file (e.g., "TableName_batch_1_of_16_timestamp.csv")
            const batchMatch = fileName.match(/_batch_(\d+)_of_(\d+)_/);
            if (batchMatch) {
                uploadInfo.expectedFiles = parseInt(batchMatch[2]);
                logger.info(`Expecting ${uploadInfo.expectedFiles} files for ${tableName}, received ${uploadInfo.totalFiles} so far`);
            }

            return {
                success: true,
                message: `CSV file received: ${fileName}`,
                fileName,
                rowCount
            };

        } catch (error) {
            logger.error(`Failed to handle CSV bulk upload: ${error.message}`, error);
            return {
                success: false,
                error: error.message,
                fileName
            };
        }
    }

    async getCSVUploadInfo(appId, tableName) {
        const uploadKey = `${appId}_${tableName}`;
        const uploadInfo = this.csvUploadQueue.get(uploadKey);
        
        if (!uploadInfo) {
            return null;
        }

        // Check if all expected files have been received
        const allFilesReceived = uploadInfo.expectedFiles > 0 && 
                                uploadInfo.totalFiles >= uploadInfo.expectedFiles;

        return {
            ...uploadInfo,
            allFilesReceived
        };
    }

    async processSingleCSVFile(storeId, appId, tableName, fileName, progressCallback = null) {
        try {
            logger.info(`Processing single CSV file: ${fileName} for Store ${storeId}, App ${appId}/${tableName}`);

            // Get database for this store and app using license service
            const licenseService = require('./licenseService');
            const database = await licenseService.getDatabaseByStoreAndApp(storeId, appId);
            if (!database) {
                throw new Error(`No database configuration found for Store ${storeId}, App ${appId}`);
            }

            // Check if table exists
            const tableExists = await dbManager.tableExists(database, tableName);
            if (!tableExists) {
                throw new Error(`Table ${tableName} does not exist in database ${database}`);
            }

            // Find the file in uploads directory
            const uploadsDir = path.join(__dirname, '../uploads');
            const filePath = path.join(uploadsDir, fileName);

            // Check if file exists
            try {
                await fs.access(filePath);
            } catch (error) {
                throw new Error(`CSV file not found: ${filePath}`);
            }

            // Send initial progress
            if (progressCallback) {
                progressCallback({
                    fileName: fileName,
                    processedRows: 0,
                    message: `Starting Full Sync import of ${fileName}...`
                });
            }

            // Note: Table clearing is now handled separately by Full Sync clear_database_tables request
            if (progressCallback) {
                progressCallback({
                    fileName: fileName,
                    processedRows: 0,
                    message: `Starting import of ${fileName}...`
                });
            }

            // Process this CSV file using LOAD DATA INFILE
            // Table already cleared above, so don't clear again
            const processedRows = await this.importCSVFileToMySQL(database, tableName, filePath, fileName, false);

            // Send completion progress
            if (progressCallback) {
                progressCallback({
                    fileName: fileName,
                    processedRows: processedRows,
                    message: `Completed importing ${fileName} - ${processedRows} rows processed`
                });
            }

            // Clean up file after processing
            try {
                await fs.unlink(filePath);
                logger.info(`Cleaned up CSV file: ${filePath}`);
            } catch (cleanupError) {
                logger.warn(`Failed to cleanup CSV file ${filePath}: ${cleanupError.message}`);
            }

            logger.info(`Single CSV file processing completed: ${fileName} - ${processedRows} rows imported`);

            return {
                success: true,
                processedRows: processedRows,
                fileName: fileName,
                tableName: tableName,
                appId: appId
            };

        } catch (error) {
            logger.error(`Single CSV file processing failed for ${fileName}: ${error.message}`, error);
            
            // Try to clean up file on error
            try {
                const uploadsDir = path.join(__dirname, '../uploads');
                const filePath = path.join(uploadsDir, fileName);
                await fs.unlink(filePath).catch(() => {});
            } catch (cleanupError) {
                logger.warn(`Error during cleanup: ${cleanupError.message}`);
            }

            return {
                success: false,
                error: error.message,
                processedRows: 0,
                fileName: fileName,
                tableName: tableName,
                appId: appId
            };
        }
    }

    async processCSVBulkImport(storeId, appId, tableName, progressCallback = null) {
        const uploadKey = `${appId}_${tableName}`;
        const uploadInfo = this.csvUploadQueue.get(uploadKey);

        if (!uploadInfo || uploadInfo.files.length === 0) {
            throw new Error(`No CSV files found for Store ${storeId}, App ${appId}/${tableName}`);
        }

        try {
            logger.info(`Starting CSV bulk import for Store ${storeId}, App ${appId}/${tableName}: ${uploadInfo.totalFiles} files, ${uploadInfo.totalRows} total rows`);

            // Get database for this store and app using license service
            const licenseService = require('./licenseService');
            const database = await licenseService.getDatabaseByStoreAndApp(storeId, appId);
            if (!database) {
                throw new Error(`No database configuration found for Store ${storeId}, App ${appId}`);
            }

            // Check if table exists
            const tableExists = await dbManager.tableExists(database, tableName);
            if (!tableExists) {
                throw new Error(`Table ${tableName} does not exist in database ${database}`);
            }

            let totalProcessedRows = 0;
            let currentFileIndex = 0;

            for (const fileInfo of uploadInfo.files) {
                currentFileIndex++;
                
                logger.info(`Processing CSV file ${currentFileIndex}/${uploadInfo.totalFiles}: ${fileInfo.fileName}`);

                // Send progress update if callback is provided
                if (progressCallback) {
                    progressCallback({
                        fileName: fileInfo.fileName,
                        processedRows: 0,
                        currentFile: currentFileIndex,
                        totalFiles: uploadInfo.totalFiles,
                        message: `Processing ${fileInfo.fileName}...`
                    });
                }

                // Process this CSV file using LOAD DATA INFILE
                // Never clear table automatically - clearing is handled by separate clear_database_tables request
                const processedRows = await this.importCSVFileToMySQL(database, tableName, fileInfo.filePath, fileInfo.fileName, false);
                totalProcessedRows += processedRows;

                // Send progress update after file is processed
                if (progressCallback) {
                    progressCallback({
                        fileName: fileInfo.fileName,
                        processedRows: processedRows,
                        currentFile: currentFileIndex,
                        totalFiles: uploadInfo.totalFiles,
                        message: `Completed ${fileInfo.fileName} - ${processedRows} rows imported`
                    });
                }

                // Clean up file after processing
                try {
                    await fs.unlink(fileInfo.filePath);
                    logger.info(`Cleaned up CSV file: ${fileInfo.filePath}`);
                } catch (cleanupError) {
                    logger.warn(`Failed to cleanup CSV file ${fileInfo.filePath}: ${cleanupError.message}`);
                }
            }

            // Clear upload queue for this machine/table
            this.csvUploadQueue.delete(uploadKey);

            logger.info(`CSV bulk import completed for App ${appId}/${tableName}: ${totalProcessedRows} rows imported from ${uploadInfo.totalFiles} files`);

            return {
                success: true,
                totalRows: totalProcessedRows,
                totalFiles: uploadInfo.totalFiles,
                tableName,
                appId
            };

        } catch (error) {
            logger.error(`CSV bulk import failed for App ${appId}/${tableName}: ${error.message}`, error);
            
            // Clean up any remaining files
            try {
                for (const fileInfo of uploadInfo.files) {
                    await fs.unlink(fileInfo.filePath).catch(() => {});
                }
            } catch (cleanupError) {
                logger.warn(`Error during cleanup: ${cleanupError.message}`);
            }

            // Clear upload queue
            this.csvUploadQueue.delete(uploadKey);

            throw error;
        }
    }

    async importCSVFileToMySQL(database, tableName, filePath, fileName, shouldClearTable = false) {
        try {
            logger.info(`=== CSV Import Debug Start ===`);
            logger.info(`File: ${fileName}`);
            logger.info(`Database: ${database}`);
            logger.info(`Table: ${tableName}`);
            logger.info(`File Path: ${filePath}`);
            
            // Check if file exists
            const fs = require('fs');
            if (!fs.existsSync(filePath)) {
                throw new Error(`CSV file does not exist: ${filePath}`);
            }
            
            const fileStats = fs.statSync(filePath);
            logger.info(`File size: ${fileStats.size} bytes`);
            
            // Read first few lines to show sample data
            const firstLines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/).slice(0, 5);
            logger.info(`First 5 lines of CSV file:`);
            firstLines.forEach((line, index) => {
                logger.info(`Line ${index + 1}: ${line.substring(0, 200)}${line.length > 200 ? '...' : ''}`);
            });
            
            // Detect line endings
            const content = fs.readFileSync(filePath, 'utf8');
            const hasCarriageReturn = content.includes('\r\n');
            const lineEnding = hasCarriageReturn ? '\\r\\n' : '\\n';
            logger.info(`Detected line ending: ${hasCarriageReturn ? 'CRLF (\\r\\n)' : 'LF (\\n)'}`);
            
            // Get the actual table name from database
            const actualTableName = await this.getActualTableName(database, tableName);
            logger.info(`Actual table name: ${actualTableName}`);
            
            // Check MySQL settings to determine the best approach
            logger.info(`Checking MySQL settings...`);
            let secureFilePrivDir = null;
            let localInfileEnabled = false;
            
            try {
                const localInfileCheck = await dbManager.executeQuery(database, "SHOW GLOBAL VARIABLES LIKE 'local_infile'");
                localInfileEnabled = localInfileCheck.rows[0]?.Value === 'ON' || localInfileCheck.rows[0]?.Value === '1';
                logger.info(`MySQL local_infile enabled: ${localInfileEnabled}`);
                
                const secureFilePriv = await dbManager.executeQuery(database, "SHOW GLOBAL VARIABLES LIKE 'secure_file_priv'");
                secureFilePrivDir = secureFilePriv.rows[0]?.Value;
                logger.info(`MySQL secure_file_priv directory: ${secureFilePrivDir || 'NULL'}`);
                
                const version = await dbManager.executeQuery(database, "SELECT VERSION() as version");
                logger.info(`MySQL version: ${version.rows[0].version}`);
            } catch (checkError) {
                logger.error(`Failed to check MySQL settings: ${checkError.message}`);
            }
            
            // Strategy 1: Try LOCAL INFILE first (if enabled)
            if (localInfileEnabled) {
                logger.info(`=== Attempting LOAD DATA LOCAL INFILE ===`);
                try {
                    const normalizedPath = filePath.replace(/\\/g, '/');
                    
                    // Get table columns to build proper column list with conversions
                    const tableColumns = await this.getTableColumns(database, actualTableName);
                    logger.info(`Table columns: ${tableColumns.join(', ')}`);
                    
                    // Read CSV headers to map columns
                    const csvHeaders = firstLines[0].split(',').map(header => header.replace(/['"]/g, '').trim());
                    logger.info(`CSV headers: ${csvHeaders.join(', ')}`);
                    
                    // Build column mapping with data type conversions
                    const columnMappings = this.buildColumnMappings(csvHeaders, tableColumns);
                    logger.info(`Column mappings generated successfully`);
                    logger.info(`CSV Variables (${columnMappings.csvColumns.length}): ${columnMappings.csvColumns.slice(0, 5).join(', ')}...`);
                    logger.info(`SET Statements (${columnMappings.setStatements.length}): First 3 statements generated`);
                    
                    // Clear existing data only if explicitly requested (should only happen via clear_database_tables)
                    if (shouldClearTable) {
                        logger.info(`Clearing existing data from table ${actualTableName} (explicitly requested)`);
                        await dbManager.execute(database, `DELETE FROM \`${actualTableName}\``);
                    } 
                    
                    const localQuery = `
                        LOAD DATA LOCAL INFILE '${normalizedPath}'
                        INTO TABLE \`${actualTableName}\`
                        FIELDS TERMINATED BY ','
                        OPTIONALLY ENCLOSED BY '"'
                        LINES TERMINATED BY '${lineEnding}'
                        IGNORE 1 ROWS
                        (${columnMappings.csvColumns.join(', ')})
                        SET ${columnMappings.setStatements.join(', ')}
                    `;
                    
                   
                    const startTime = Date.now();
                    const result = await dbManager.executeLoadDataQuery(database, localQuery);
                    const duration = Date.now() - startTime;
                    
                    const affectedRows = result.affectedRows || 0;
                    logger.info(`LOAD DATA LOCAL INFILE completed in ${duration}ms`);
                    logger.info(`Rows imported: ${affectedRows}`);
                    
                    // Always check warnings to understand what happened
                    let skippedRows = 0;
                    let warnings = [];
                    try {
                        // Use regular query for SHOW WARNINGS (not supported in prepared statements)
                        const connection = await dbManager.getConnection(database);
                        try {
                            const [warningRows] = await connection.query('SHOW WARNINGS');
                            if (warningRows.length > 0) {
                                logger.info(`MySQL warnings/info (${warningRows.length} found):`);
                                warningRows.forEach((warning, index) => {
                                    logger.info(`[${warning.Level}] ${warning.Code} - ${warning.Message}`);
                                    warnings.push(warning);
                                    
                                    // Count skipped rows from duplicate key warnings
                                    if (warning.Code === 1062 || warning.Message.includes('Duplicate entry')) {
                                        skippedRows++;
                                    }
                                });
                            }
                        } finally {
                            connection.release();
                        }
                    } catch (warnError) {
                        logger.error(`Could not retrieve warnings: ${warnError.message}`);
                    }
                    
                    if (affectedRows > 0 || skippedRows > 0) {
                        logger.info(`=== CSV Import Completed ===`);
                        logger.info(`Rows successfully imported: ${affectedRows}`);
                        logger.info(`Rows skipped (duplicates): ${skippedRows}`);
                        logger.info(`Total rows processed: ${affectedRows + skippedRows} (estimated)`);
                        return affectedRows;
                    } else {
                        logger.warn(`No rows imported and no duplicates found`);
                        return 0;
                    }
                } catch (localError) {
                    logger.warn(`LOAD DATA LOCAL INFILE failed: ${localError.message}`);
                }
            }
            
            // Strategy 2: Use secure-file-priv directory if specified
            if (secureFilePrivDir && secureFilePrivDir !== 'NULL' && secureFilePrivDir !== '') {
                logger.info(`=== Attempting LOAD DATA INFILE with secure-file-priv directory ===`);
                logger.info(`Copying file to secure directory: ${secureFilePrivDir}`);
                
                try {
                    // Ensure the secure directory exists
                    if (!fs.existsSync(secureFilePrivDir)) {
                        throw new Error(`Secure file directory does not exist: ${secureFilePrivDir}`);
                    }
                    
                    // Copy file to secure directory
                    const secureFilePath = path.join(secureFilePrivDir, fileName);
                    const normalizedSecurePath = secureFilePath.replace(/\\/g, '/');
                    
                    await fs.promises.copyFile(filePath, secureFilePath);
                    logger.info(`File copied to: ${secureFilePath}`);
                    
                    // Get table columns to build proper column list with conversions
                    const tableColumns = await this.getTableColumns(database, actualTableName);
                    logger.info(`Table columns: ${tableColumns.join(', ')}`);
                    
                    // Read CSV headers to map columns
                    const csvHeaders = firstLines[0].split(',').map(header => header.replace(/['"]/g, '').trim());
                    logger.info(`CSV headers: ${csvHeaders.join(', ')}`);
                    
                    // Build column mapping with data type conversions
                    const columnMappings = this.buildColumnMappings(csvHeaders, tableColumns);
                    logger.info(`Column mappings: ${JSON.stringify(columnMappings, null, 2)}`);
                    
                    const query = `
                        LOAD DATA INFILE '${normalizedSecurePath}'
                        IGNORE INTO TABLE \`${actualTableName}\`
                        FIELDS TERMINATED BY ','
                        OPTIONALLY ENCLOSED BY '"'
                        LINES TERMINATED BY '${lineEnding}'
                        IGNORE 1 ROWS
                        (${columnMappings.csvColumns.join(', ')})
                        SET ${columnMappings.setStatements.join(', ')}
                    `;
                    
                    logger.info(`SQL Query: ${query}`);
                    
                    const startTime = Date.now();
                    const result = await dbManager.executeLoadDataQuery(database, query);
                    const duration = Date.now() - startTime;
                    
                    const affectedRows = result.affectedRows || 0;
                    logger.info(`LOAD DATA INFILE completed in ${duration}ms`);
                    logger.info(`Rows imported: ${affectedRows}`);
                    
                    // Clean up the copied file
                    try {
                        await fs.promises.unlink(secureFilePath);
                        logger.info(`Cleaned up copied file: ${secureFilePath}`);
                    } catch (cleanupError) {
                        logger.warn(`Failed to cleanup copied file: ${cleanupError.message}`);
                    }
                    
                    // Always check warnings to understand what happened
                    let skippedRows = 0;
                    let warnings = [];
                    try {
                        // Use regular query for SHOW WARNINGS (not supported in prepared statements)
                        const connection = await dbManager.getConnection(database);
                        try {
                            const [warningRows] = await connection.query('SHOW WARNINGS');
                            if (warningRows.length > 0) {
                                logger.info(`MySQL warnings/info (${warningRows.length} found):`);
                                warningRows.forEach((warning, index) => {
                                    logger.info(`[${warning.Level}] ${warning.Code} - ${warning.Message}`);
                                    warnings.push(warning);
                                    
                                    // Count skipped rows from duplicate key warnings
                                    if (warning.Code === 1062 || warning.Message.includes('Duplicate entry')) {
                                        skippedRows++;
                                    }
                                });
                            }
                        } finally {
                            connection.release();
                        }
                    } catch (warnError) {
                        logger.error(`Could not retrieve warnings: ${warnError.message}`);
                    }
                    
                    // Also check the data load info
                    try {
                        const infoResult = await dbManager.executeQuery(database, 'SHOW STATUS LIKE "Com_load"');
                        if (infoResult.rows.length > 0) {
                            logger.info(`LOAD DATA statistics: ${JSON.stringify(infoResult.rows)}`);
                        }
                    } catch (infoError) {
                        // Ignore if this fails
                    }
                    
                    if (affectedRows > 0 || skippedRows > 0) {
                        logger.info(`=== CSV Import Completed ===`);
                        logger.info(`Rows successfully imported: ${affectedRows}`);
                        logger.info(`Rows skipped (duplicates): ${skippedRows}`);
                        logger.info(`Total rows in CSV: ${affectedRows + skippedRows} (estimated)`);
                        return affectedRows;
                    } else {
                        logger.warn(`No rows imported and no duplicates found`);
                        return 0;
                    }
                } catch (secureError) {
                    logger.error(`LOAD DATA INFILE with secure directory failed: ${secureError.message}`);
                }
            }
            
            // If we get here, both methods failed
            logger.error(`=== All LOAD DATA methods failed ===`);
            throw new Error(`Could not import CSV file. LOCAL INFILE disabled: ${!localInfileEnabled}, secure-file-priv: ${secureFilePrivDir || 'not set'}`);

        } catch (error) {
            logger.error(`=== CSV Import Fatal Error ===`);
            logger.error(`Error in importCSVFileToMySQL: ${error.message}`);
            logger.error(`Error stack:`, error.stack);
            throw error;
        }
    }

    // Helper method to get actual table name from database
    async getActualTableName(database, tableName) {
        try {
            const query = `
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND UPPER(TABLE_NAME) = UPPER(?)
                LIMIT 1
            `;
            
            const result = await dbManager.executeQuery(database, query, [database, tableName]);
            
            if (result.rows.length === 0) {
                throw new Error(`Table ${tableName} does not exist in database ${database}`);
            }
            
            return result.rows[0].TABLE_NAME;
        } catch (error) {
            logger.error(`Failed to get actual table name for ${tableName}: ${error.message}`);
            throw error;
        }
    }

    async fallbackCSVImport(database, tableName, filePath, fileName) {
        try {
            // Check if file exists
            const fs = require('fs');
            if (!fs.existsSync(filePath)) {
                logger.error(`CSV file does not exist: ${filePath}`);
                throw new Error(`CSV file not found: ${filePath}`);
            }

            // Get actual table name
            const actualTableName = await this.getActualTableName(database, tableName);
            
            const csv = require('csv-parser');
            
            return new Promise((resolve, reject) => {
                let processedRows = 0;
                const batchSize = 1000;
                let batch = [];

                fs.createReadStream(filePath)
                    .pipe(csv())
                    .on('data', async (row) => {
                        batch.push(row);
                        
                        if (batch.length >= batchSize) {
                            try {
                                await this.processBatch(database, actualTableName, batch);
                                processedRows += batch.length;
                                batch = [];
                            } catch (error) {
                                logger.error(`Error processing batch in fallback: ${error.message}`);
                                reject(error);
                                return;
                            }
                        }
                    })
                    .on('end', async () => {
                        try {
                            // Process remaining rows
                            if (batch.length > 0) {
                                await this.processBatch(database, actualTableName, batch);
                                processedRows += batch.length;
                            }
                            
                            logger.info(`Fallback CSV import completed for ${fileName}: ${processedRows} rows`);
                            resolve(processedRows);
                        } catch (error) {
                            logger.error(`Error processing final batch in fallback: ${error.message}`);
                            reject(error);
                        }
                    })
                    .on('error', (error) => {
                        logger.error(`Fallback CSV import failed for ${fileName}: ${error.message}`);
                        reject(error);
                    });
            });

        } catch (error) {
            logger.error(`Fallback CSV import error for ${fileName}: ${error.message}`);
            throw error;
        }
    }

    async processBatch(database, tableName, batch) {
        if (batch.length === 0) return;

        try {
            // Get actual table columns from database
            const tableColumns = await this.getTableColumns(database, tableName);
            logger.debug(`Table ${tableName} has columns: ${tableColumns.join(', ')}`);
            
            // Get columns from CSV data
            const csvColumns = Object.keys(batch[0]);
            logger.debug(`CSV data has columns: ${csvColumns.join(', ')}`);
            
            // Create case-insensitive comparison sets
            const tableColumnMap = new Map();
            tableColumns.forEach(col => {
                tableColumnMap.set(col.toUpperCase(), col); // Map uppercase to actual column name
            });
            
            // Filter to only include columns that exist in the table (case-insensitive)
            const validColumns = [];
            const invalidColumns = [];
            
            csvColumns.forEach(csvCol => {
                const upperCsvCol = csvCol.toUpperCase();
                if (tableColumnMap.has(upperCsvCol)) {
                    // Use the actual table column name (with correct case)
                    validColumns.push(tableColumnMap.get(upperCsvCol));
                } else {
                    invalidColumns.push(csvCol);
                }
            });
            
            if (validColumns.length === 0) {
                logger.error(`No matching columns found between CSV data and table ${tableName}`);
                logger.error(`CSV columns: ${csvColumns.join(', ')}`);
                logger.error(`Table columns: ${tableColumns.join(', ')}`);
                throw new Error(`No matching columns found for table ${tableName}`);
            }
            
            // Log column mapping for debugging
            if (invalidColumns.length > 0) {
                logger.warn(`Skipping columns not found in table ${tableName}: ${invalidColumns.join(', ')}`);
            }
            
            logger.info(`Using columns for table ${tableName}: ${validColumns.join(', ')}`);
            
            const placeholders = validColumns.map(() => '?').join(', ');
            const columnList = validColumns.map(col => `\`${col}\``).join(', ');
            
            const query = `INSERT IGNORE INTO \`${tableName}\` (${columnList}) VALUES (${placeholders})`;
            
            for (const row of batch) {
                // Map CSV column names to table column names for data extraction
                const values = validColumns.map(tableCol => {
                    // Find the corresponding CSV column (case-insensitive)
                    const csvCol = csvColumns.find(col => col.toUpperCase() === tableCol.toUpperCase());
                    return csvCol ? (row[csvCol] === '' ? null : row[csvCol]) : null;
                });
                await dbManager.executeQuery(database, query, values);
            }
            
        } catch (error) {
            logger.error(`Error processing batch for ${tableName}: ${error.message}`);
            throw error;
        }
    }

    async getTableColumns(database, tableName) {
        try {
            logger.info(`=== getTableColumns Debug Start ===`);
            logger.info(`Database: ${database}, TableName: ${tableName}`);
            
            // First, get the actual table name from the database (case-insensitive)
            const tableExistsQuery = `
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND UPPER(TABLE_NAME) = UPPER(?)
                LIMIT 1
            `;
            
            const tableResult = await dbManager.executeQuery(database, tableExistsQuery, [database, tableName]);
            logger.info(`Table existence check - rows found: ${tableResult.rows.length}`);
            
            if (tableResult.rows.length === 0) {
                throw new Error(`Table ${tableName} does not exist in database ${database}`);
            }
            
            // Use the actual table name from the database
            const actualTableName = tableResult.rows[0].TABLE_NAME;
            logger.info(`Actual table name found: ${actualTableName}`);
            
            // Method 1: Try SHOW COLUMNS first (more reliable)
            try {
                const showColumnsQuery = `SHOW COLUMNS FROM \`${database}\`.\`${actualTableName}\``;
                logger.info(`Executing SHOW COLUMNS query: ${showColumnsQuery}`);
                
                const showResult = await dbManager.executeQuery(database, showColumnsQuery);
                logger.info(`SHOW COLUMNS returned ${showResult.rows.length} columns`);
                
                if (showResult.rows.length > 0) {
                    const columns = showResult.rows.map(row => row.Field);
                    logger.info(`Columns from SHOW COLUMNS: ${columns.join(', ')}`);
                    logger.info(`First column: ${columns[0]}, Last column: ${columns[columns.length-1]}`);
                    logger.info(`InvoiceNo in columns: ${columns.includes('InvoiceNo')}`);
                    logger.info(`=== getTableColumns Debug End (using SHOW COLUMNS) ===`);
                    return columns;
                }
            } catch (showError) {
                logger.warn(`SHOW COLUMNS failed, falling back to INFORMATION_SCHEMA: ${showError.message}`);
            }
            
            // Method 2: Fallback to INFORMATION_SCHEMA
            const infoSchemaQuery = `
                SELECT COLUMN_NAME, ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            `;
            
            logger.info(`Executing INFORMATION_SCHEMA query for database: ${database}, table: ${actualTableName}`);
            const result = await dbManager.executeQuery(database, infoSchemaQuery, [database, actualTableName]);
            
            logger.info(`INFORMATION_SCHEMA returned ${result.rows.length} rows`);
            if (result.rows.length > 0) {
                logger.info(`First 3 columns from INFORMATION_SCHEMA:`, result.rows.slice(0, 3).map(r => ({name: r.COLUMN_NAME, position: r.ORDINAL_POSITION})));
            }
            
            const columns = result.rows.map(row => row.COLUMN_NAME);
            
            logger.info(`All columns from INFORMATION_SCHEMA: ${columns.join(', ')}`);
            logger.info(`Total columns: ${columns.length}`);
            logger.info(`InvoiceNo in columns: ${columns.includes('InvoiceNo')}`);
            logger.info(`=== getTableColumns Debug End (using INFORMATION_SCHEMA) ===`);
            
            return columns;
            
        } catch (error) {
            logger.error(`Failed to get table columns for ${tableName}: ${error.message}`);
            logger.error(`Error stack:`, error.stack);
            throw error;
        }
    }

    // Get column types for a table
    async getTableColumnTypes(database, tableName) {
        try {
            // Get the actual table name from the database (case-insensitive)
            const tableExistsQuery = `
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND UPPER(TABLE_NAME) = UPPER(?)
                LIMIT 1
            `;
            
            const tableResult = await dbManager.executeQuery(database, tableExistsQuery, [database, tableName]);
            if (tableResult.rows.length === 0) {
                throw new Error(`Table ${tableName} does not exist in database ${database}`);
            }
            
            const actualTableName = tableResult.rows[0].TABLE_NAME;
            
            // Get column data types
            const query = `
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            `;
            
            const result = await dbManager.executeQuery(database, query, [database, actualTableName]);
            
            // Build a map of column name to data type
            const columnTypes = {};
            result.rows.forEach(row => {
                columnTypes[row.COLUMN_NAME] = row.COLUMN_TYPE; // Use COLUMN_TYPE (e.g., "varchar(50)")
            });
            
            logger.info(`Retrieved column types for ${tableName}: ${Object.keys(columnTypes).length} columns`);
            return columnTypes;
            
        } catch (error) {
            logger.error(`Failed to get column types for ${tableName}: ${error.message}`);
            return null; // Return null to allow fallback to basic conversion
        }
    }

    // Helper method to build column mappings for LOAD DATA INFILE
    buildColumnMappings(csvHeaders, tableColumns) {
        const csvVariables = [];
        const setStatements = [];
        
        // Create variable names for CSV columns
        csvHeaders.forEach((csvCol, index) => {
            csvVariables.push(`@${csvCol.replace(/[^a-zA-Z0-9]/g, '_')}`);
        });
        
        // Map each table column with smart value-based conversion
        tableColumns.forEach((tableCol, index) => {
            if (index < csvHeaders.length) {
                const csvVar = csvVariables[index];
                
                // Simplified protection: directly check for StockId or ItemCode
                const isProtectedColumn = tableCol === 'StockId' || tableCol === 'ItemCode';
                
                // Smart value-based type detection and conversion
                setStatements.push(`\`${tableCol}\` = CASE 
                    -- Handle NULL and empty values first
                    WHEN ${csvVar} IS NULL OR TRIM(${csvVar}) = '' THEN NULL
                    
                    -- Handle boolean values (True/False, 1/0, etc) - but not for protected columns
                    ${!isProtectedColumn ? `WHEN LOWER(TRIM(${csvVar})) IN ('true', 'false', 'yes', 'no', 'y', 'n', 'on', 'off') THEN
                        CASE 
                            WHEN LOWER(TRIM(${csvVar})) IN ('true', 'yes', 'y', 'on') THEN 1 
                            ELSE 0 
                        END` : ''}
                    
                    -- Handle SQL Server default dates and invalid dates
                    WHEN ${csvVar} LIKE '1899-12-30%' OR ${csvVar} = '1900-01-01T00:00:00.000Z' OR ${csvVar} = '0000-00-00' THEN NULL
                    
                    -- Handle ISO datetime format (YYYY-MM-DDTHH:MM:SS)
                    WHEN ${csvVar} REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}' THEN 
                        STR_TO_DATE(SUBSTRING(${csvVar}, 1, 19), '%Y-%m-%dT%H:%i:%s')
                    
                    -- Handle standard datetime format (YYYY-MM-DD HH:MM:SS)
                    WHEN ${csvVar} REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' THEN 
                        STR_TO_DATE(${csvVar}, '%Y-%m-%d %H:%i:%s')
                    
                    -- Handle date only format (YYYY-MM-DD)
                    WHEN ${csvVar} REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 
                        STR_TO_DATE(${csvVar}, '%Y-%m-%d')
                    
                    ${!isProtectedColumn ? `-- Handle pure numeric values (integers and decimals) - only for non-protected columns
                    WHEN ${csvVar} REGEXP '^-?[0-9]+$' THEN CAST(${csvVar} AS SIGNED)
                    WHEN ${csvVar} REGEXP '^-?[0-9]+\\.[0-9]+$' THEN CAST(${csvVar} AS DECIMAL(18,4))` : ''}
                    
                    -- Handle text values (trim and keep as-is, preserving leading zeros for protected columns)
                    ELSE TRIM(${csvVar})
                END`);
            }
        });
        
        return {
            csvColumns: csvVariables,
            setStatements: setStatements
        };
    }
}

module.exports = new SyncService(); 