const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const logger = require('./utils/logger');
const dbManager = require('./utils/database');
const syncService = require('./services/syncService');
const licenseService = require('./services/licenseService');

// Load environment variables
require('dotenv').config();

// SQL Server to MySQL DDL conversion function
async function convertSqlServerDDLToMySQL(sqlCommand, tableName, operation) {
    try {
        if (!sqlCommand || typeof sqlCommand !== 'string') {
            return null;
        }

        const upperSql = sqlCommand.toUpperCase().trim();
        
        if (operation === 'DDL_ALTER_TABLE') {
            // Convert ALTER TABLE statements
            let mysqlDDL = sqlCommand
                // Replace SQL Server specific data types FIRST
                .replace(/\[dbo\]\./gi, '')
                .replace(/\[NVARCHAR\]\((\d+)\)/gi, 'VARCHAR($1)')  // 处理 [NVARCHAR](2) 格式
                .replace(/NVARCHAR\(MAX\)/gi, 'TEXT')
                .replace(/NVARCHAR\((\d+)\)/gi, 'VARCHAR($1)')
                .replace(/NTEXT/gi, 'TEXT')
                .replace(/BIT/gi, 'BOOLEAN')
                .replace(/DATETIME2/gi, 'DATETIME')
                .replace(/UNIQUEIDENTIFIER/gi, 'VARCHAR(36)')
                .replace(/BIGINT\s+IDENTITY\(1,1\)/gi, 'BIGINT AUTO_INCREMENT')
                .replace(/INT\s+IDENTITY\(1,1\)/gi, 'INT AUTO_INCREMENT')
                .replace(/GETDATE\(\)/gi, 'NOW()')
                .replace(/NEWID\(\)/gi, 'UUID()');

            logger.info(`After data type conversion: ${mysqlDDL}`);

            // Convert common ALTER TABLE operations
            if (upperSql.includes('ADD COLUMN') || upperSql.includes('ADD ')) {
                // Fix ADD COLUMN syntax for MySQL - AFTER data type conversion
                logger.info(`Attempting ADD COLUMN conversion on: ${mysqlDDL}`);
                
                // 先处理带长度和NULL约束的
                const before1 = mysqlDDL;
                mysqlDDL = mysqlDDL.replace(/Add\s+\[([^\]]+)\]\s+([A-Z]+)\((\d+)\)\s+(NULL|NOT NULL)/gi, 'ADD COLUMN `$1` $2($3) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci $4');
                if (before1 !== mysqlDDL) {
                    logger.info(`Matched pattern 1 (with length and NULL): ${mysqlDDL}`);
                    return mysqlDDL;
                }
                
                // 再处理带长度无NULL约束的
                const before2 = mysqlDDL;
                mysqlDDL = mysqlDDL.replace(/Add\s+\[([^\]]+)\]\s+([A-Z]+)\((\d+)\)/gi, 'ADD COLUMN `$1` $2($3) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci');
                if (before2 !== mysqlDDL) {
                    logger.info(`Matched pattern 2 (with length, no NULL): ${mysqlDDL}`);
                    return mysqlDDL;
                }
                
                // 处理无长度有NULL约束的
                const before3 = mysqlDDL;
                mysqlDDL = mysqlDDL.replace(/Add\s+\[([^\]]+)\]\s+([A-Z]+)\s+(NULL|NOT NULL)/gi, 'ADD COLUMN `$1` $2 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci $3');
                if (before3 !== mysqlDDL) {
                    logger.info(`Matched pattern 3 (no length, with NULL): ${mysqlDDL}`);
                    return mysqlDDL;
                }
                
                // 处理无长度无NULL约束的
                const before4 = mysqlDDL;
                mysqlDDL = mysqlDDL.replace(/Add\s+\[([^\]]+)\]\s+([A-Z]+)/gi, 'ADD COLUMN `$1` $2 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci');
                if (before4 !== mysqlDDL) {
                    logger.info(`Matched pattern 4 (no length, no NULL): ${mysqlDDL}`);
                    return mysqlDDL;
                }
                
                logger.info(`No ADD COLUMN patterns matched, returning original: ${mysqlDDL}`);
                return mysqlDDL;
            } else if (upperSql.includes('DROP COLUMN')) {
                // 处理DROP COLUMN，移除dbo.前缀
                mysqlDDL = mysqlDDL.replace(/dbo\./gi, '');
                mysqlDDL = mysqlDDL.replace(/DROP\s+\[([^\]]+)\]/gi, 'DROP COLUMN `$1`');
                mysqlDDL = mysqlDDL.replace(/DROP\s+COLUMN\s+([^\s]+)/gi, 'DROP COLUMN `$1`');
                return mysqlDDL;
            } else if (upperSql.includes('MODIFY') || upperSql.includes('ALTER COLUMN')) {
                mysqlDDL = mysqlDDL.replace(/ALTER COLUMN/gi, 'MODIFY COLUMN');
                return mysqlDDL;
            } else if (upperSql.includes('SET (LOCK_ESCALATION') || upperSql.includes('SET (LOCK ESCALATION')) {
                // SQL Server specific: ALTER TABLE ... SET (LOCK ESCALATION = TABLE)
                // MySQL doesn't support this, so we'll skip it (return null to indicate no-op)
                logger.info(`Skipping SQL Server specific LOCK ESCALATION statement: ${sqlCommand}`);
                return null;
            }
            
            return mysqlDDL;
            
        } else if (operation === 'DDL_DROP_TABLE') {
            // Convert DROP TABLE statements
            return sqlCommand
                .replace(/\[dbo\]\./gi, '')
                .replace(/\[([^\]]+)\]/g, '`$1`');
        }
        
        // Default: try basic conversion
        return sqlCommand
            .replace(/\[dbo\]\./gi, '')
            .replace(/\[([^\]]+)\]/g, '`$1`');
            
    } catch (error) {
        logger.error(`Error converting DDL: ${error.message}`);
        return null;
    }
}

// Process Advanced Online Report sync data (with appId/storeId instead of MachineName)
async function processAdvancedSyncData(data) {
    try {
        const { appId, storeId, tableName, operation, recordData, timestamp, syncId, businessType } = data;
        
        // Get database configuration for this app from license service
        const database = await licenseService.getDatabaseByStoreAndApp(storeId, appId);
        if (!database) {
            return {
                success: false,
                error: `No database configuration found for Store ${storeId}, App ${appId}`
            };
        }

        // Convert recordData (XML) to usable format
        let parsedData;
        try {
            if (typeof recordData === 'string' && recordData.trim().startsWith('<')) {
                // Parse XML data (similar to legacy sync)
                parsedData = await parseXMLData(recordData);
            } else {
                parsedData = recordData;
            }
        } catch (parseError) {
            return {
                success: false,
                error: `Failed to parse record data: ${parseError.message}`
            };
        }

        // Process the sync operation
        logger.info(`Processing sync: tableName=${tableName}, operation=${operation}, businessType=${businessType}`);
        const result = await executeAdvancedSyncOperation(database, tableName, operation, parsedData, businessType);
        
        return {
            success: true,
            result: result,
            message: `Advanced sync completed for ${tableName}`
        };
        
    } catch (error) {
        logger.error('Error processing advanced sync data:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Execute sync operation for Advanced Online Report
async function executeAdvancedSyncOperation(database, tableName, operation, data, businessType) {
    try {
        // 如果data是XML字符串，需要解析为对象
        let parsedData;
        if (typeof data === 'string' && data.trim().startsWith('<')) {
            // 简单的XML解析 - 提取数据
            parsedData = await parseXMLToObject(data);
        } else {
            parsedData = data;
        }

        // 检查解析后的数据是否有效
        if (!parsedData || typeof parsedData !== 'object') {
            throw new Error('Invalid data format for sync operation');
        }
        
        // Don't override businessType parameter - it's already passed in correctly
        // const businessType = data.businessType; // REMOVED - this was shadowing the parameter
        
        switch (operation.toUpperCase()) {
            case 'INSERT':
                // 构建INSERT语句，使用列名和值分别指定
                const insertColumns = Object.keys(parsedData).map(key => `\`${key}\``).join(', ');
                const insertPlaceholders = Object.keys(parsedData).map(() => '?').join(', ');
                const insertValues = Object.values(parsedData);
                
                // 使用 INSERT ... ON DUPLICATE KEY UPDATE 来处理重复键冲突
                const updateColumns = Object.keys(parsedData)
                    .map(key => `\`${key}\` = VALUES(\`${key}\`)`)
                    .join(', ');
                
                const insertSql = `INSERT INTO ${tableName} (${insertColumns}) VALUES (${insertPlaceholders}) ON DUPLICATE KEY UPDATE ${updateColumns}`;
       
                
                return await dbManager.executeQuery(database, insertSql, insertValues);
            case 'UPDATE':
                // 根据业务类型和表名确定主键字段（支持联合主键）
                let whereFields = [];
                let whereValues = [];
                
              
                
                if (tableName === 'SalesDetail') {
                    if (businessType === 'hospitality') {
                        // Hospitality: SalesDetail表使用OrderNo + ItemCode作为联合WHERE条件
                        if (!('OrderNo' in parsedData) && !('old_OrderNo' in parsedData)) {
                            throw new Error('No OrderNo or old_OrderNo found for UPDATE operation on SalesDetail (Hospitality)');
                        }
                        if (!('ItemCode' in parsedData) && !('old_ItemCode' in parsedData)) {
                            throw new Error('No ItemCode or old_ItemCode found for UPDATE operation on SalesDetail (Hospitality)');
                        }
                        whereFields = ['OrderNo', 'ItemCode'];
                        whereValues = [
                            parsedData.old_OrderNo || parsedData.OrderNo,
                            parsedData.old_ItemCode || parsedData.ItemCode
                        ];
                    } else if (businessType === 'retail') {
                        // Retail: SalesDetail表使用InvoiceNo + StockId作为联合WHERE条件
                        if (!('InvoiceNo' in parsedData) && !('old_InvoiceNo' in parsedData)) {
                            throw new Error('No InvoiceNo or old_InvoiceNo found for UPDATE operation on SalesDetail (Retail)');
                        }
                        if (!('StockId' in parsedData) && !('old_StockId' in parsedData)) {
                            throw new Error('No StockId or old_StockId found for UPDATE operation on SalesDetail (Retail)');
                        }
                        whereFields = ['InvoiceNo', 'StockId'];
                        whereValues = [
                            parsedData.old_InvoiceNo || parsedData.InvoiceNo,
                            parsedData.old_StockId || parsedData.StockId
                        ];
                    }
                } else if (tableName === 'Sales') {
                    if (businessType === 'hospitality') {
                        // Hospitality: Sales表使用OrderNo作为WHERE条件
                        if (!('OrderNo' in parsedData) && !('old_OrderNo' in parsedData)) {
                            throw new Error('No OrderNo or old_OrderNo found for UPDATE operation on Sales (Hospitality)');
                        }
                        whereFields = ['OrderNo'];
                        whereValues = [parsedData.old_OrderNo || parsedData.OrderNo];
                    } else if (businessType === 'retail') {
                        // Retail: Sales表使用InvoiceNo作为WHERE条件
                        if (!('InvoiceNo' in parsedData) && !('old_InvoiceNo' in parsedData)) {
                            throw new Error('No InvoiceNo or old_InvoiceNo found for UPDATE operation on Sales (Retail)');
                        }
                        whereFields = ['InvoiceNo'];
                        whereValues = [parsedData.old_InvoiceNo || parsedData.InvoiceNo];
                    } else {
                        // 默认情况：尝试使用InvoiceNo，如果没有则使用OrderNo
                        if (('InvoiceNo' in parsedData) || ('old_InvoiceNo' in parsedData)) {
                            whereFields = ['InvoiceNo'];
                            whereValues = [parsedData.old_InvoiceNo || parsedData.InvoiceNo];
                        } else if (('OrderNo' in parsedData) || ('old_OrderNo' in parsedData)) {
                            whereFields = ['OrderNo'];
                            whereValues = [parsedData.old_OrderNo || parsedData.OrderNo];
                        } else {
                            throw new Error('No InvoiceNo or OrderNo found for UPDATE operation on Sales');
                        }
                    }
                } 
                else if (tableName === 'StockItems') {
                    if (businessType === 'retail') {
                        // Retail: StockItems表使用StockId作为WHERE条件
                        if (!('StockId' in parsedData) && !('old_StockId' in parsedData)) {
                            throw new Error('No StockId or old_StockId found for UPDATE operation on StockItems (Retail)');
                        }
                        whereFields = ['StockId'];
                        whereValues = [parsedData.old_StockId || parsedData.StockId];
                    }
                } else if (tableName === 'MenuItem') {
                    if (businessType === 'hospitality') {
                        // Hospitality: MenuItem表使用ItemCode作为WHERE条件
                        if (!('ItemCode' in parsedData) && !('old_ItemCode' in parsedData)) {
                            throw new Error('No ItemCode or old_ItemCode found for UPDATE operation on MenuItem (Hospitality)');
                        }
                        whereFields = ['ItemCode'];
                        whereValues = [parsedData.old_ItemCode || parsedData.ItemCode];
                    }
                }
                else if (tableName === 'SubMenuLinkDetail') {
                    if (businessType === 'hospitality') {
                        // Hospitality: SubMenuLinkDetail表使用ItemCode作为WHERE条件
                        if (!('ItemCode' in parsedData) && !('old_ItemCode' in parsedData)) {
                            throw new Error('No ItemCode or old_ItemCode found for UPDATE operation on SubMenuLinkDetail (Hospitality)');
                        }
                        whereFields = ['ItemCode'];
                        whereValues = [parsedData.old_ItemCode || parsedData.ItemCode];
                    }
                }
                else if (tableName === 'PaymentReceived') {
                    if (businessType === 'hospitality') {
                        // Hospitality: PaymentReceived表使用OrderNo + Id作为联合WHERE条件
                        if (!('OrderNo' in parsedData) && !('old_OrderNo' in parsedData)) {
                            throw new Error('No OrderNo or old_OrderNo found for UPDATE operation on PaymentReceived (Hospitality)');
                        }
                        if (!('Id' in parsedData) && !('old_Id' in parsedData)) {
                            throw new Error('No Id or old_Id found for UPDATE operation on PaymentReceived (Hospitality)');
                        }
                        whereFields = ['OrderNo', 'Id'];
                        whereValues = [
                            parsedData.old_OrderNo || parsedData.OrderNo,
                            parsedData.old_Id || parsedData.Id
                        ];
                    } else if (businessType === 'retail') {
                        // Retail: PaymentReceived表使用InvoiceNo + Id作为联合WHERE条件
                        if (!('InvoiceNo' in parsedData) && !('old_InvoiceNo' in parsedData)) {
                            throw new Error('No InvoiceNo or old_InvoiceNo found for UPDATE operation on PaymentReceived (Retail)');
                        }
                        if (!('Id' in parsedData) && !('old_Id' in parsedData)) {
                            throw new Error('No Id or old_Id found for UPDATE operation on PaymentReceived (Retail)');
                        }
                        whereFields = ['InvoiceNo', 'Id'];
                        whereValues = [
                            parsedData.old_InvoiceNo || parsedData.InvoiceNo,
                            parsedData.old_Id || parsedData.Id
                        ];
                    }
                }
                else if (tableName === 'Payment') {
                    // Payment表使用Payment字段作为主键（零售和酒店通用）
                    if (!('Payment' in parsedData) && !('old_Payment' in parsedData)) {
                        throw new Error('No Payment or old_Payment found for UPDATE operation on Payment');
                    }
                    whereFields = ['Payment'];
                    whereValues = [parsedData.old_Payment || parsedData.Payment];
                }
                
              
              
                else {
                    // 其他表尝试使用id字段
                    if (parsedData.id || parsedData.old_id) {
                        whereFields = ['id'];
                        whereValues = [parsedData.old_id || parsedData.id];
                    } else {
                        throw new Error(`No primary key found for UPDATE operation on ${tableName}`);
                    }
                }
                
                // 构建SET子句，用反引号包裹列名以避免保留字冲突
                const setClause = Object.keys(parsedData)
                    .filter(key => !key.startsWith('old_')) // 排除old_前缀的字段
                    .map(key => `\`${key}\` = ?`)
                    .join(', ');
                
                if (!setClause) {
                    throw new Error('No fields to update');
                }
                
                const updateValues = Object.keys(parsedData)
                    .filter(key => !key.startsWith('old_'))
                    .map(key => parsedData[key]);
                
                // 构建WHERE子句（支持联合条件）
                if (whereFields.length === 0) {
                    throw new Error(`No WHERE condition defined for UPDATE operation on ${tableName} with businessType=${businessType}`);
                }
                
                const whereClause = whereFields
                    .map(field => `\`${field}\` = ?`)
                    .join(' AND ');
                
                const sql = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
              
                
                return await dbManager.executeQuery(database, sql, [...updateValues, ...whereValues]);
            case 'DELETE':
                // 根据业务类型和表名确定主键字段（支持联合主键）
                let deleteFields = [];
                let deleteValues = [];
                
              
                
                if (tableName === 'SalesDetail') {
                    if (businessType === 'hospitality') {
                        // Hospitality: SalesDetail表使用OrderNo + ItemCode作为联合WHERE条件
                        if (!('OrderNo' in parsedData)) {
                            throw new Error('No OrderNo found for DELETE operation on SalesDetail (Hospitality)');
                        }
                        if (!('ItemCode' in parsedData)) {
                            throw new Error('No ItemCode found for DELETE operation on SalesDetail (Hospitality)');
                        }
                        deleteFields = ['OrderNo', 'ItemCode'];
                        deleteValues = [parsedData.OrderNo, parsedData.ItemCode];
                    } else if (businessType === 'retail') {
                        // Retail: SalesDetail表使用InvoiceNo + StockId作为联合WHERE条件
                        if (!('InvoiceNo' in parsedData)) {
                            throw new Error('No InvoiceNo found for DELETE operation on SalesDetail (Retail)');
                        }
                        if (!('StockId' in parsedData)) {
                            throw new Error('No StockId found for DELETE operation on SalesDetail (Retail)');
                        }
                        deleteFields = ['InvoiceNo', 'StockId'];
                        deleteValues = [parsedData.InvoiceNo, parsedData.StockId];
                    }
                } else if (tableName === 'Sales') {
                    if (businessType === 'hospitality') {
                        // Hospitality: Sales表使用OrderNo作为WHERE条件
                        if (!('OrderNo' in parsedData)) {
                            throw new Error('No OrderNo found for DELETE operation on Sales (Hospitality)');
                        }
                        deleteFields = ['OrderNo'];
                        deleteValues = [parsedData.OrderNo];
                    } else if (businessType === 'retail') {
                        // Retail: Sales表使用InvoiceNo作为WHERE条件
                        if (!('InvoiceNo' in parsedData)) {
                            throw new Error('No InvoiceNo found for DELETE operation on Sales (Retail)');
                        }
                        deleteFields = ['InvoiceNo'];
                        deleteValues = [parsedData.InvoiceNo];
                    }
                } else if (tableName === 'MenuItem') {
                    // MenuItem表使用ItemCode作为主键
                    if (!('ItemCode' in parsedData)) {
                        throw new Error('No ItemCode found for DELETE operation on MenuItem');
                    }
                    deleteFields = ['ItemCode'];
                    deleteValues = [parsedData.ItemCode];
                } else if (tableName === 'StockItems') {
                    // StockItems表使用StockId作为主键
                    if (!('StockId' in parsedData)) {
                        throw new Error('No StockId found for DELETE operation on StockItems');
                    }
                    deleteFields = ['StockId'];
                    deleteValues = [parsedData.StockId];
                } else if (tableName === 'PaymentReceived') {
                    if (businessType === 'hospitality') {
                        // Hospitality: PaymentReceived表使用OrderNo + Id作为联合WHERE条件
                        if (!('OrderNo' in parsedData)) {
                            throw new Error('No OrderNo found for DELETE operation on PaymentReceived (Hospitality)');
                        }
                        if (!('Id' in parsedData)) {
                            throw new Error('No Id found for DELETE operation on PaymentReceived (Hospitality)');
                        }
                        deleteFields = ['OrderNo', 'Id'];
                        deleteValues = [parsedData.OrderNo, parsedData.Id];
                    } else if (businessType === 'retail') {
                        // Retail: PaymentReceived表使用InvoiceNo + Id作为联合WHERE条件
                        if (!('InvoiceNo' in parsedData)) {
                            throw new Error('No InvoiceNo found for DELETE operation on PaymentReceived (Retail)');
                        }
                        if (!('Id' in parsedData)) {
                            throw new Error('No Id found for DELETE operation on PaymentReceived (Retail)');
                        }
                        deleteFields = ['InvoiceNo', 'Id'];
                        deleteValues = [parsedData.InvoiceNo, parsedData.Id];
                    }
                } else if (tableName === 'Payment') {
                    // Payment表使用Payment字段作为主键（零售和酒店通用）
                    if (!('Payment' in parsedData)) {
                        throw new Error('No Payment found for DELETE operation on Payment');
                    }
                    deleteFields = ['Payment'];
                    deleteValues = [parsedData.Payment];
                } else {
                    // 其他表尝试使用id字段
                    if ('id' in parsedData) {
                        deleteFields = ['id'];
                        deleteValues = [parsedData.id];
                    } else {
                        throw new Error(`No primary key found for DELETE operation on ${tableName}`);
                    }
                }
                
                // 构建WHERE子句（支持联合条件）
                if (deleteFields.length === 0) {
                    throw new Error(`No WHERE condition defined for DELETE operation on ${tableName} with businessType=${businessType}`);
                }
                
                const deleteWhereClause = deleteFields
                    .map(field => `\`${field}\` = ?`)
                    .join(' AND ');
                
                const deleteSql = `DELETE FROM ${tableName} WHERE ${deleteWhereClause}`;
              
                return await dbManager.executeQuery(database, deleteSql, deleteValues);
            default:
                throw new Error(`Unsupported operation: ${operation}`);
        }
    } catch (error) {
        logger.error(`Error in executeAdvancedSyncOperation: ${error.message}`);
        throw error;
    }
}

// Parse XML data to object
async function parseXMLToObject(xmlString) {
    try {
        // 简单的XML解析 - 提取标签内容
        const result = {};
        
        // 检查是否有新旧数据对比（UPDATE操作）
        if (xmlString.includes('<new>') && xmlString.includes('<old>')) {
            // 提取new部分的数据
            const newMatch = xmlString.match(/<new>(.*?)<\/new>/s);
            if (newMatch) {
                const newData = newMatch[1];
                const tagRegex = /<([^>]+)>([^<]*)<\/\1>/g;
                let match;
                
                while ((match = tagRegex.exec(newData)) !== null) {
                    const tagName = match[1];
                    const tagValue = match[2].trim();
                    
                    if (tagValue && tagValue !== '') {
                        result[tagName] = tagValue;
                    }
                }
            }
            
            // 提取old部分的数据（用于WHERE条件）
            const oldMatch = xmlString.match(/<old>(.*?)<\/old>/s);
            if (oldMatch) {
                const oldData = oldMatch[1];
                const tagRegex = /<([^>]+)>([^<]*)<\/\1>/g;
                let match;
                
                while ((match = tagRegex.exec(oldData)) !== null) {
                    const tagName = match[1];
                    const tagValue = match[2].trim();
                    
                    if (tagValue && tagValue !== '') {
                        result[`old_${tagName}`] = tagValue;
                    }
                }
            }
        } else {
            // 普通XML解析
            const tagRegex = /<([^>]+)>([^<]*)<\/\1>/g;
            let match;
            
            while ((match = tagRegex.exec(xmlString)) !== null) {
                const tagName = match[1];
                const tagValue = match[2].trim();
                
                if (tagValue && tagValue !== '') {
                    result[tagName] = tagValue;
                }
            }
        }
        
     
        return result;
        
    } catch (error) {
        logger.error(`Error parsing XML: ${error.message}`);
        throw new Error(`Failed to parse XML data: ${error.message}`);
    }
}

// Parse XML data (placeholder - you may need to implement based on your XML format)
async function parseXMLData(xmlString) {
    // This is a simplified parser - you may need a proper XML parser
    // For now, just return the XML string as is
    return xmlString;
}

const app = express();
const server = http.createServer(app);

// Socket.io configuration with defaults
const socketConfig = {
    pingTimeout: parseInt(process.env.SOCKETIO_PING_TIMEOUT) || 60000,
    pingInterval: parseInt(process.env.SOCKETIO_PING_INTERVAL) || 25000,
    upgradeTimeout: parseInt(process.env.SOCKETIO_UPGRADE_TIMEOUT) || 10000,
    maxHttpBufferSize: parseInt(process.env.SOCKETIO_MAX_BUFFER_SIZE) || 10000000
};
const fullSyncConfig = {
    batchSize: parseInt(process.env.FULL_SYNC_BATCH_SIZE) || 1000,
    timeout: parseInt(process.env.FULL_SYNC_TIMEOUT) || 300000,
    retryAttempts: parseInt(process.env.FULL_SYNC_RETRY_ATTEMPTS) || 3
};

const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    pingTimeout: socketConfig.pingTimeout || 600000, // 10分钟
    pingInterval: socketConfig.pingInterval || 120000, // 2分钟
    upgradeTimeout: socketConfig.upgradeTimeout || 60000, // 1分钟
    maxHttpBufferSize: socketConfig.maxHttpBufferSize || 2000000000, // 2GB
    allowEIO3: true,
    transports: ['websocket', 'polling'],
    allowUpgrades: true,
    httpCompression: false, // 禁用压缩避免超时
    connectTimeout: 300000, // 5分钟连接超时
    timeout: 600000 // 10分钟总超时
});

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
const updateRoutes = require('./routes/updateRoutes');
app.use('/api/updates', updateRoutes);

// Basic health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'OK', 
        timestamp: new Date().toISOString(),
        service: 'syncdb-server'
    });
});

// Status endpoint
app.get('/status', async (req, res) => {
    try {
        const dbConfig = require('./config/database.json');
        const appCount = Object.keys(dbConfig.machineMapping).length;
        
        res.json({
            status: 'running',
            connectedClients: io.sockets.sockets.size,
            configuredApps: appCount,
            uptime: process.uptime(),
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Socket.io connection handling
const clientFullSyncStatus = new Map(); // Track full sync status for each client

io.on('connection', (socket) => {
  
    
    // Handle client identification with license validation
    socket.on('identify', async (data) => {
        try {
            const { storeId, appId, serviceType } = data;
            
            logger.info(`Client identification attempt: ${socket.id} with AppId: ${appId}, ServiceType: ${serviceType}`);
            
            // Check if this is an Advanced Online Report client
            if (serviceType === 'advanced_online_report') {
                if (!storeId || !appId) {
                    logger.warn(`Advanced Report client ${socket.id} missing credentials: storeId=${storeId}, appId=${appId}`);
                    socket.emit('license_error', { 
                        success: false, 
                        error: 'Missing storeId or appId for Advanced Online Report',
                        code: 400
                    });
                    socket.disconnect(true);
                    return;
                }
                
            
                const licenseValidation = await licenseService.validateAdvancedReportLicense(storeId, appId);
                
                if (!licenseValidation.isValid || licenseValidation.isExpired) {
                    logger.warn(`License validation failed for Store ${storeId}: ${licenseValidation.error || 'License expired'}`);
                    
                    // Send license expired/invalid message with 410 status code equivalent
                    socket.emit('license_expired', { 
                        success: false, 
                        error: 'License expired or invalid',
                        message: 'Advanced Online Report license has expired or is invalid',
                        code: 410,
                        storeInfo: licenseValidation.storeInfo
                    });
                    
                    // Disconnect the client
                    setTimeout(() => {
                        socket.disconnect(true);
                    }, 1000); // Give client time to receive the message
                    return;
                }
                
                // License is valid, store license info in socket
                socket.licenseInfo = licenseValidation.storeInfo;
                
            }
            
            // Standard identification process
            socket.storeId = storeId;
            socket.appId = appId;
            socket.serviceType = serviceType;
        
        // Initialize full sync status
        clientFullSyncStatus.set(socket.id, false);
        

            socket.emit('identified', { 
                success: true, 
                message: 'Client identified successfully',
                licenseInfo: socket.licenseInfo 
            });
            
        } catch (error) {
            logger.error(`Error during client identification: ${error.message}`);
            socket.emit('identification_error', { 
                success: false, 
                error: 'Identification failed',
                message: error.message 
            });
            socket.disconnect(true);
        }
    });

    // Handle sync data from client
    socket.on('sync_data', async (data) => {
        try {
            
            // Check if this is from Advanced Online Report (has appId and storeId)
            if (data.appId && data.storeId) {
                // Process Advanced Online Report sync data
                const result = await processAdvancedSyncData(data);
                
                // Send response back to client
                if (result.success) {
                    socket.emit('sync_response', {
                        syncId: data.syncId,
                        success: true,
                        result: result.result,
                        timestamp: new Date().toISOString()
                    });
                    
                } else {
                    socket.emit('sync_response', {
                        syncId: data.syncId,
                        success: false,
                        error: result.error,
                        timestamp: new Date().toISOString()
                    });
                    logger.error(`Advanced sync failed for ${data.syncId}: ${result.error}`);
                }
            } else {
                // Process legacy sync data (with MachineName)
                const result = await syncService.processSyncData(data);
                
                // Send response back to client
                if (result.success) {
                    socket.emit('sync_response', {
                        syncId: data.SyncId,
                        success: true,
                        result: result.result,
                        timestamp: new Date().toISOString()
                    });
                 
                } else {
                    socket.emit('sync_response', {
                        syncId: data.SyncId,
                        success: false,
                        error: result.error,
                        timestamp: new Date().toISOString()
                    });
                    logger.error(`Sync failed for ${data.SyncId}: ${result.error}`);
                }
            }
            
        } catch (error) {
            logger.error(`Sync processing failed: ${error.message}`, error);
            
            // Check if error is due to table not existing
            if (error.message.startsWith('TABLE_NOT_EXISTS:')) {
                const tableName = error.message.split(':')[1];
                
                // Request table schema from client
                socket.emit('request_table_schema', {
                    tableName: tableName,
                    originalSyncId: data.SyncId
                });
            } else {
                // Send error response for other errors
                socket.emit('sync_response', {
                    syncId: data.SyncId,
                    success: false,
                    error: error.message,
                    timestamp: new Date().toISOString()
                });
            }
        }
    });

    // Handle table schema response from client
    socket.on('table_schema_response', async (data) => {
        try {
            const { tableName, schema, originalSyncId } = data;
            
            // Validate client identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('table_schema_error', {
                    error: 'Client not properly identified',
                    originalSyncId: originalSyncId
                });
                return;
            }
            
            // Get database for this app from license service
            const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                socket.emit('table_schema_error', {
                    error: `No database configuration found for Store ${socket.storeId}`,
                    originalSyncId: originalSyncId
                });
                return;
            }
            
            // Create table with provided schema
            await syncService.createTableWithSchema(database, tableName, schema);
            
            // Request full data sync for the newly created table
           
            socket.emit('request_full_data_sync', {
                tableName: tableName,
                originalSyncId: originalSyncId,
                batchSize: fullSyncConfig.batchSize // Use config-defined batch size
            });
            
        } catch (error) {
            logger.error(`Failed to create table from schema: ${error.message}`, error);
            socket.emit('table_created', {
                tableName: data.tableName,
                success: false,
                error: error.message,
                originalSyncId: data.originalSyncId
            });
        }
    });

    // Handle full data sync response from client
    socket.on('full_data_sync_response', async (data) => {
        try {
            const { tableName, data: tableData, originalSyncId, isLastBatch, currentBatch, totalBatches, totalRows } = data;
            
            const batchInfo = totalBatches ? `batch ${currentBatch}/${totalBatches}` : 'batch';
            
            // Validate client identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('full_data_sync_error', {
                    error: 'Client not properly identified',
                    originalSyncId: data.originalSyncId
                });
                return;
            }
            
            // Get database for this app from license service
            const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                socket.emit('full_data_sync_error', {
                    error: `No database configuration found for Store ${socket.storeId}`,
                    originalSyncId: data.originalSyncId
                });
                return;
            }
            
            // Insert batch data
            let successCount = 0;
            let errorCount = 0;
            let insertCount = 0;
            let updateCount = 0;
            let skipCount = 0;
            
            for (const row of tableData) {
                try {
                    const result = await syncService.handleInsert(database, tableName, row, true); // Pass isFullSync=true
                    successCount++;
                    
                    if (result.operation === 'INSERT') {
                        insertCount++;
                    } else if (result.operation === 'UPDATE') {
                        updateCount++;
                    } else if (result.operation === 'SKIP') {
                        skipCount++;
                    }
                } catch (error) {
                    logger.error(`Failed to insert row in ${tableName}: ${error.message}`);
                    errorCount++;
                }
            }
            
            // Send batch progress update
            socket.emit('full_data_sync_progress', {
                tableName: tableName,
                currentBatch: currentBatch,
                totalBatches: totalBatches,
                batchSuccessCount: successCount,
                batchErrorCount: errorCount,
                batchInsertCount: insertCount,
                batchUpdateCount: updateCount,
                batchSkipCount: skipCount,
                originalSyncId: originalSyncId
            });
            
            
            // If this is the last batch, send completion notification
            if (isLastBatch) {
                // Calculate total stats (this is simplified - in production you'd track cumulative stats)
                socket.emit('full_data_sync_complete', {
                    tableName: tableName,
                    success: true,
                    message: `Full data sync completed: ${totalRows} total rows processed`,
                    originalSyncId: originalSyncId,
                    totalRows: totalRows
                });
                
                // Also send success response for the original sync request
                socket.emit('sync_response', {
                    syncId: originalSyncId,
                    success: true,
                    result: { message: 'Included in full table sync', rowsAffected: 1 },
                    timestamp: new Date().toISOString()
                });
                
                logger.info(`Full data sync completed for ${tableName}: ${totalRows} total rows`);
            }
            
        } catch (error) {
            logger.error(`Failed to process full data sync batch: ${error.message}`, error);
            socket.emit('full_data_sync_complete', {
                tableName: data.tableName,
                success: false,
                error: error.message,
                originalSyncId: data.originalSyncId
            });
        }
    });

    // Handle batch sync data
    socket.on('batch_sync', async (batchData) => {
        try {
            
            const results = [];
            for (const data of batchData) {
                const result = await syncService.processSyncData(data);
                results.push({
                    syncId: data.syncId,
                    success: result.success,
                    error: result.error || null,
                    result: result.result || null
                });
            }
            
            socket.emit('batch_sync_response', {
                results,
                timestamp: new Date().toISOString()
            });
            
            
        } catch (error) {
            logger.error(`Error processing batch sync: ${error.message}`, error);
            socket.emit('batch_sync_response', {
                error: 'Internal server error',
                timestamp: new Date().toISOString()
            });
        }
    });

    // Handle ping/pong for connection health
    socket.on('ping', () => {
        socket.emit('pong', { timestamp: new Date().toISOString() });
    });

    // Handle DDL operations (schema changes)
    socket.on('sync_ddl_operation', async (data) => {
        try {
            const { appId, storeId, tableName, operation, sqlCommand, timestamp, syncId } = data;
            

            // Get database configuration for this app from license service
            const database = await licenseService.getDatabaseByStoreAndApp(storeId, appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${storeId}, AppId: ${appId}`);
                socket.emit('ddl_sync_error', {
                    syncId,
                    error: `No database configuration found for Store ${storeId}`
                });
                return;
            }

            // Convert SQL Server DDL to MySQL DDL
            const mysqlDDL = await convertSqlServerDDLToMySQL(sqlCommand, tableName, operation);
            
            if (!mysqlDDL) {
               
                // Send success response for skipped operations
                socket.emit('ddl_sync_success', {
                    syncId,
                    message: 'DDL operation skipped (SQL Server specific)',
                    skipped: true
                });
                return;
            }

            // Execute the converted MySQL DDL

            try {
                const result = await dbManager.executeQuery(database, mysqlDDL);
                logger.info(`✅ DDL operation completed successfully for ${tableName}: ${operation}`);
                socket.emit('ddl_sync_success', {
                    syncId,
                    message: 'DDL operation completed successfully'
                });
            } catch (ddlError) {
                logger.error(`❌ DDL operation failed for ${tableName}: ${ddlError.message}`);
                socket.emit('ddl_sync_error', {
                    syncId,
                    error: `DDL execution failed: ${ddlError.message}`
                });
            }

        } catch (error) {
            logger.error(`Error processing DDL operation from ${socket.id}:`, error);
            socket.emit('ddl_sync_error', {
                syncId: data.syncId,
                error: error.message
            });
        }
    });

    // Handle client disconnect
    socket.on('disconnect', (reason) => {
       
        // Clean up client data
        clientFullSyncStatus.delete(socket.id);
    });

    // Handle errors
    socket.on('error', (error) => {
        logger.error(`Socket error for ${socket.id}: ${error.message}`, error);
    });

    // Handle table verification and initial sync requests
    socket.on('verify_and_sync_table', async (data) => {
        try {
            const { tableName, batchSize } = data;
            
            // Validate that client has storeId and appId from identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('verify_and_sync_error', {
                    error: 'Client not properly identified. Missing storeId or appId.'
                });
                return;
            }
            
            // Get database name from license service using storeId and appId
            const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                socket.emit('verify_and_sync_error', {
                    error: `No database configuration found for Store ${socket.storeId} with App ${socket.appId}`
                });
                return;
            }
            
            
            // Check if table exists
            const exists = await dbManager.tableExists(database, tableName);
            
            let needsSync = false;
            let rowCount = 0;
            
            if (exists) {
                // Table exists, check if it has data
                try {
                    const countQuery = `SELECT COUNT(*) as count FROM \`${tableName}\``;
                    const result = await dbManager.executeQuery(database, countQuery);
                    rowCount = result.rows[0].count;
                    
                    
                    // Only request CSV sync if table is completely empty
                    if (rowCount === 0) {
                        needsSync = true;
                      
                    } else {
                        needsSync = false;
                        
                    }
                } catch (countError) {
                    logger.error(`Error counting rows in table ${tableName}: ${countError.message}`);
                    needsSync = false; // If we can't count, assume it has data
                }
            }
            
            // Send response back to client
            socket.emit('verify_and_sync_response', {
                tableName: tableName,
                exists: exists,
                needsSync: needsSync,
                rowCount: rowCount,
                useCSVSync: needsSync // Add flag to indicate CSV sync should be used
            });
            
            // If table exists and needs sync, request CSV bulk sync instead of initial data
            if (exists && needsSync) {
                
                // Add a small delay to ensure the verify response is processed first
                setTimeout(() => {
                    socket.emit('csv_bulk_sync_request', {
                        tableName: tableName,
                        appId: socket.appId
                    });
                }, 100);
            }
            
        } catch (error) {
            logger.error(`Table verification and sync error: ${error.message}`, error);
            socket.emit('verify_and_sync_response', {
                tableName: data.tableName || 'unknown',
                exists: false,
                needsSync: false,
                error: error.message
            });
        }
    });

    // Handle table creation from schema requests
    socket.on('create_table_from_schema', async (data) => {
        try {
            const { tableName, schema, isInitialSync, databaseType } = data;
            
            // Validate client identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('table_creation_error', {
                    error: 'Client not properly identified',
                    tableName: tableName
                });
                return;
            }
            
            // Get database configuration for this app from license service
            const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                socket.emit('table_creation_error', {
                    error: `No database configuration found for Store ${socket.storeId}`,
                    tableName: tableName
                });
                return;
            }
            
            // Create table using the schema
            await syncService.createTableWithSchema(database, tableName, schema, databaseType);
            
            // Send success response back to client
            socket.emit('table_created', {
                tableName: tableName,
                success: true
            });
            
            
            // If this is for initial sync, request CSV bulk sync after table creation
            if (isInitialSync) {
                
                // Wait a bit for the client to process the table_created event
                setTimeout(() => {
                    socket.emit('csv_bulk_sync_request', {
                        tableName: tableName,
                        appId: socket.appId
                    });
                }, 500);
            }
            
            // Comment out the old initial sync request
            /*
            if (isInitialSync) {
                logger.info(`Requesting initial data sync for newly created table ${tableName} from ${machineName}`);
                
                // Wait a bit for the client to process the table_created event
                setTimeout(() => {
                    socket.emit('initial_sync_data_request', {
                        tableName: tableName,
                        batchSize: fullSyncConfig.batchSize // Use config batch size
                    });
                }, 500);
            }
            */
            
        } catch (error) {
            logger.error(`Table creation error: ${error.message}`);
            socket.emit('table_created', {
                tableName: data.tableName || 'unknown',
                success: false,
                error: error.message
            });
        }
    });

    // Handle initial sync data response from client
    socket.on('initial_sync_data_response', async (data) => {
        try {
            const { tableName, data: tableData, currentBatch, totalBatches, totalRows, isLastBatch } = data;
            
            const batchInfo = totalBatches ? `batch ${currentBatch}/${totalBatches}` : 'batch';
            
            // Validate client identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('initial_sync_error', {
                    error: 'Client not properly identified',
                    tableName: tableName
                });
                return;
            }
            
            // Get database for this app from license service
            const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                socket.emit('initial_sync_error', {
                    error: `No database configuration found for Store ${socket.storeId}`,
                    tableName: tableName
                });
                return;
            }
            
            // Insert batch data
            let successCount = 0;
            let errorCount = 0;
            let insertCount = 0;
            let updateCount = 0;
            let skipCount = 0;
            
            for (const row of tableData) {
                try {
                    const result = await syncService.handleInsert(database, tableName, row, true); // Pass isFullSync=true
                    successCount++;
                    
                    if (result.operation === 'INSERT') {
                        insertCount++;
                    } else if (result.operation === 'UPDATE') {
                        updateCount++;
                    } else if (result.operation === 'SKIP') {
                        skipCount++;
                    }
                } catch (error) {
                    logger.error(`Failed to insert row in ${tableName}: ${error.message}`);
                    errorCount++;
                }
            }
            
            // Send batch progress update (no originalSyncId needed for initial sync)
            socket.emit('initial_sync_progress', {
                tableName: tableName,
                currentBatch: currentBatch,
                totalBatches: totalBatches,
                batchSuccessCount: successCount,
                batchErrorCount: errorCount,
                batchInsertCount: insertCount,
                batchUpdateCount: updateCount,
                batchSkipCount: skipCount
            });
            
            
            // If this is the last batch, send completion notification
            if (isLastBatch) {
                socket.emit('initial_sync_complete', {
                    tableName: tableName,
                    success: true,
                    message: `Initial data sync completed: ${totalRows} total rows processed`,
                    totalRows: totalRows
                });
                
                logger.info(`Initial data sync completed for ${tableName}: ${totalRows} total rows`);
            }
            
        } catch (error) {
            logger.error(`Failed to process initial sync batch: ${error.message}`, error);
            socket.emit('initial_sync_complete', {
                tableName: data.tableName,
                success: false,
                error: error.message
            });
        }
    });

    // Handle force sync requests - drop all tables
    socket.on('force_sync_request', async (data) => {
        try {
            const { action } = data;
            
            // Validate client identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('force_sync_error', {
                    error: 'Client not properly identified'
                });
                return;
            }
            
            if (action === 'drop_all_tables') {
                // Get database configuration for this app from license service
                const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
                if (!database) {
                    logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                    socket.emit('force_sync_error', {
                        error: `No database configuration found for Store ${socket.storeId}`
                    });
                    return;
                }
                
                // Tables to drop
                const tablesToDrop = ['SalesDetail', 'StockItems'];
                
                for (const tableName of tablesToDrop) {
                    try {
                        // Check if table exists before trying to drop
                        const exists = await dbManager.tableExists(database, tableName);
                        if (exists) {
                            await dbManager.executeQuery(database, `DROP TABLE \`${tableName}\``);
                            logger.info(`Dropped table ${tableName} for ${machineName}`);
                        } else {
                            logger.info(`Table ${tableName} does not exist for ${machineName}, skipping`);
                        }
                    } catch (error) {
                        logger.warn(`Failed to drop table ${tableName} for ${machineName}: ${error.message}`);
                        // Continue with other tables even if one fails
                    }
                }
                
                // Send success response
                socket.emit('force_sync_response', {
                    success: true,
                    message: 'All tables dropped successfully',
                    droppedTables: tablesToDrop
                });
                
                logger.info(`Force sync completed for ${machineName}: dropped ${tablesToDrop.length} tables`);
            } else {
                socket.emit('force_sync_response', {
                    success: false,
                    error: `Unknown action: ${action}`
                });
            }
            
        } catch (error) {
            logger.error(`Force sync error: ${error.message}`, error);
            socket.emit('force_sync_response', {
                success: false,
                error: error.message
            });
        }
    });

    // CSV chunk storage
    const csvChunkStorage = new Map();

    // Handle CSV bulk upload start (for chunked uploads)
    socket.on('csv_bulk_upload_start', (data) => {
        const { tableName, fileName, totalChunks, fileSizeBytes, rowCount } = data;
        const chunkKey = `${socket.appId}_${fileName}`;
        
        
        csvChunkStorage.set(chunkKey, {
            appId: socket.appId,
            tableName,
            fileName,
            totalChunks,
            fileSizeBytes,
            rowCount,
            chunks: new Map(),
            startTime: Date.now()
        });
    });

    // Handle CSV bulk upload chunk
    socket.on('csv_bulk_upload_chunk', async (data) => {
        const { tableName, fileName, chunkIndex, totalChunks, chunkContent, isLastChunk } = data;
        const chunkKey = `${socket.appId}_${fileName}`;
        
        const uploadInfo = csvChunkStorage.get(chunkKey);
        if (!uploadInfo) {
            logger.error(`No upload info found for chunked upload: ${fileName}`);
            return;
        }
        
        // Store chunk
        uploadInfo.chunks.set(chunkIndex, chunkContent);
        
        // If all chunks received, reassemble and process
        if (uploadInfo.chunks.size === totalChunks) {
            
            try {
                // Create file path
                const fs = require('fs');
                const path = require('path');
                const uploadsDir = path.join(__dirname, 'uploads');
                await require('fs').promises.mkdir(uploadsDir, { recursive: true });
                const filePath = path.join(uploadsDir, fileName);
                
                // Create write stream
                const writeStream = fs.createWriteStream(filePath);
                
                // Write chunks sequentially
                for (let i = 0; i < totalChunks; i++) {
                    const chunk = uploadInfo.chunks.get(i);
                    if (!chunk) {
                        logger.error(`Missing chunk ${i} for ${fileName}`);
                        throw new Error(`Missing chunk ${i}`);
                    }
                    
                    // Decode chunk and write to file
                    const chunkBuffer = Buffer.from(chunk, 'base64');
                    writeStream.write(chunkBuffer);
                    logger.debug(`Wrote chunk ${i}, size: ${chunkBuffer.length} bytes`);
                }
                
                // Close stream and wait for completion
                await new Promise((resolve, reject) => {
                    writeStream.end(() => {
                        logger.info(`File written successfully: ${filePath}`);
                        resolve();
                    });
                    writeStream.on('error', reject);
                });
                
                // Verify file size and content
                const stats = await require('fs').promises.stat(filePath);
                
                // Verify file integrity
                const fileContent = await require('fs').promises.readFile(filePath, 'utf8');
                const lines = fileContent.split('\n');
                
                // Clean up chunk storage
                csvChunkStorage.delete(chunkKey);
                
                // Now we don't need to pass file content, just notify success
                socket.emit('csv_bulk_upload_response', {
                    fileName: uploadInfo.fileName,
                    success: true,
                    message: 'Chunked upload completed',
                    importing: true
                });
                
                
                // Start import process directly without passing through handleCSVBulkUpload
                setTimeout(async () => {
                    try {
                        const importResult = await syncService.processSingleCSVFile(
                            socket.storeId,
                            uploadInfo.appId, 
                            uploadInfo.tableName, 
                            uploadInfo.fileName,
                            (progress) => {
                                socket.emit('csv_bulk_import_progress', {
                                    tableName: uploadInfo.tableName,
                                    fileName: progress.fileName,
                                    processedRows: progress.processedRows,
                                    message: progress.message
                                });
                            }
                        );
                        
                        socket.emit('csv_file_import_complete', {
                            fileName: uploadInfo.fileName,
                            tableName: uploadInfo.tableName,
                            success: importResult.success,
                            processedRows: importResult.processedRows,
                            error: importResult.error,
                            message: importResult.success ? 
                                `File ${uploadInfo.fileName} processed successfully - ${importResult.processedRows} rows imported` :
                                `File ${uploadInfo.fileName} processing failed: ${importResult.error}`
                        });
                        
                        
                    } catch (importError) {
                        logger.error(`CSV file processing failed: ${uploadInfo.fileName} - ${importError.message}`, importError);
                        socket.emit('csv_file_import_complete', {
                            fileName: uploadInfo.fileName,
                            tableName: uploadInfo.tableName,
                            success: false,
                            error: importError.message,
                            processedRows: 0,
                            message: `File ${uploadInfo.fileName} processing failed: ${importError.message}`
                        });
                    }
                }, 2000);
            } catch (error) {
                logger.error(`Failed to process chunked upload: ${error.message}`);
                socket.emit('csv_bulk_upload_response', {
                    fileName: uploadInfo.fileName,
                    success: false,
                    error: error.message,
                    message: 'Chunked upload failed'
                });
            }
        }
    });

    // Handle clear database tables request
    socket.on('clear_database_tables', async (data) => {
        try {
            const { machineName, action, timestamp } = data;
            
            
            // Validate client identification
            if (!socket.storeId || !socket.appId) {
                logger.error(`Client ${socket.id} missing store or app identification`);
                socket.emit('clear_database_response', {
                    success: false,
                    error: 'Client missing store or app identification'
                });
                return;
            }

            // Get database for this app from license service
            const database = await licenseService.getDatabaseByStoreAndApp(socket.storeId, socket.appId);
            if (!database) {
                logger.error(`No database found for StoreId: ${socket.storeId}, AppId: ${socket.appId}`);
                socket.emit('clear_database_response', {
                    success: false,
                    error: `No database configuration found for Store ${socket.storeId}`
                });
                return;
            }

            // Define tables to clear (include Sales table, PaymentReceived and Payment for both retail and hospitality)
            const tablesToClear = ['SalesDetail', 'StockItems', 'Sales', 'MenuItem', 'SubMenuLinkDetail', 'PaymentReceived', 'Payment'];


            // Clear each table
            const connection = await dbManager.getConnection();
            try {
                await connection.beginTransaction();

                for (const tableName of tablesToClear) {
                    try {
                        // Check if table exists first
                        const [tableExists] = await connection.execute(
                            'SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?',
                            [database, tableName]
                        );

                        if (tableExists[0].count > 0) {
                            // Disable foreign key checks temporarily
                            await connection.execute('SET FOREIGN_KEY_CHECKS = 0');
                            
                            // Drop the table
                            await connection.execute(`DROP TABLE IF EXISTS \`${database}\`.\`${tableName}\``);
                            
                            // Re-enable foreign key checks
                            await connection.execute('SET FOREIGN_KEY_CHECKS = 1');
                            
                        } else {
                            logger.info(`ℹ️ Table ${database}.${tableName} does not exist, skipping`);
                        }
                    } catch (tableError) {
                        logger.error(`Error dropping table ${tableName}: ${tableError.message}`);
                        // Continue with other tables even if one fails
                    }
                }

                await connection.commit();
                
                
                socket.emit('clear_database_response', {
                    success: true,
                    message: `Successfully cleared ${tablesToClear.length} tables`,
                    clearedTables: tablesToClear,
                    database: database
                });

            } catch (error) {
                await connection.rollback();
                throw error;
            } finally {
                connection.release();
            }

        } catch (error) {
            logger.error(`Error clearing database tables: ${error.message}`);
            socket.emit('clear_database_response', {
                success: false,
                error: error.message
            });
        }
    });

    // Handle CSV bulk upload
    socket.on('csv_bulk_upload', async (data) => {
        try {
            const { tableName, fileName, fileContent, fileSizeBytes, rowCount } = data;
            
            
            // Handle the CSV file upload
            const result = await syncService.handleCSVBulkUpload(
                socket.appId, 
                tableName, 
                fileName, 
                fileContent, 
                fileSizeBytes, 
                rowCount
            );
            
            if (!result.success) {
                // Send upload error response
                socket.emit('csv_bulk_upload_response', {
                    fileName: fileName,
                    success: false,
                    error: result.error,
                    message: result.message || 'Upload failed'
                });
                return;
            }
            
            
            // Immediately process this single CSV file
            try {
                
                // Send processing start notification
                socket.emit('csv_bulk_upload_response', {
                    fileName: fileName,
                    success: true,
                    message: 'File received, starting import...',
                    importing: true
                });
                
                // Process the single CSV file import
                const importResult = await syncService.processSingleCSVFile(
                    socket.storeId,
                    socket.appId, 
                    tableName, 
                    fileName,
                    (progress) => {
                        // Send progress updates
                        socket.emit('csv_bulk_import_progress', {
                            tableName: tableName,
                            fileName: progress.fileName,
                            processedRows: progress.processedRows,
                            message: progress.message
                        });
                    }
                );
                
                // Send completion notification for this file
                socket.emit('csv_file_import_complete', {
                    fileName: fileName,
                    tableName: tableName,
                    success: importResult.success,
                    processedRows: importResult.processedRows,
                    error: importResult.error,
                    message: importResult.success ? 
                        `File ${fileName} processed successfully - ${importResult.processedRows} rows imported` :
                        `File ${fileName} processing failed: ${importResult.error}`
                });
                
                
            } catch (importError) {
                logger.error(`CSV file processing failed: ${fileName} - ${importError.message}`, importError);
                socket.emit('csv_file_import_complete', {
                    fileName: fileName,
                    tableName: tableName,
                    success: false,
                    error: importError.message,
                    processedRows: 0,
                    message: `File ${fileName} processing failed: ${importError.message}`
                });
            }
            
        } catch (error) {
            logger.error(`CSV bulk upload error: ${error.message}`, error);
            socket.emit('csv_bulk_upload_response', {
                fileName: data.fileName || 'unknown',
                success: false,
                error: error.message,
                message: 'Upload failed'
            });
        }
    });
});

// Server startup
const PORT = parseInt(process.env.PORT) || 3031;
const HOST = process.env.HOST || '0.0.0.0';

server.listen(PORT, HOST, async () => {
    logger.info(`Server started on ${HOST}:${PORT}`);
    logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
    
    // Initialize license service
    try {
        const initialized = await licenseService.initialize();
        if (initialized) {
            logger.info('License service initialized successfully');
        } else {
            logger.warn('License service initialization failed - license validation will not work');
        }
    } catch (error) {
        logger.error('Error initializing license service:', error);
    }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, shutting down gracefully');
    
    // Close Socket.io server
    io.close(() => {
        logger.info('Socket.io server closed');
    });
    
    // Close database connections
    await dbManager.closeAll();
    
    // Close license service
    await licenseService.close();
    
    // Close HTTP server
    server.close(() => {
        logger.info('HTTP server closed');
        process.exit(0);
    });
});

process.on('SIGINT', async () => {
    
    // Close Socket.io server
    io.close(() => {
        logger.info('Socket.io server closed');
    });
    
    // Close database connections
    await dbManager.closeAll();
    
    // Close license service
    await licenseService.close();
    
    // Close HTTP server
    server.close(() => {
        logger.info('HTTP server closed');
        process.exit(0);
    });
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception:', error);
    process.exit(1);
});

module.exports = app; 