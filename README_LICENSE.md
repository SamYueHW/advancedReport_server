# Advanced Online Report License Validation

本文档说明如何设置Advanced Online Report的许可证验证功能。

## 配置步骤

### 1. 安装依赖
```bash
npm install
```

### 2. 创建环境配置文件
复制 `env.example` 为 `.env` 并修改数据库连接信息：

```bash
cp env.example .env
```

编辑 `.env` 文件：
```ini
# Database configuration for license validation
ONLINE_REPORT_DB_HOST=localhost
ONLINE_REPORT_DB_PORT=1433
ONLINE_REPORT_DB_NAME=online_report
ONLINE_REPORT_DB_USER=Enrich
ONLINE_REPORT_DB_PASSWORD=84150011@Enrich

# Server configuration
PORT=3031
NODE_ENV=development

# Logging
LOG_LEVEL=info
```

### 3. 数据库表结构
确保 `online_report` 数据库中有 `stores` 表，包含以下字段：

```sql
CREATE TABLE stores (
    StoreId NVARCHAR(50) NOT NULL,
    StoreName NVARCHAR(255),
    AdvancedReportAppId NVARCHAR(255),
    AdvancedReportLicenseExpire DATETIME,
    CreatedAt DATETIME DEFAULT GETDATE(),
    UpdatedAt DATETIME DEFAULT GETDATE(),
    PRIMARY KEY (StoreId)
);
```

### 4. 启动服务器
```bash
npm start
```

## 功能说明

### Socket连接时的许可证验证
当Flutter客户端连接到Socket.IO服务器时：

1. 客户端发送 `identify` 事件，包含：
   - `machineName`: 机器名
   - `storeId`: 商店ID
   - `appId`: Advanced Report App ID  
   - `serviceType`: 'advanced_online_report'

2. 服务器验证许可证：
   - 查询 `online_report.stores` 表
   - 检查 `StoreId` 和 `AdvancedReportAppId` 是否匹配
   - 检查 `AdvancedReportLicenseExpire` 是否过期

3. 验证结果：
   - **许可证有效**: 发送 `identified` 事件，连接继续
   - **许可证过期**: 发送 `license_expired` 事件（410状态码），然后断开连接
   - **许可证无效**: 发送 `license_error` 事件，然后断开连接

### Flutter端处理
当收到许可证过期事件时，Flutter应用会：

1. 停止Advanced Online Report服务
2. 将配置文件中的 `License` 设置为 `0`
3. 记录错误日志

### 启动时许可证检查
- Advanced Online Report服务启动时会检查配置中的 `license` 字段
- 如果 `license != 1`，服务将拒绝启动
- 这与Online Report的行为保持一致

## 测试许可证过期

可以通过修改数据库中的过期时间来测试：

```sql
-- 设置许可证已过期（用于测试）
UPDATE stores 
SET AdvancedReportLicenseExpire = '2020-01-01 00:00:00' 
WHERE StoreId = '239';

-- 设置许可证有效（用于正常使用）  
UPDATE stores 
SET AdvancedReportLicenseExpire = '2025-12-31 23:59:59' 
WHERE StoreId = '239';
```

## 故障排除

### 1. 许可证服务数据库连接失败
检查 `.env` 文件中的数据库配置是否正确。

### 2. 找不到商店记录
确保 `stores` 表中有对应的 `StoreId` 和 `AdvancedReportAppId` 记录。

### 3. Flutter客户端连接被拒绝
检查客户端发送的 `storeId` 和 `appId` 是否与数据库中的记录匹配。
