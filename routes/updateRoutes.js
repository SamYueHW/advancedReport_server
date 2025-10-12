const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const crypto = require('crypto');
const logger = require('../utils/logger');

const router = express.Router();

// 添加文本解析中间件用于XML内容
const textParser = express.text({ type: 'application/xml', limit: '1mb' });

// 配置文件上传
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        const uploadPath = path.join(__dirname, '../uploads/updates');
        // 确保目录存在
        require('fs').mkdirSync(uploadPath, { recursive: true });
        cb(null, uploadPath);
    },
    filename: function (req, file, cb) {
        // 保持原文件名
        cb(null, file.originalname);
    }
});

const upload = multer({ 
    storage: storage,
    limits: {
        fileSize: 500 * 1024 * 1024 // 500MB 限制
    },
    fileFilter: function (req, file, cb) {
        // 允许的文件类型
        const allowedTypes = ['.exe', '.zip', '.msi', '.7z'];
        const ext = path.extname(file.originalname).toLowerCase();
        
        if (allowedTypes.includes(ext)) {
            cb(null, true);
        } else {
            cb(new Error('不支持的文件类型'), false);
        }
    }
});

// 基本认证中间件
const basicAuth = (req, res, next) => {
    const authHeader = req.headers.authorization;
    
    if (!authHeader || !authHeader.startsWith('Basic ')) {
        res.set('WWW-Authenticate', 'Basic realm="Update Manager"');
        return res.status(401).json({ error: '需要认证' });
    }
    
    const base64Credentials = authHeader.split(' ')[1];
    const credentials = Buffer.from(base64Credentials, 'base64').toString('ascii');
    const [username, password] = credentials.split(':');
    
    // 从配置或环境变量中获取认证信息
    const validUsername = process.env.UPDATE_ADMIN_USERNAME || 'admin';
    const validPassword = process.env.UPDATE_ADMIN_PASSWORD || 'updatepassword123';
    
    if (username === validUsername && password === validPassword) {
        next();
    } else {
        res.set('WWW-Authenticate', 'Basic realm="Update Manager"');
        return res.status(401).json({ error: '认证失败' });
    }
};

// 存储更新信息的文件路径
const updateInfoPath = path.join(__dirname, '../uploads/updates/update.xml');
const updateMetadataPath = path.join(__dirname, '../uploads/updates/metadata.json');

// 获取更新信息 (XML格式)
router.get('/check', async (req, res) => {
    try {
        logger.info('Client checking for updates');
        
        // 检查是否存在更新信息文件
        try {
            const xmlContent = await fs.readFile(updateInfoPath, 'utf8');
            res.set('Content-Type', 'application/xml');
            res.send(xmlContent);
            logger.info('Update information sent to client');
        } catch (error) {
            if (error.code === 'ENOENT') {
                // 如果没有更新文件，返回空的更新信息
                const emptyUpdate = `<?xml version="1.0" encoding="UTF-8"?>
<item>
  <version>1.0.0.0</version>
  <url></url>
  <mandatory>false</mandatory>
</item>`;
                res.set('Content-Type', 'application/xml');
                res.send(emptyUpdate);
                logger.info('No updates available, sent empty update info');
            } else {
                throw error;
            }
        }
    } catch (error) {
        logger.error('Error checking for updates:', error);
        res.status(500).json({ error: '检查更新失败' });
    }
});

// 下载更新文件
router.get('/download/:filename', async (req, res) => {
    try {
        const filename = req.params.filename;
        const filePath = path.join(__dirname, '../uploads/updates', filename);
        
        // 验证文件是否存在
        try {
            await fs.access(filePath);
        } catch (error) {
            return res.status(404).json({ error: '文件未找到' });
        }
        
        logger.info(`Client downloading update file: ${filename}`);
        
        // 设置响应头
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.setHeader('Content-Type', 'application/octet-stream');
        
        // 发送文件
        res.sendFile(filePath);
        
    } catch (error) {
        logger.error('Error downloading update file:', error);
        res.status(500).json({ error: '下载失败' });
    }
});

// 上传更新文件 (需要认证)
router.post('/upload', basicAuth, upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: '没有文件被上传' });
        }
        
        const file = req.file;
        logger.info(`Update file uploaded: ${file.filename}, size: ${file.size} bytes`);
        
        // 计算文件校验和
        const fileBuffer = await fs.readFile(file.path);
        const sha256Hash = crypto.createHash('sha256').update(fileBuffer).digest('hex').toUpperCase();
        const md5Hash = crypto.createHash('md5').update(fileBuffer).digest('hex').toUpperCase();
        
        // 保存文件元数据
        const metadata = {
            filename: file.filename,
            originalName: file.originalname,
            size: file.size,
            uploadDate: new Date().toISOString(),
            checksums: {
                sha256: sha256Hash,
                md5: md5Hash
            }
        };
        
        await fs.writeFile(updateMetadataPath, JSON.stringify(metadata, null, 2));
        
        res.json({
            message: '文件上传成功',
            filename: file.filename,
            size: file.size,
            checksums: metadata.checksums
        });
        
    } catch (error) {
        logger.error('Error uploading update file:', error);
        res.status(500).json({ error: '上传失败: ' + error.message });
    }
});

// 上传更新信息XML (需要认证) - 修改版本
router.post('/xml', basicAuth, textParser, async (req, res) => {
    try {
        // 使用text parser后，XML内容在req.body中
        const xmlContent = req.body;
        
        logger.info('Received XML content length:', xmlContent ? xmlContent.length : 0);
        logger.debug('XML content preview:', xmlContent ? xmlContent.substring(0, 200) : 'empty');
        
        if (!xmlContent || typeof xmlContent !== 'string' || !xmlContent.trim()) {
            logger.warn('Empty or invalid XML content received');
            return res.status(400).json({ error: 'XML内容为空或无效' });
        }
        
        // 验证XML格式
        try {
            const parseString = require('xml2js').parseString;
            await new Promise((resolve, reject) => {
                parseString(xmlContent, (err, result) => {
                    if (err) {
                        logger.warn('XML parsing failed:', err.message);
                        reject(err);
                    } else {
                        logger.info('XML validation successful');
                        resolve(result);
                    }
                });
            });
        } catch (error) {
            return res.status(400).json({ error: 'XML格式无效: ' + error.message });
        }
        
        // 确保目录存在
        const uploadDir = path.dirname(updateInfoPath);
        await fs.mkdir(uploadDir, { recursive: true });
        
        // 保存XML文件
        await fs.writeFile(updateInfoPath, xmlContent, 'utf8');
        
        logger.info('Update XML information saved successfully');
        
        res.json({
            message: '更新信息保存成功',
            timestamp: new Date().toISOString(),
            xmlLength: xmlContent.length
        });
        
    } catch (error) {
        logger.error('Error saving update XML:', error);
        res.status(500).json({ error: '保存更新信息失败: ' + error.message });
    }
});

// 获取已上传的文件列表 (需要认证)
router.get('/files', basicAuth, async (req, res) => {
    try {
        const uploadDir = path.join(__dirname, '../uploads/updates');
        
        try {
            const files = await fs.readdir(uploadDir);
            const fileList = [];
            
            for (const file of files) {
                if (file === 'update.xml' || file === 'metadata.json') continue;
                
                const filePath = path.join(uploadDir, file);
                const stats = await fs.stat(filePath);
                
                fileList.push({
                    name: file,
                    size: stats.size,
                    uploadDate: stats.mtime,
                    downloadUrl: `/api/updates/download/${file}`
                });
            }
            
            res.json({ files: fileList });
            
        } catch (error) {
            if (error.code === 'ENOENT') {
                res.json({ files: [] });
            } else {
                throw error;
            }
        }
        
    } catch (error) {
        logger.error('Error listing update files:', error);
        res.status(500).json({ error: '获取文件列表失败' });
    }
});

// 删除更新文件 (需要认证)
router.delete('/files/:filename', basicAuth, async (req, res) => {
    try {
        const filename = req.params.filename;
        const filePath = path.join(__dirname, '../uploads/updates', filename);
        
        // 检查文件是否存在
        try {
            await fs.access(filePath);
        } catch (error) {
            return res.status(404).json({ error: '文件未找到' });
        }
        
        // 删除文件
        await fs.unlink(filePath);
        logger.info(`Update file deleted: ${filename}`);
        
        res.json({ message: '文件删除成功' });
        
    } catch (error) {
        logger.error('Error deleting update file:', error);
        res.status(500).json({ error: '删除文件失败' });
    }
});

// 获取当前更新信息状态 (需要认证)
router.get('/status', basicAuth, async (req, res) => {
    try {
        let updateInfo = null;
        let metadata = null;
        
        // 读取更新XML信息
        try {
            const xmlContent = await fs.readFile(updateInfoPath, 'utf8');
            updateInfo = xmlContent;
        } catch (error) {
            // 文件不存在
        }
        
        // 读取文件元数据
        try {
            const metadataContent = await fs.readFile(updateMetadataPath, 'utf8');
            metadata = JSON.parse(metadataContent);
        } catch (error) {
            // 文件不存在
        }
        
        res.json({
            hasUpdateInfo: !!updateInfo,
            hasUpdateFile: !!metadata,
            updateInfo: updateInfo,
            fileMetadata: metadata
        });
        
    } catch (error) {
        logger.error('Error getting update status:', error);
        res.status(500).json({ error: '获取状态失败' });
    }
});

module.exports = router; 