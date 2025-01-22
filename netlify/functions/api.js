const express = require('express');
const serverless = require('serverless-http');
const multer = require('multer');
const { parse } = require('csv-parse');
const XLSX = require('xlsx');
const OpenAI = require('openai');
const winston = require('winston');
const axios = require('axios');
require('dotenv').config();

// Налаштування Winston logger для виводу в консоль та відстеження всіх операцій
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss'
    }),
    winston.format.colorize(),
    winston.format.printf(({ level, message, timestamp, ...metadata }) => {
      let msg = `${timestamp} [${level}]: ${message}`;
      if (Object.keys(metadata).length > 0) {
        msg += ` ${JSON.stringify(metadata)}`;
      }
      return msg;
    })
  ),
  transports: [
    new winston.transports.Console()
  ]
});

const app = express();
const router = express.Router();

router.get('/hello', (req, res) => {
  logger.info('Health check request received');
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Налаштування multer для роботи з буфером замість файлової системи
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
}).single('file');

// CORS middleware з логуванням на рівні router
router.use((req, res, next) => {
  logger.info('Incoming request', {
    method: req.method,
    path: req.path,
    origin: req.headers.origin
  });

  const allowedOrigin = process.env.CORS_ORIGIN;

  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') {
    logger.debug('Handling OPTIONS request');
    return res.sendStatus(200);
  }
  next();
});

// Middleware для обробки завантаження файлів
const uploadMiddleware = (req, res, next) => {
  upload(req, res, (err) => {
    if (err) {
      logger.error('File upload error', { error: err.message });
      return res.status(400).json({ error: 'Помилка завантаження файлу' });
    }
    next();
  });
};

// Конфігурація OpenAI
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// Цільові поля для маппінгу
const targetFields = [
  { name: 'IEW number', validation: 'string' },
  { name: 'Sender type', validation: 'enum:1,2' },
  { name: 'Sender full name', validation: 'string' },
  { name: 'Sender contact name', validation: 'string' },
  { name: 'Sender phones', validation: 'phone' },
  { name: 'Sender email', validation: 'email' },
  { name: 'Sender postcode', validation: 'postcode' },
  { name: 'Sender country', validation: 'string' },
  { name: 'Sender region', validation: 'string' },
  { name: 'Sender city', validation: 'string' },
  { name: 'Sender address', validation: 'string' },
  { name: 'street', validation: 'string' },
  { name: 'house', validation: 'string' },
  { name: 'appartment', validation: 'string' },
  { name: 'Cost (TOP)', validation: 'decimal' },
  { name: 'Currency (TOP)', validation: 'string' },
  { name: 'Receiver type', validation: 'enum:1,2' },
  { name: 'Receiver full name', validation: 'string' },
  { name: 'Receiver contact name', validation: 'string' },
  { name: 'Receiver phones', validation: 'phone' },
  { name: 'Receiver email', validation: 'email' },
  { name: 'Receiver postcode', validation: 'postcode' },
  { name: 'Receiver country', validation: 'string' },
  { name: 'Receiver region', validation: 'string' },
  { name: 'Receiver city', validation: 'string' },
  { name: 'Receiver address', validation: 'string' },
  { name: 'street', validation: 'string' },
  { name: 'house', validation: 'string' },
  { name: 'appartment', validation: 'string' },
  { name: 'IOSS number', validation: 'string' },
  { name: 'Incoterms', validation: 'string' },
  { name: 'Invoice description', validation: 'string' },
  { name: 'Full description', validation: 'string' },
  { name: 'Place description', validation: 'string' },
  { name: 'Actual weight, kg', validation: 'decimal' },
  { name: 'Related order', validation: 'string' },
  { name: 'Receiver postcode', validation: 'postcode' },
  { name: 'Receiver country', validation: 'string' },
  { name: 'Receiver region', validation: 'string' },
  { name: 'Receiver city', validation: 'string' },
  { name: 'Receiver address', validation: 'string' }
];

const targetFieldNames = targetFields.map(field => field.name);

// Function to get product name column from GPT
async function getProductNameColumn(headers) {
  logger.info('Requesting product name column identification from GPT-4', { headers });
  
  const prompt = `I have a CSV/Excel file with the following column headers: ${headers.join(', ')}
  
  I need to identify which column contains product names or product descriptions.
  Respond with a JSON object containing a single key 'productColumn' with the exact header name that represents product names.
  If you're not sure or can't find a product name column, respond with null.`;

  try {
    const completion = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" }
    });

    const result = JSON.parse(completion.choices[0].message.content);
    logger.info('Received product column identification', { result });
    return result.productColumn;
  } catch (error) {
    logger.error('GPT-4 API error during product column identification', { error: error.message });
    throw new Error('Error identifying product name column');
  }
}

// Function to fetch HS code for a product name
async function getHSCode(productName) {
    try {
      // Parse API configuration from environment variable
      const apiConfig = JSON.parse(process.env.HS_CODE_API_CONFIG);
      
      // Make request to HS code API with basic auth
      const response = await axios({
        method: 'post',
        url: `${apiConfig.host}/api/v1/hscodes`,
        data: {
          productName
        },
        auth: {
          username: apiConfig.username,
          password: apiConfig.password
        },
        validateStatus: function (status) {
          return status >= 200 && status < 500; // Don't reject if status is 404
        }
      });
      
      // Return empty string for 404 or any error response
      if (response.status === 404 || !response.data || response.status >= 400) {
        logger.warn('HS code not found or error response', { 
          productName,
          status: response.status 
        });
        return '';
      }
      
    //   logger.warn('HS code response', { 
    //     responseData: response.data
    //   });

      return response.data.data.hsCode || '';
    } catch (error) {
      logger.error('HS code API error', { 
        productName, 
        error: error.message 
      });
      return ''; // Return empty string for any error
    }
  }

// Функція для отримання маппінгу колонок через GPT-4
async function getColumnMapping(headers) {
  logger.info('Requesting column mapping from GPT-4', { headers });
  
  const prompt = `Я маю CSV/Excel файл з наступними заголовками колонок: ${headers.join(', ')}
  
  Мені потрібно зіставити ці заголовки з наступними цільовими полями та їх правилами валідації:
  ${targetFields.map(field => `${field.name} (${field.validation})`).join('\n')}
  
  Надай відповідь у форматі JSON, де ключі - це заголовки з файлу, а значення - це відповідні цільові поля.
  Якщо для якогось заголовка немає відповідного цільового поля, або ти не впевнений у відповідності - пропусти його.
  Поверни тільки ті поля, у відповідності яких ти впевнений на 100%.`;
  

  try {
    const completion = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" }
    });

    const mapping = JSON.parse(completion.choices[0].message.content);
    logger.info('Received column mapping', { mapping });
    return mapping;
  } catch (error) {
    logger.error('GPT-4 API error', { error: error.message });
    throw new Error('Помилка при отриманні маппінгу колонок');
  }
}

// Функція для обробки CSV даних з буфера
async function parseCSVBuffer(buffer) {
  logger.info('Starting CSV parsing from buffer');
  
  return new Promise((resolve, reject) => {
    parse(buffer.toString(), {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      delimiter: ';',
      relax_quotes: true,
      skip_records_with_empty_values: false
    }, (error, data) => {
      if (error) {
        logger.error('CSV parsing error', { error: error.message });
        reject(error);
      } else {
        logger.info('CSV parsing completed', { 
          recordCount: data.length,
          sampleHeaders: Object.keys(data[0])
        });
        resolve(data);
      }
    });
  });
}

// Функція для обробки Excel даних з буфера
function parseExcelBuffer(buffer) {
  logger.info('Starting Excel parsing from buffer');
  
  try {
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet);
    
    logger.info('Excel parsing completed', {
      recordCount: data.length,
      sheetName: workbook.SheetNames[0]
    });
    
    return data;
  } catch (error) {
    logger.error('Excel parsing error', { error: error.message });
    throw error;
  }
}

// Функція для перейменування колонок і забезпечення однакової структури
async function transformData(data, mapping, originalHeaders) {
  logger.info('Starting data transformation with HS code lookup');

  // Get product name column
  const productColumn = await getProductNameColumn(originalHeaders);
  if (!productColumn) {
    logger.warn('No product name column identified');
  }

  // Create reverse mapping and template as before
  const reverseMapping = {};
  for (const [oldKey, newKey] of Object.entries(mapping)) {
    reverseMapping[newKey] = oldKey;
  }
  
  const template = {};
  targetFieldNames.forEach(field => {
    template[field] = '';
  });
  
  try {
    // Track all HS codes for the separate column
    const hsCodesColumn = [];

    // Transform data and fetch HS codes
    const transformedData = await Promise.all(data.map(async (row, index) => {
      const newRow = {...template};
      
      // Map fields as before
      for (const targetField of targetFieldNames) {
        const sourceField = reverseMapping[targetField];
        if (sourceField && row[sourceField] !== undefined) {
          newRow[targetField] = row[sourceField]?.toString().trim() || '';
        }
      }
      
      // Add HS code if product column was found
      if (productColumn && row[productColumn]) {
        const hsCode = await getHSCode(row[productColumn]);
        newRow['HS_Code'] = hsCode || '';
        
        // Add to hsCodesColumn array
        hsCodesColumn.push({
          productName: row[productColumn],
          hsCode: hsCode || '',
          rowIndex: index
        });
        
        logger.info('Retrieved HS code', {
          productName: row[productColumn],
          hsCode,
          rowIndex: index
        });
      }
      
      return newRow;
    }));

    // Get unmapped columns and empty fields as before
    const mappedSourceColumns = new Set(Object.keys(mapping));
    const unmappedColumns = [];
    const uniqueUnmappedColumns = new Set();
    
    data.forEach(row => {
      Object.entries(row).forEach(([columnName, value]) => {
        if (!mappedSourceColumns.has(columnName) && value) {
          uniqueUnmappedColumns.add(columnName);
        }
      });
    });

    const columnRenameMap = {};
    Array.from(uniqueUnmappedColumns).forEach((columnName, index) => {
      columnRenameMap[columnName] = `unmappedColumn${index + 1}`;
    });

    data.forEach(row => {
      Object.entries(row).forEach(([columnName, value]) => {
        if (!mappedSourceColumns.has(columnName) && value) {
          unmappedColumns.push({ [columnRenameMap[columnName]]: value });
        }
      });
    });

    logger.info('Data transformation with HS codes completed', { 
      transformedCount: transformedData.length,
      unmappedCount: unmappedColumns.length,
    });
    
    return {
      transformedData,
      unmappedColumns,
      emptyFields: targetFieldNames,
      productColumn, // Return the identified product column name
      hsCodesColumn // Return the array of HS codes with their corresponding products
    };
  } catch (error) {
    logger.error('Data transformation error', { error: error.message });
    throw error;
  }
}

// Тестовий роут для перевірки працездатності API
router.get('/health', (req, res) => {
  logger.info('Health check request received');
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Modified parse-file endpoint to include the new functionality
router.post('/parse-file', uploadMiddleware, async (req, res) => {
    const startTime = Date.now();
    
    try {
      if (!req.file) {
        logger.error('No file uploaded');
        throw new Error('File was not uploaded');
      }
  
      logger.info('File received', { 
        filename: req.file.originalname,
        size: req.file.size,
        mimetype: req.file.mimetype
      });
  
      let data;
      let headers;
  
      if (req.file.originalname.toLowerCase().endsWith('.csv')) {
        data = await parseCSVBuffer(req.file.buffer);
      } else if (req.file.originalname.toLowerCase().endsWith('.xlsx')) {
        data = parseExcelBuffer(req.file.buffer);
      } else {
        throw new Error('Unsupported file type. Only CSV and XLSX formats are supported.');
      }
  
      headers = Object.keys(data[0]);
      const mapping = await getColumnMapping(headers);
      const { transformedData, unmappedColumns, emptyFields, productColumn, hsCodesColumn } = await transformData(data, mapping, headers);
  
      const processingTime = Date.now() - startTime;
      logger.info('Request completed successfully', { 
        processingTimeMs: processingTime,
        recordsProcessed: transformedData.length,
        unmappedCount: unmappedColumns.length,
        emptyFieldsCount: emptyFields.length,
        productColumn
      });
  
      res.json({
        data: transformedData,
        unmappedColumns,
        emptyFields
        // productColumn
       // hsCodesColumn // Include HS codes in the response
      });
  
    } catch (error) {
      logger.error('Request failed', { 
        error: error.message,
        stack: error.stack,
        processingTimeMs: Date.now() - startTime
      });
  
      res.status(500).json({
        error: error.message,
        details: 'An error occurred while processing the file'
      });
    }
  });
  

// Функція для створення Excel файлу з даних
// Функція для створення Excel файлу з даних
function createExcelBuffer(jsonData) {
  logger.info('Starting Excel file creation', { recordCount: jsonData.length });
  
  try {
    const worksheet = XLSX.utils.json_to_sheet(jsonData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
    
    const excelBuffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    
    logger.info('Excel file creation completed', { 
      sizeBytes: excelBuffer.length
    });
    
    // Конвертуємо buffer в base64 для передачі через Netlify
    return excelBuffer.toString('base64');
  } catch (error) {
    logger.error('Excel file creation error', { error: error.message });
    throw error;
  }
}

// Middleware для парсингу JSON body
const jsonParser = express.json({
  limit: '10mb'
});

// Endpoint для збереження даних з шаблоном
router.post('/parse-file/save/templates', jsonParser, async (req, res) => {
  const startTime = Date.now();
  
  try {
    if (!req.body || !req.body.data) {
      logger.error('No data provided');
      throw new Error('Дані не було надано');
    }

    const { data, templateName } = req.body;
    
    if (!Array.isArray(data)) {
      logger.error('Invalid data format', { receivedType: typeof data });
      throw new Error('Дані мають неправильний формат');
    }
    
    if (!templateName) {
      logger.error('Template name not provided');
      throw new Error('Назва шаблону відсутня');
    }

    logger.info('Processing template save request', { 
      templateName,
      recordCount: data.length
    });

    const base64Data = createExcelBuffer(data);
    
    const processingTime = Date.now() - startTime;
    logger.info('Template save request completed successfully', { 
      processingTimeMs: processingTime,
      templateName,
      recordCount: data.length
    });

    res.json({
      data: base64Data,
      filename: `${templateName}.xlsx`,
      contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    });

  } catch (error) {
    logger.error('Template save request failed', { 
      error: error.message,
      stack: error.stack,
      processingTimeMs: Date.now() - startTime
    });

    res.status(500).json({
      error: error.message,
      details: 'Виникла помилка при створенні Excel файлу з шаблоном'
    });
  }
});

// Endpoint для збереження даних без шаблону
router.post('/parse-file/save', jsonParser, async (req, res) => {
  const startTime = Date.now();
  
  try {
    if (!req.body || !req.body.data) {
      logger.error('No data provided');
      throw new Error('Дані не було надано');
    }

    const { data } = req.body;
    
    if (!Array.isArray(data)) {
      logger.error('Invalid data format', { receivedType: typeof data });
      throw new Error('Дані мають неправильний формат');
    }

    logger.info('Processing save request', { 
      recordCount: data.length
    });

    const base64Data = createExcelBuffer(data);
    
    const processingTime = Date.now() - startTime;
    logger.info('Save request completed successfully', { 
      processingTimeMs: processingTime,
      recordCount: data.length
    });

    res.json({
      data: base64Data,
      filename: 'data.xlsx',
      contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    });

  } catch (error) {
    logger.error('Save request failed', { 
      error: error.message,
      stack: error.stack,
      processingTimeMs: Date.now() - startTime
    });

    res.status(500).json({
      error: error.message,
      details: 'Виникла помилка при створенні Excel файлу'
    });
  }
});

app.use('/api/', router)

// Експортуємо handler для Netlify
exports.handler = serverless(app);