const express = require('express');
const serverless = require('serverless-http');
const multer = require('multer');
const { parse } = require('csv-parse');
const XLSX = require('xlsx');
const OpenAI = require('openai');
const winston = require('winston');
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
  { name: 'Creation Date', validation: 'datetime' },
  { name: 'Payment Status', validation: 'enum:paid,pending,cancelled' },
  { name: 'Online Order Number', validation: 'string' },
  { name: 'Recipient', validation: 'string' },
  { name: 'Declared Value', validation: 'decimal' },
  { name: 'Invoice', validation: 'string' },
  { name: 'Phone number', validation: 'phone number' },
  { name: 'Tracking Number', validation: 'string' },
  { name: 'Notes', validation: 'string' },
  { name: 'Delivery Status', validation: 'enum:pending,in_transit,delivered,failed' },
  { name: 'Destination Country', validation: 'string' }
];

const targetFieldNames = targetFields.map(field => field.name);

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
function transformData(data, mapping, originalHeaders) {
  logger.info('Starting data transformation', { 
    recordCount: data.length,
    mappingFields: Object.entries(mapping).length
  });

  const reverseMapping = {};
  for (const [oldKey, newKey] of Object.entries(mapping)) {
    reverseMapping[newKey] = oldKey;
  }
  
  // Створюємо шаблон з усіма цільовими полями
  const template = {};
  targetFieldNames.forEach(field => {
    template[field] = '';
  });
  
  try {
    // Трансформуємо дані, зберігаючи всі поля незалежно від їх заповненості
    const transformedData = data.map((row, index) => {
      const newRow = {...template};
      
      for (const targetField of targetFieldNames) {
        const sourceField = reverseMapping[targetField];
        if (sourceField && row[sourceField] !== undefined) {
          newRow[targetField] = row[sourceField]?.toString().trim() || '';
        }
      }
      
      return newRow;
    });

    // Повертаємо всі цільові поля як emptyFields
    // const emptyFields = targetFields.map(field => ({
    //   name: field.name,
    //   validation: field.validation
    // }));
    const emptyFields = targetFieldNames;

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

    logger.info('Data transformation completed', { 
      transformedCount: transformedData.length,
      unmappedCount: unmappedColumns.length,
      emptyFieldsCount: emptyFields.length
    });
    
    return {
      transformedData,
      unmappedColumns,
      emptyFields
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

// Основний роут для обробки файлів
router.post('/parse-file', uploadMiddleware, async (req, res) => {
  const startTime = Date.now();
  
  try {
    if (!req.file) {
      logger.error('No file uploaded');
      throw new Error('Файл не було завантажено');
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
      throw new Error('Непідтримуваний тип файлу. Підтримуються лише CSV та XLSX формати.');
    }

    headers = Object.keys(data[0]);
    const mapping = await getColumnMapping(headers);
    const { transformedData, unmappedColumns, emptyFields } = transformData(data, mapping, headers);

    const processingTime = Date.now() - startTime;
    logger.info('Request completed successfully', { 
      processingTimeMs: processingTime,
      recordsProcessed: transformedData.length,
      unmappedCount: unmappedColumns.length,
      emptyFieldsCount: emptyFields.length
    });

    res.json({
      data: transformedData,
      unmappedColumns,
      emptyFields
    });

  } catch (error) {
    logger.error('Request failed', { 
      error: error.message,
      stack: error.stack,
      processingTimeMs: Date.now() - startTime
    });

    res.status(500).json({
      error: error.message,
      details: 'Виникла помилка при обробці файлу'
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