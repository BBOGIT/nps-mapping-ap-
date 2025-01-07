const express = require('express');
const serverless = require('serverless-http');
const multer = require('multer');
const { parse } = require('csv-parse');
const XLSX = require('xlsx');
const OpenAI = require('openai');
const winston = require('winston');
require('dotenv').config();

// Налаштування Winston logger для виводу в консоль
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

// CORS middleware з логуванням
app.use((req, res, next) => {
  logger.info('Incoming request', {
    method: req.method,
    path: req.path,
    origin: req.headers.origin
  });

  const allowedOrigin = process.env.CORS_ORIGIN;

  res.header('Access-Control-Allow-Origin', allowedOrigin);
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') {
    logger.debug('Handling OPTIONS request');
    return res.sendStatus(200);
  }
  next();
});

// Налаштування multer для роботи з буфером замість файлової системи
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
}).single('file');

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
  'Дата створення',
  'Статус оплати',
  'Номер інтернет замовлення',
  'Отримувач',
  'Оголошена вартість',
  'Інвойс',
  'Теги',
  'Трек номер',
  'Нотатки',
  'Статус доставки',
  'Країна отримання'
];

// Функція для отримання маппінгу колонок через GPT-4
async function getColumnMapping(headers) {
  logger.info('Requesting column mapping from GPT-4', { headers });
  
  const prompt = `Я маю CSV/Excel файл з наступними заголовками колонок: ${headers.join(', ')}
  
Мені потрібно зіставити ці заголовки з наступними цільовими полями:
${targetFields.join('\n')}

Надай відповідь у форматі JSON, де ключі - це заголовки з файлу, а значення - це відповідні цільові поля.
Якщо для якогось заголовка немає відповідного цільового поля, або ти не впевнений у відповідності - пропусти його.
Поверни тільки ті поля, у відповідності яких ти впевнений на 100%.`;

  try {
    const completion = await openai.chat.completions.create({
      model: "gpt-4",
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
      delimiter: ';',  // Вказуємо що розділювач - крапка з комою
      relax_quotes: true, // Дозволяємо більш гнучку обробку лапок
      skip_records_with_empty_values: false // Не пропускаємо рядки з пустими значеннями
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
    mappingFields: Object.keys(mapping).length
  });

  // Створюємо зворотній маппінг
  const reverseMapping = {};
  for (const [oldKey, newKey] of Object.entries(mapping)) {
    reverseMapping[newKey] = oldKey;
  }
  
  // Створюємо шаблон об'єкту
  const template = {};
  targetFields.forEach(field => {
    template[field] = '';
  });
  
  try {
    // Спочатку трансформуємо дані як зазвичай
    const initialTransform = data.map((row, index) => {
      const newRow = {...template};
      
      for (const targetField of targetFields) {
        const sourceField = reverseMapping[targetField];
        if (sourceField && row[sourceField] !== undefined) {
          newRow[targetField] = row[sourceField]?.toString().trim() || '';
        }
      }
      
      return newRow;
    });

    // Знаходимо поля, які порожні у всіх записах
    const emptyFields = [];
    targetFields.forEach(field => {
      const hasValue = initialTransform.some(row => row[field] !== '');
      if (!hasValue) {
        emptyFields.push(field);
      }
    });

    // Очищуємо порожні поля з даних
    const transformedData = initialTransform.map(row => {
      const cleanedRow = {};
      Object.entries(row).forEach(([key, value]) => {
        if (!emptyFields.includes(key)) {
          cleanedRow[key] = value;
        }
      });
      return cleanedRow;
    });

    // Збираємо дані про неспівставлені колонки з їх значеннями
    const mappedSourceColumns = new Set(Object.keys(mapping));
    const unmappedColumns = [];
    
    // Створюємо мапу для відстеження унікальних неспівставлених колонок
    const uniqueUnmappedColumns = new Set();
    data.forEach(row => {
      Object.entries(row).forEach(([columnName, value]) => {
        if (!mappedSourceColumns.has(columnName) && value) {
          uniqueUnmappedColumns.add(columnName);
        }
      });
    });

    // Створюємо мапу для перейменування колонок
    const columnRenameMap = {};
    Array.from(uniqueUnmappedColumns).forEach((columnName, index) => {
      columnRenameMap[columnName] = `unmappedColumn${index + 1}`;
    });

    // Збираємо значення з перейменованими колонками
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

// Основний обробник файлів
app.post('/parse-file', uploadMiddleware, async (req, res) => {
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

    // Визначення типу файлу та його парсинг
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

module.exports.handler = serverless(app);