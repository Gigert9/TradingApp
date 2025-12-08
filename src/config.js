import dotenv from 'dotenv';
dotenv.config();

function parseSymbols(raw) {
  if (!raw) return [];
  return String(raw)
    .split(',')
    .map(s => s.trim().toUpperCase())
    .filter(Boolean);
}

const settings = {
  apcaApiKeyId:
    process.env.APCA_API_KEY_ID || process.env['APCA-API-KEY-ID'] || '',
  apcaApiSecretKey:
    process.env.APCA_API_SECRET_KEY || process.env['APCA-API-SECRET-KEY'] || '',
  apcaApiBaseUrl:
    process.env.APCA_API_BASE_URL || 'https://paper-api.alpaca.markets',

  mongodbUri:
    process.env.MONGODB_URI || 'mongodb://localhost:27017/tradingapp',
  mongodbDb: process.env.MONGODB_DB || 'tradingapp',

  alpacaDataFeed: process.env.ALPACA_DATA_FEED || 'iex',
  symbols: parseSymbols(process.env.SYMBOLS || '')
};

Object.defineProperty(settings, 'isPaper', {
  get() {
    return String(settings.apcaApiBaseUrl || '').includes('paper-api');
  }
});

export { settings, parseSymbols };
