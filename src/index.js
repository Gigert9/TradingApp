import express from 'express';
import apiRouter from './api.js';
import { connectDb, closeDb, getDb } from './db.js';
import { settings } from './config.js';
import Alpaca from '@alpacahq/alpaca-trade-api';
import { getCurrentClock, getTradingCalendar } from './alpacaHelpers.js';
import { syncBars } from './syncBars.js';
import { syncPositions } from './positions.js';
import { syncQuotes } from './syncQuotes.js';
import { syncTrades } from './syncTrades.js';
import { syncOrders } from './syncOrders.js';
import { syncActivities } from './syncActivities.js';
import { buildSnapshots1m } from './jobs/buildSnapshots1m.js';
import { enrichIndicators } from './jobs/enrichIndicators.js';

const PORT = process.env.PORT ? Number(process.env.PORT) : 8000;


async function main() {
  await connectDb();

  let timers = {};

  const db = await getDb();
  try {
    await db.createCollection('startup_events');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection startup_events:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('startup_events').createIndex({ started_at: -1 });
  } catch (e) {
    console.warn('createIndex on startup_events:', e && e.message ? e.message : e);
  }

  // Ensure listeningsymbols collection and unique index on symbol
  try {
    await db.createCollection('listeningsymbols');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection listeningsymbols:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('listeningsymbols').createIndex({ symbol: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on listeningsymbols:', e && e.message ? e.message : e);
  }

  // Seed listeningsymbols from env-configured symbols (settings.symbols)
  try {
    const raw = Array.isArray(settings.symbols) ? settings.symbols : [];
    const seedSymbols = [...new Set(raw.map(s => String(s).trim().toUpperCase()).filter(Boolean))];

    if (seedSymbols.length) {
      const ops = seedSymbols.map(sym => ({
        updateOne: {
          filter: { symbol: sym },
          update: { $setOnInsert: { symbol: sym } },
          upsert: true
        }
      }));
      const res = await db.collection('listeningsymbols').bulkWrite(ops, { ordered: false });
      const upserted = (res && typeof res.upsertedCount === 'number') ? res.upsertedCount : 0;
      if (upserted > 0) {
        console.log('Seeded listeningsymbols with %d symbol(s) from .env', upserted);
      }
    } else {
      console.log('No symbols found in settings to seed listeningsymbols');
    }
  } catch (e) {
    console.warn('Failed to seed listeningsymbols:', e && e.message ? e.message : e);
  }

  // Ensure timeframe bar collections and indexes
  try {
    await db.createCollection('1m_bars');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection 1m_bars:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.createCollection('15m_bars');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection 15m_bars:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.createCollection('1h_bars');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection 1h_bars:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.createCollection('1d_bars');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection 1d_bars:', e && e.message ? e.message : e);
    }
  }
  for (const name of ['1m_bars', '15m_bars', '1h_bars', '1d_bars']) {
    try {
      await db.collection(name).createIndex({ symbol: 1, t: 1 }, { unique: true });
    } catch (e) {
      console.warn(`createIndex on ${name} (symbol+t):`, e && e.message ? e.message : e);
    }
    try {
      await db.collection(name).createIndex({ t: -1 });
    } catch (e) {
      console.warn(`createIndex on ${name} (t desc):`, e && e.message ? e.message : e);
    }
  }

  // Ensure quotes_hist collection and indexes
  try {
    await db.createCollection('quotes_hist');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection quotes_hist:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('quotes_hist').createIndex({ symbol: 1, t: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on quotes_hist (symbol+t):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('quotes_hist').createIndex({ t: -1 });
  } catch (e) {
    console.warn('createIndex on quotes_hist (t desc):', e && e.message ? e.message : e);
  }

  // Ensure trades_hist collection and indexes
  try {
    await db.createCollection('trades_hist');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection trades_hist:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('trades_hist').createIndex({ symbol: 1, t: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on trades_hist (symbol+t):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('trades_hist').createIndex({ t: -1 });
  } catch (e) {
    console.warn('createIndex on trades_hist (t desc):', e && e.message ? e.message : e);
  }

  // Ensure market_snapshots_1m collection and indexes
  try {
    await db.createCollection('market_snapshots_1m');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection market_snapshots_1m:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('market_snapshots_1m').createIndex({ symbol: 1, t: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on market_snapshots_1m (symbol+t):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('market_snapshots_1m').createIndex({ t: -1 });
  } catch (e) {
    console.warn('createIndex on market_snapshots_1m (t desc):', e && e.message ? e.message : e);
  }

  // Ensure orders collection and indexes
  try {
    await db.createCollection('orders');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection orders:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('orders').createIndex({ id: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on orders (id):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('orders').createIndex({ symbol: 1, updated_at: -1 });
  } catch (e) {
    console.warn('createIndex on orders (symbol+updated_at):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('orders').createIndex({ status: 1, symbol: 1 });
  } catch (e) {
    console.warn('createIndex on orders (status+symbol):', e && e.message ? e.message : e);
  }

  // Ensure activities collection and indexes
  try {
    await db.createCollection('activities');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection activities:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('activities').createIndex({ id: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on activities (id):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('activities').createIndex({ paper: 1, t: -1 });
  } catch (e) {
    console.warn('createIndex on activities (paper+t):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('activities').createIndex({ activity_type: 1, symbol: 1, t: -1 });
  } catch (e) {
    console.warn('createIndex on activities (activity_type+symbol+t):', e && e.message ? e.message : e);
  }

  // Ensure positions collections and indexes
  try {
    await db.createCollection('positions_current');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection positions_current:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.createCollection('positions_historical');
  } catch (e) {
    if (!(e && (e.codeName === 'NamespaceExists' || e.code === 48))) {
      console.warn('createCollection positions_historical:', e && e.message ? e.message : e);
    }
  }
  try {
    await db.collection('positions_current').createIndex({ paper: 1, symbol: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on positions_current (paper+symbol):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('positions_current').createIndex({ updated_at: -1 });
  } catch (e) {
    console.warn('createIndex on positions_current (updated_at):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('positions_historical').createIndex({ paper: 1, symbol: 1, snapshot_at: 1 }, { unique: true });
  } catch (e) {
    console.warn('createIndex on positions_historical (paper+symbol+snapshot_at):', e && e.message ? e.message : e);
  }
  try {
    await db.collection('positions_historical').createIndex({ snapshot_at: -1 });
  } catch (e) {
    console.warn('createIndex on positions_historical (snapshot_at):', e && e.message ? e.message : e);
  }

  // Fetch Alpaca market clock (if credentials available)
  let clock = null;
  let alpacaClient = null;
  try {
    if (settings.apcaApiKeyId && settings.apcaApiSecretKey) {
      alpacaClient = new Alpaca({
        keyId: settings.apcaApiKeyId,
        secretKey: settings.apcaApiSecretKey,
        paper: settings.isPaper,
        baseUrl: settings.apcaApiBaseUrl
      });
      clock = await getCurrentClock(alpacaClient);
    } else {
      console.warn('Skipping clock fetch: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('Failed to fetch Alpaca market clock:', e && e.message ? e.message : e);
  }
  // Fetch Alpaca market calendar (today) if credentials available
  let calendar = null;
  try {
    if (alpacaClient) {
      const today = new Date().toISOString().slice(0, 10);
      calendar = await getTradingCalendar(alpacaClient, { start: today, end: today });
    } else if (settings.apcaApiKeyId && settings.apcaApiSecretKey) {
      alpacaClient = new Alpaca({
        keyId: settings.apcaApiKeyId,
        secretKey: settings.apcaApiSecretKey,
        paper: settings.isPaper,
        baseUrl: settings.apcaApiBaseUrl
      });
      const today = new Date().toISOString().slice(0, 10);
      calendar = await getTradingCalendar(alpacaClient, { start: today, end: today });
    } else {
      console.warn('Skipping calendar fetch: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('Failed to fetch Alpaca market calendar:', e && e.message ? e.message : e);
  }
  // Sync historical bars on startup (seed/refresh deltas)
  try {
    if (alpacaClient) {
      await syncBars(alpacaClient, { db: () => db });
      console.log('syncBars completed on startup');
    } else {
      console.warn('Skipping syncBars: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('syncBars on startup failed:', e && e.message ? e.message : e);
  }

  // Enrich indicators after bars are synced, before building snapshots
  try {
    await enrichIndicators(db);
    console.log('enrichIndicators completed on startup');
  } catch (e) {
    console.warn('enrichIndicators on startup failed:', e && e.message ? e.message : e);
  }


  // Sync positions on startup (current + historical snapshot)
  try {
    if (alpacaClient) {
      await syncPositions(alpacaClient, db, { paper: settings.isPaper });
      console.log('syncPositions completed on startup');
    } else {
      console.warn('Skipping syncPositions: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('syncPositions on startup failed:', e && e.message ? e.message : e);
  }

  // Sync historical quotes on startup (last 30 days)
  try {
    if (alpacaClient) {
      await syncQuotes(alpacaClient, db, { daysBack: 30, feed: settings.alpacaDataFeed || 'iex' });
      console.log('syncQuotes (30d) completed on startup');
    } else {
      console.warn('Skipping syncQuotes: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('syncQuotes on startup failed:', e && e.message ? e.message : e);
  }

  // Sync historical trades on startup (last 30 days)
  try {
    if (alpacaClient) {
      await syncTrades(alpacaClient, db, { daysBack: 30, feed: settings.alpacaDataFeed || 'iex' });
      console.log('syncTrades (30d) completed on startup');
    } else {
      console.warn('Skipping syncTrades: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('syncTrades on startup failed:', e && e.message ? e.message : e);
  }

  // Sync orders on startup (last 30 days)
  try {
    if (alpacaClient) {
      await syncOrders(alpacaClient, db, { daysBack: 30 });
      console.log('syncOrders (30d) completed on startup');
    } else {
      console.warn('Skipping syncOrders: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('syncOrders on startup failed:', e && e.message ? e.message : e);
  }

  // Sync account activities on startup (last 30 days)
  try {
    if (alpacaClient) {
      await syncActivities(alpacaClient, db, { daysBack: 30, paper: settings.isPaper });
      console.log('syncActivities (30d) completed on startup');
    } else {
      console.warn('Skipping syncActivities: missing Alpaca credentials');
    }
  } catch (e) {
    console.warn('syncActivities on startup failed:', e && e.message ? e.message : e);
  }

  // Build market snapshots (1m) on startup after bars/quotes/trades are synced
  try {
    await buildSnapshots1m(db, { daysBack: 30, incremental: true });
    console.log('buildSnapshots1m (30d) completed on startup');
  } catch (e) {
    console.warn('buildSnapshots1m on startup failed:', e && e.message ? e.message : e);
  }

  // Periodic refresh timers for positions, portfolio history, bars, quotes, trades, orders
  if (alpacaClient) {
    // Positions: every 15s (existing behavior)
    try {
      timers.pos = setInterval(() => {
        syncPositions(alpacaClient, db, { paper: settings.isPaper }).catch(() => {});
      }, 15000);
    } catch (_) {}


    // Bars every 2 minutes, followed by enrichIndicators
    try {
      timers._barsRunning = false;
      timers.bars = setInterval(async () => {
        if (timers._barsRunning) return;
        timers._barsRunning = true;
        try {
          await syncBars(alpacaClient, { db: () => db });
          await enrichIndicators(db);
        } catch (_) {
          // no-op
        } finally {
          timers._barsRunning = false;
        }
      }, 120000);
    } catch (_) {}

    // Quotes + Trades + market_snapshots_1m every 3 minutes (sequential)
    try {
      timers._quotesPipelineRunning = false;
      timers.quotes = setInterval(async () => {
        if (timers._quotesPipelineRunning) return;
        timers._quotesPipelineRunning = true;
        try {
          await syncQuotes(alpacaClient, db, { daysBack: 30, feed: settings.alpacaDataFeed || 'iex' });
          await syncTrades(alpacaClient, db, { daysBack: 30, feed: settings.alpacaDataFeed || 'iex' });
          await buildSnapshots1m(db, { daysBack: 30, incremental: true });
        } catch (_) {
          // no-op
        } finally {
          timers._quotesPipelineRunning = false;
        }
      }, 180000);
    } catch (_) {}


    // Orders every 15 seconds
    try {
      timers._ordersRunning = false;
      timers.orders = setInterval(async () => {
        if (timers._ordersRunning) return;
        timers._ordersRunning = true;
        try {
          await syncOrders(alpacaClient, db, { daysBack: 30 });
        } catch (_) {
          // no-op
        } finally {
          timers._ordersRunning = false;
        }
      }, 15000);
    } catch (_) {}
  }

  const startupDoc = {
    started_at: new Date(),
    pid: process.pid,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    port: PORT,
    paper: settings.isPaper,
    data_feed: settings.alpacaDataFeed,
    clock,
    calendar,
    module: 'server',
    symbols: settings.symbols || [],
    symbols_count: (settings.symbols || []).length,
    has_apca_credentials: Boolean(settings.apcaApiKeyId && settings.apcaApiSecretKey),
    schedules: alpacaClient
      ? [
          { name: 'positions', interval_ms: 15000 },
          { name: 'bars+enrichIndicators', interval_ms: 120000 },
          { name: 'quotes+trades+buildSnapshots1m', interval_ms: 180000 },
          { name: 'orders', interval_ms: 15000 }
        ]
      : [],
    env: { NODE_ENV: process.env.NODE_ENV || null }
  };
  try {
    await db.collection('startup_events').insertOne(startupDoc);
    console.log('Logged startup event to MongoDB');
  } catch (e) {
    console.error('Failed to log startup event:', e);
  }


  const app = express();
  app.use(express.json());
  app.use(apiRouter);

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`API listening on http://0.0.0.0:${PORT}`);
  });

  const shutdown = async () => {
    console.log('Shutting down...');
    try {
      if (timers && timers.pos) clearInterval(timers.pos);
      if (timers && timers.bars) clearInterval(timers.bars);
      if (timers && timers.quotes) clearInterval(timers.quotes);
      if (timers && timers.trades) clearInterval(timers.trades);
      if (timers && timers.orders) clearInterval(timers.orders);
      await closeDb();
    } finally {
      server.close(() => process.exit(0));
      // Fallback hard-exit after 3s
      setTimeout(() => process.exit(0), 3000).unref();
    }
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
