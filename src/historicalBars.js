/**
 * Helper functions for fetching and preparing historical price bars from Alpaca,
 * and utilities to persist them into MongoDB timeframe collections.
 *
 * @typedef {import('@alpacahq/alpaca-trade-api').default} Alpaca
 */

/**
 * Ensure every bar object contains a normalised ISO-8601 `t` timestamp.
 *
 * @param {any} bar Raw bar object from Alpaca
 * @returns {any}   Bar with `{ …bar, t: <iso-string> }`
 */
export function normaliseBar(bar) {
  const ts = bar.t ?? bar.Timestamp;
  let iso = null;
  if (ts instanceof Date) iso = ts.toISOString();
  else if (typeof ts === 'string') iso = new Date(ts).toISOString();
  else if (typeof ts === 'number') iso = new Date(ts).toISOString();
  const out = { ...bar };
  if (iso) out.t = iso;
  return out;
}

/**
 * Remove duplicate bars (same timestamp) from a bar array.
 *
 * @param {any[]} bars
 * @returns {any[]}
 */
export function dedupeBars(bars) {
  return Array.from(new Map(bars.map((b) => [b.t, b])).values());
}

/**
 * Return daily bars for a given window.
 *
 * @param {Alpaca} alpaca Initialised Alpaca client
 * @param {string} symbol  Ticker symbol, e.g. "AAPL"
 * @param {object} options { since?: string|Date, until?: string|Date, daysBack?: number }
 * @returns {Promise<any[]>}
 */
export async function fetchDailyBars(alpaca, symbol, options = {}) {
  const { since, until, daysBack = 730 } = options;

  const end = until ? new Date(until) : new Date();
  if (!until) {
    end.setDate(end.getDate() - 1);
    end.setHours(23, 59, 59, 999);
  }

  let start;
  if (since) {
    start = new Date(since);
    start.setDate(start.getDate() - 1); // small overlap
  } else {
    start = new Date(end);
    start.setDate(start.getDate() - daysBack);
  }

  const barsIter = alpaca.getBarsV2(symbol, {
    start: start.toISOString(),
    ...(until ? { end: end.toISOString() } : {}),
    timeframe: '1Day',
    adjustment: 'all',
    feed: 'iex',
  });

  const results = [];
  for await (const bar of barsIter) {
    results.push(normaliseBar(bar));
  }
  return dedupeBars(results);
}

/**
 * Return 15-minute bars for a given window.
 */
export async function fetch15MinBars(alpaca, symbol, options = {}) {
  const { since, until, daysBack = 90 } = options;

  const CHUNK_DAYS = 7;
  const MS_PER_DAY = 24 * 60 * 60 * 1000;

  const endAll = until ? new Date(until) : new Date();
  const startAll = since ? new Date(since) : new Date(endAll.getTime() - daysBack * MS_PER_DAY);

  const results = [];

  for (
    let sliceStart = new Date(startAll);
    sliceStart < endAll;
    sliceStart = new Date(sliceStart.getTime() + CHUNK_DAYS * MS_PER_DAY)
  ) {
    const barsIter = alpaca.getBarsV2(symbol, {
      start: sliceStart.toISOString(),
      end: new Date(
        Math.min(
          sliceStart.getTime() + CHUNK_DAYS * MS_PER_DAY - 1,
          endAll.getTime(),
        ),
      ).toISOString(),
      timeframe: '15Min',
      adjustment: 'all',
      feed: 'iex',
    });

    for await (const bar of barsIter) {
      results.push(normaliseBar(bar));
    }
  }

  return dedupeBars(results);
}

/**
 * Return 1-minute bars for a given window.
 */
export async function fetch1MinBars(alpaca, symbol, options = {}) {
  const { since, until, daysBack = 30 } = options;

  // Use 1-day slices to avoid paging bugs that caused missing / duplicate data
  const CHUNK_DAYS = 1;
  const MS_PER_DAY = 24 * 60 * 60 * 1000;

  const endAll = until ? new Date(until) : new Date();
  if (!until) {
    endAll.setDate(endAll.getDate() - 1);
    endAll.setHours(23, 59, 59, 999);
  }

  const startAll = since ? new Date(since) : new Date(endAll.getTime() - daysBack * MS_PER_DAY);

  const results = [];

  for (
    let sliceStart = new Date(startAll);
    sliceStart < endAll;
    sliceStart = new Date(sliceStart.getTime() + CHUNK_DAYS * MS_PER_DAY)
  ) {
    const barsIter = alpaca.getBarsV2(symbol, {
      start: sliceStart.toISOString(),
      end: new Date(
        Math.min(
          sliceStart.getTime() + CHUNK_DAYS * MS_PER_DAY - 1,
          endAll.getTime(),
        ),
      ).toISOString(),
      timeframe: '1Min',
      adjustment: 'all',
      feed: 'iex',
    });

    for await (const bar of barsIter) {
      results.push(normaliseBar(bar));
    }
  }

  return dedupeBars(results);
}

/**
 * Return 1-hour bars for a given window.
 */
export async function fetchHourlyBars(alpaca, symbol, options = {}) {
  const { since, until, hoursBack = 24 * 180 } = options;

  const CHUNK_DAYS = 30;
  const MS_PER_HOUR = 60 * 60 * 1000;
  const MS_PER_DAY = 24 * MS_PER_HOUR;

  const endAll = until ? new Date(until) : new Date();
  if (!until) {
    endAll.setDate(endAll.getDate() - 1);
    endAll.setHours(23, 59, 59, 999);
  }

  const startAll = since ? new Date(since) : new Date(endAll.getTime() - hoursBack * MS_PER_HOUR);

  const results = [];

  for (
    let sliceStart = new Date(startAll);
    sliceStart < endAll;
    sliceStart = new Date(sliceStart.getTime() + CHUNK_DAYS * MS_PER_DAY)
  ) {
    const barsIter = alpaca.getBarsV2(symbol, {
      start: sliceStart.toISOString(),
      end: new Date(
        Math.min(
          sliceStart.getTime() + CHUNK_DAYS * MS_PER_DAY,
          endAll.getTime(),
        ),
      ).toISOString(),
      timeframe: '1Hour',
      adjustment: 'all',
      feed: 'iex',
    });

    for await (const bar of barsIter) {
      results.push(normaliseBar(bar));
    }
  }

  return dedupeBars(results);
}

/**
 * Upsert an array of bars into a timeframe collection with unique key (symbol, t).
 *
 * @param {import('mongodb').Db} db
 * @param {string} collectionName One of: '1m_bars', '15m_bars', '1h_bars', '1d_bars'
 * @param {string} symbol
 * @param {any[]} bars
 */
export async function upsertBars(db, collectionName, symbol, bars) {
  if (!Array.isArray(bars) || !bars.length) return { matched: 0, upserted: 0 };
  const sym = String(symbol).trim().toUpperCase();
  const ops = bars
    .map(normaliseBar)
    .filter(b => b.t)
    .map((b) => ({
      updateOne: {
        filter: { symbol: sym, t: b.t },
        update: { $set: { ...b, symbol: sym } },
        upsert: true,
      },
    }));
  if (!ops.length) return { matched: 0, upserted: 0 };
  const res = await db.collection(collectionName).bulkWrite(ops, { ordered: false });
  return {
    matched: (res.matchedCount || 0),
    upserted: (res.upsertedCount || 0),
  };
}
