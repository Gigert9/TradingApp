/**
 * Helper functions for fetching and preparing historical stock trades from Alpaca,
 * and utilities to persist them into MongoDB.
 *
 * @typedef {import('@alpacahq/alpaca-trade-api').default} Alpaca
 */

/**
 * Ensure every trade contains a normalised ISO-8601 `t` timestamp and upper-cased `symbol`.
 *
 * @param {any} tr Raw trade from Alpaca
 * @param {string} symbolFallback
 * @returns {any}
 */
export function normaliseTrade(tr, symbolFallback = '') {
  const ts = tr.t ?? tr.Timestamp ?? tr.timestamp ?? tr.trade_timestamp;
  let iso = null;
  if (ts instanceof Date) iso = ts.toISOString();
  else if (typeof ts === 'string') iso = new Date(ts).toISOString();
  else if (typeof ts === 'number') iso = new Date(ts < 1e12 ? ts * 1000 : ts).toISOString();

  const symbol =
    (tr.symbol || tr.S || tr.Symbol || symbolFallback || '').toString().trim().toUpperCase();

  const out = { ...tr };
  if (iso) out.t = iso;
  if (symbol) out.symbol = symbol;
  return out;
}

/**
 * Remove duplicate trades (same timestamp) from a trades array.
 *
 * @param {any[]} trades
 * @returns {any[]}
 */
export function dedupeTrades(trades) {
  return Array.from(new Map(trades.map((t) => [t.t, t])).values());
}

/**
 * Fetch trades for a symbol over a time window, chunked by day to avoid limits.
 *
 * @param {Alpaca} alpaca
 * @param {string} symbol
 * @param {{ since?: string|Date, until?: string|Date, daysBack?: number, feed?: 'iex'|'sip'|'boats'|'otc' }} options
 * @returns {Promise<any[]>}
 */
export async function fetchTrades(alpaca, symbol, options = {}) {
  const { since, until, daysBack = 10, feed = 'iex' } = options;

  const MS_PER_DAY = 24 * 60 * 60 * 1000;

  const endAll = until ? new Date(until) : new Date();
  const startAll = since ? new Date(since) : new Date(endAll.getTime() - daysBack * MS_PER_DAY);

  const results = [];

  for (
    let sliceStart = new Date(startAll);
    sliceStart < endAll;
    sliceStart = new Date(sliceStart.getTime() + MS_PER_DAY)
  ) {
    const sliceEnd = new Date(Math.min(sliceStart.getTime() + MS_PER_DAY - 1, endAll.getTime()));

    // The JS SDK exposes an async iterator for historical trades which transparently follows next_page_token until exhaustion
    const iter = alpaca.getTradesV2(symbol, {
      start: sliceStart.toISOString(),
      end: sliceEnd.toISOString(),
      feed,
      limit: 10000,
      sort: 'asc',
    });

    for await (const trade of iter) {
      results.push(normaliseTrade(trade, symbol));
    }
  }

  return dedupeTrades(results);
}

/**
 * Upsert an array of trades into trades_hist collection with unique key (symbol, t).
 *
 * @param {import('mongodb').Db} db
 * @param {string} symbol
 * @param {any[]} trades
 */
export async function upsertTrades(db, symbol, trades) {
  if (!Array.isArray(trades) || !trades.length) return { matched: 0, upserted: 0 };
  const sym = String(symbol).trim().toUpperCase();

  const ops = trades
    .map((t) => normaliseTrade(t, sym))
    .filter((t) => t.t)
    .map((t) => ({
      updateOne: {
        filter: { symbol: sym, t: t.t },
        update: { $set: { ...t, symbol: sym } },
        upsert: true,
      },
    }));

  if (!ops.length) return { matched: 0, upserted: 0 };
  const res = await db.collection('trades_hist').bulkWrite(ops, { ordered: false });
  return {
    matched: res.matchedCount || 0,
    upserted: res.upsertedCount || 0,
  };
}
