/**
 * Helper functions for fetching and preparing historical stock quotes from Alpaca,
 * and utilities to persist them into MongoDB.
 *
 * @typedef {import('@alpacahq/alpaca-trade-api').default} Alpaca
 */

/**
 * Ensure every quote contains a normalised ISO-8601 `t` timestamp and upper-cased `symbol`.
 *
 * @param {any} q Raw quote from Alpaca
 * @param {string} symbolFallback
 * @returns {any}
 */
export function normaliseQuote(q, symbolFallback = '') {
  const ts = q.t ?? q.Timestamp ?? q.timestamp ?? q.quote_timestamp;
  let iso = null;
  if (ts instanceof Date) iso = ts.toISOString();
  else if (typeof ts === 'string') iso = new Date(ts).toISOString();
  else if (typeof ts === 'number') iso = new Date(ts < 1e12 ? ts * 1000 : ts).toISOString();

  const symbol =
    (q.symbol || q.S || q.Symbol || symbolFallback || '').toString().trim().toUpperCase();

  const out = { ...q };
  if (iso) out.t = iso;
  if (symbol) out.symbol = symbol;
  return out;
}

/**
 * Remove duplicate quotes (same timestamp) from a quote array.
 *
 * @param {any[]} quotes
 * @returns {any[]}
 */
export function dedupeQuotes(quotes) {
  return Array.from(new Map(quotes.map((q) => [q.t, q])).values());
}

/**
 * Fetch quotes for a symbol over a time window, chunked by day to avoid limits.
 *
 * @param {Alpaca} alpaca
 * @param {string} symbol
 * @param {{ since?: string|Date, until?: string|Date, daysBack?: number, feed?: 'iex'|'sip'|'boats'|'otc' }} options
 * @returns {Promise<any[]>}
 */
export async function fetchQuotes(alpaca, symbol, options = {}) {
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

    // The JS SDK exposes an async iterator for historical quotes which transparently follows next_page_token until exhaustion
    const iter = alpaca.getQuotesV2(symbol, {
      start: sliceStart.toISOString(),
      end: sliceEnd.toISOString(),
      feed,
      limit: 10000,
      sort: 'asc',
    });

    for await (const quote of iter) {
      results.push(normaliseQuote(quote, symbol));
    }
  }

  return dedupeQuotes(results);
}

/**
 * Upsert an array of quotes into quotes_hist collection with unique key (symbol, t).
 *
 * @param {import('mongodb').Db} db
 * @param {string} symbol
 * @param {any[]} quotes
 */
export async function upsertQuotes(db, symbol, quotes) {
  if (!Array.isArray(quotes) || !quotes.length) return { matched: 0, upserted: 0 };
  const sym = String(symbol).trim().toUpperCase();

  const ops = quotes
    .map((q) => normaliseQuote(q, sym))
    .filter((q) => q.t)
    .map((q) => ({
      updateOne: {
        filter: { symbol: sym, t: q.t },
        update: { $set: { ...q, symbol: sym } },
        upsert: true,
      },
    }));

  if (!ops.length) return { matched: 0, upserted: 0 };
  const res = await db.collection('quotes_hist').bulkWrite(ops, { ordered: false });
  return {
    matched: res.matchedCount || 0,
    upserted: res.upsertedCount || 0,
  };
}
