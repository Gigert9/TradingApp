/* Local helpers to avoid dependency on historicalTrades.js and add rate limiting/backoff */
const MS_PER_DAY = 24 * 60 * 60 * 1000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchTradesLocal(alpaca, symbol, options = {}) {
  const { since, until, daysBack = 30, feed = 'iex' } = options;

  const endAll = until ? new Date(until) : new Date();
  const startAll = since ? new Date(since) : new Date(endAll.getTime() - daysBack * MS_PER_DAY);

  const results = [];

  for (
    let sliceStart = new Date(startAll);
    sliceStart < endAll;
    sliceStart = new Date(sliceStart.getTime() + MS_PER_DAY)
  ) {
    const sliceEnd = new Date(Math.min(sliceStart.getTime() + MS_PER_DAY - 1, endAll.getTime()));

    const iter = alpaca.getTradesV2(symbol, {
      start: sliceStart.toISOString(),
      end: sliceEnd.toISOString(),
      feed,
      limit: 10000,
      sort: 'asc',
    });

    for await (const trade of iter) {
      // Normalise timestamp and symbol
      const ts = trade.t ?? trade.Timestamp ?? trade.timestamp ?? trade.trade_timestamp;
      let iso = null;
      if (ts instanceof Date) iso = ts.toISOString();
      else if (typeof ts === 'string') iso = new Date(ts).toISOString();
      else if (typeof ts === 'number') iso = new Date(ts < 1e12 ? ts * 1000 : ts).toISOString();

      const sym = (trade.symbol || trade.S || trade.Symbol || symbol || '')
        .toString()
        .trim()
        .toUpperCase();

      const out = { ...trade };
      if (iso) out.t = iso;
      if (sym) out.symbol = sym;

      results.push(out);
    }

    // tiny pause between day-slices to ease rate limits
    await sleep(100);
  }

  // de-dupe by timestamp
  return Array.from(new Map(results.map((t) => [t.t, t])).values());
}

async function upsertTradesLocal(db, symbol, trades) {
  if (!Array.isArray(trades) || !trades.length) return { matched: 0, upserted: 0 };
  const sym = String(symbol).trim().toUpperCase();

  const ops = trades
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

/**
 * Fetches historical trades (last 30 days by default) for each symbol in the
 * listeningsymbols collection and persists them into `trades_hist`.
 *
 * @param {import('@alpacahq/alpaca-trade-api').default} alpaca
 * @param {import('mongodb').Db} db
 * @param {{ daysBack?: number, feed?: 'iex'|'sip'|'boats'|'otc' }} options
 */
export async function syncTrades(alpaca, db, options = {}) {
  const daysBack = typeof options.daysBack === 'number' ? options.daysBack : 30;
  const feed = options.feed || 'iex';

  const col = db.collection('listeningsymbols');
  const symDocs = await col.find({}, { projection: { symbol: 1, trade: 1 } }).toArray();
  if (!symDocs.length) {
    console.warn('[trades] No symbols found in listeningsymbols – nothing to sync');
    return;
  }

  for (const symDoc of symDocs) {
    const symbol = String(symDoc?.symbol || '').trim().toUpperCase();
    if (!symbol) continue;

    const now = new Date();
    const since = symDoc?.trade ? new Date(symDoc.trade) : new Date(now.getTime() - daysBack * 24 * 60 * 60 * 1000);

    console.log(`[trades] Syncing last ${daysBack}d of trades for ${symbol}`);

    // Backoff on 429 rate limits
    let trades = [];
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        trades = await fetchTradesLocal(alpaca, symbol, { since, until: now, daysBack, feed });
        break;
      } catch (e) {
        const code = e && (e.statusCode || e.code || e.status);
        if (code === 429 && attempt < 4) {
          const delay = Math.min(30000, 2000 * Math.pow(2, attempt)) + Math.floor(Math.random() * 250);
          console.warn(`[trades] ${symbol} rate-limited (429). Retrying in ${delay}ms (attempt ${attempt + 1})`);
          await sleep(delay);
          continue;
        }
        console.warn(`[trades] ${symbol} failed to fetch trades:`, e && e.message ? e.message : e);
        trades = [];
        break;
      }
    }

    // Only keep trades strictly newer than the saved watermark
    const freshTrades = filterNew(trades, symDoc?.trade);

    const res = await upsertTradesLocal(db, symbol, freshTrades);
    console.log(
      `[trades] ${symbol} — upserted ${res.upserted} trades (${freshTrades.length} fetched)`
    );

    // Update last seen trade timestamp
    await updateLastSeen(col, symbol, 'trade', freshTrades);

    // small pause between symbols to ease rate limits
    await sleep(250);
  }

  // helpers
  function filterNew(items, since) {
    return since ? items.filter((t) => new Date(t.t) > new Date(since)) : items;
  }

  async function updateLastSeen(col, symbol, field, items) {
    if (!Array.isArray(items) || items.length === 0) return;
    const latest = items.reduce((max, it) => {
      const t = new Date(it.t).toISOString();
      return !max || t > max ? t : max;
    }, null);
    if (!latest) return;
    await col.updateOne({ symbol }, { $set: { [field]: latest } }, { upsert: true });
  }
}
