import { fetchQuotes, upsertQuotes } from './historicalQuotes.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Fetches historical quotes (last 30 days by default) for each symbol in the
 * listeningsymbols collection and persists them into `quotes_hist`.
 *
 * @param {import('@alpacahq/alpaca-trade-api').default} alpaca
 * @param {import('mongodb').Db} db
 * @param {{ daysBack?: number, feed?: 'iex'|'sip'|'boats'|'otc' }} options
 */
export async function syncQuotes(alpaca, db, options = {}) {
  const daysBack = typeof options.daysBack === 'number' ? options.daysBack : 30;
  const feed = options.feed || 'iex';

  const col = db.collection('listeningsymbols');
  const symDocs = await col.find({}, { projection: { symbol: 1, quote: 1 } }).toArray();
  if (!symDocs.length) {
    console.warn('[quotes] No symbols found in listeningsymbols – nothing to sync');
    return;
  }

  for (const symDoc of symDocs) {
    const symbol = String(symDoc?.symbol || '').trim().toUpperCase();
    if (!symbol) continue;

    const now = new Date();
    const since = symDoc?.quote ? new Date(symDoc.quote) : new Date(now.getTime() - daysBack * 24 * 60 * 60 * 1000);

    console.log(`[quotes] Syncing last ${daysBack}d of quotes for ${symbol}`);

    // Backoff on 429 rate limits
    let quotes = [];
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        quotes = await fetchQuotes(alpaca, symbol, { since, until: now, daysBack, feed });
        break;
      } catch (e) {
        const code = e && (e.statusCode || e.code || e.status);
        if (code === 429 && attempt < 4) {
          const delay = Math.min(30000, 2000 * Math.pow(2, attempt)) + Math.floor(Math.random() * 250);
          console.warn(`[quotes] ${symbol} rate-limited (429). Retrying in ${delay}ms (attempt ${attempt + 1})`);
          await sleep(delay);
          continue;
        }
        console.warn(`[quotes] ${symbol} failed to fetch quotes:`, e && e.message ? e.message : e);
        quotes = [];
        break;
      }
    }

    // Only keep quotes strictly newer than the saved watermark
    const freshQuotes = filterNew(quotes, symDoc?.quote);

    const res = await upsertQuotes(db, symbol, freshQuotes);
    console.log(
      `[quotes] ${symbol} — upserted ${res.upserted} quotes (${freshQuotes.length} fetched)`
    );

    // Update last seen quote timestamp
    await updateLastSeen(col, symbol, 'quote', freshQuotes);

    // small pause between symbols to ease rate limits
    await sleep(250);
  }

  // helpers
  function filterNew(items, since) {
    return since ? items.filter((q) => new Date(q.t) > new Date(since)) : items;
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
