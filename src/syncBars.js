import {
  fetchDailyBars,
  fetch15MinBars,
  fetch1MinBars,
  fetchHourlyBars,
  upsertBars as upsertBarsToCollection,
} from './historicalBars.js';
import { getListeningSymbolsCollection } from './listeningSymbols.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withBackoff(fn, label) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      const code = e && (e.statusCode || e.code || e.status);
      if (code === 429 && attempt < 5) {
        const delay = Math.min(30000, 2000 * Math.pow(2, attempt)) + Math.floor(Math.random() * 250);
        console.warn(`[bars] ${label} rate-limited (429). Retrying in ${delay}ms (attempt ${attempt + 1})`);
        await sleep(delay);
        attempt++;
        continue;
      }
      throw e;
    }
  }
}

/**
 * Fetches new historical bars for every symbol in the `listeningsymbols`
 * collection and persists them into the dedicated bar collections:
 *
 *   1m_bars, 15m_bars, 1h_bars, 1d_bars
 *
 * After inserting the bars the corresponding “last-seen” timestamp field on the
 * listeningsymbols document is updated (`1min_bar`, `15min_bar`, `1h_bar`,
 * `1d_bar`).
 *
 * @param {import('@alpacahq/alpaca-trade-api').default} alpaca
 * @param {import('mongodb').MongoClient} client
 */
export async function syncBars(alpaca, client) {
  const db = client.db();
  const listeningSymbolsCol = getListeningSymbolsCollection(client);
  const listeningSymbols = await listeningSymbolsCol.find({}).toArray();

  if (listeningSymbols.length === 0) {
    console.warn('[bars] No symbols found in listeningsymbols – nothing to sync');
    return;
  }

  /** Helper to filter only bars strictly newer than `since` */
  const filterNew = (bars, since) =>
    since ? bars.filter((b) => new Date(b.t) > new Date(since)) : bars;

  for (const symDoc of listeningSymbols) {
    const symbol = String(symDoc.symbol || '').trim().toUpperCase();
    if (!symbol) continue;

    const now = new Date();
    console.log(`[bars] Syncing ${symbol}`);

    // ---- 1-DAY -----------------------------------------------------------
    let dailyBars = await withBackoff(
      () =>
        fetchDailyBars(
          alpaca,
          symbol,
          symDoc['1d_bar'] ? { since: symDoc['1d_bar'], until: now } : { until: now },
        ),
      `${symbol} 1d`,
    );
    dailyBars = filterNew(dailyBars, symDoc['1d_bar']);
    await upsertBarsToCollection(db, '1d_bars', symbol, dailyBars);
    await updateLastSeen(listeningSymbolsCol, symbol, '1d_bar', dailyBars);
    await sleep(150);

    // ---- 15-MIN ----------------------------------------------------------
    let fifteenMinBars = await withBackoff(
      () =>
        fetch15MinBars(
          alpaca,
          symbol,
          symDoc['15min_bar'] ? { since: symDoc['15min_bar'], until: now } : { until: now },
        ),
      `${symbol} 15m`,
    );
    fifteenMinBars = filterNew(fifteenMinBars, symDoc['15min_bar']);
    await upsertBarsToCollection(db, '15m_bars', symbol, fifteenMinBars);
    await updateLastSeen(listeningSymbolsCol, symbol, '15min_bar', fifteenMinBars);
    await sleep(150);

    // ---- 1-MIN -----------------------------------------------------------
    let oneMinBars = await withBackoff(
      () =>
        fetch1MinBars(
          alpaca,
          symbol,
          symDoc['1min_bar'] ? { since: symDoc['1min_bar'], until: now } : { until: now },
        ),
      `${symbol} 1m`,
    );
    oneMinBars = filterNew(oneMinBars, symDoc['1min_bar']);
    await upsertBarsToCollection(db, '1m_bars', symbol, oneMinBars);
    await updateLastSeen(listeningSymbolsCol, symbol, '1min_bar', oneMinBars);
    await sleep(150);

    // ---- 1-HOUR ----------------------------------------------------------
    let oneHourBars = await withBackoff(
      () =>
        fetchHourlyBars(
          alpaca,
          symbol,
          symDoc['1h_bar'] ? { since: symDoc['1h_bar'], until: now } : { until: now },
        ),
      `${symbol} 1h`,
    );
    oneHourBars = filterNew(oneHourBars, symDoc['1h_bar']);
    await upsertBarsToCollection(db, '1h_bars', symbol, oneHourBars);
    await updateLastSeen(listeningSymbolsCol, symbol, '1h_bar', oneHourBars);
    await sleep(150);

    console.log(
      `[bars] ${symbol} – upserted ${dailyBars.length}/${fifteenMinBars.length}/${oneMinBars.length}/${oneHourBars.length} (1d / 15m / 1m / 1h)`
    );
    // small pause between symbols to ease rate limits
    await sleep(250);
  }

  /**
   * Update the listeningsymbols.<field> with the newest bar timestamp.
   * @param {import('mongodb').Collection} col
   * @param {string} symbol
   * @param {string} field
   * @param {any[]} bars
   */
  async function updateLastSeen(col, symbol, field, bars) {
    if (!Array.isArray(bars) || bars.length === 0) return;
    const latest = bars.reduce((max, b) => {
      const t = new Date(b.t).toISOString();
      return !max || t > max ? t : max;
    }, null);
    if (!latest) return;
    await col.updateOne({ symbol }, { $set: { [field]: latest } }, { upsert: true });
  }
}
