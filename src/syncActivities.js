/**
 * Fetch and persist Alpaca account activities into MongoDB.
 *
 * Creates/uses the `activities` collection and upserts entries by unique key (id).
 *
 * @param {import('@alpacahq/alpaca-trade-api').default} alpaca
 * @param {import('mongodb').Db} db
 * @param {{ since?: string|Date, until?: string|Date, daysBack?: number, paper?: boolean }} options
 */
export async function syncActivities(alpaca, db, options = {}) {
  const paper = !!options.paper;

  const now = new Date();
  const until = options.until ? new Date(options.until) : now;
  const daysBack = typeof options.daysBack === 'number' ? options.daysBack : 365;
  const since = options.since ? new Date(options.since) : new Date(until.getTime() - daysBack * 24 * 60 * 60 * 1000);

  const col = db.collection('activities');

  // Ensure indexes (non-blocking)
  col.createIndex({ id: 1 }, { unique: true }).catch(() => undefined);
  col.createIndex({ paper: 1, t: -1 }).catch(() => undefined);
  col.createIndex({ activity_type: 1, symbol: 1, t: -1 }).catch(() => undefined);

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function withBackoff(fn, label) {
    for (let attempt = 0; attempt < 6; attempt++) {
      try {
        return await fn();
      } catch (e) {
        const code = e && (e.statusCode || e.code || e.status);
        if (code === 429 && attempt < 5) {
          const delay = Math.min(30000, 2000 * Math.pow(2, attempt)) + Math.floor(Math.random() * 250);
          console.warn(`[activities] rate-limited (429) ${label}. Retrying in ${delay}ms (attempt ${attempt + 1})`);
          await sleep(delay);
          continue;
        }
        throw e;
      }
    }
  }

  const paramsBase = {
    direction: 'asc',
    page_size: 100,
    after: since.toISOString(),
    until: until.toISOString()
  };

  let cursor = new Date(since);
  let total = 0;

  while (true) {
    const params = { ...paramsBase, after: cursor.toISOString() };

    /** @type {any[]} */
    let batch = [];
    try {
      batch = await withBackoff(() => alpaca.getAccountActivities(params), `after=${params.after}`);
    } catch (e) {
      console.warn('getActivities failed:', e && e.message ? e.message : e);
      break;
    }

    if (!Array.isArray(batch) || batch.length === 0) break;

    const nowIso = new Date().toISOString();

    const ops = batch.map((a) => {
      // Normalise time
      const tIso =
        (a.transaction_time && new Date(a.transaction_time).toISOString()) ||
        (a.date && new Date(`${a.date}T00:00:00.000Z`).toISOString()) ||
        null;

      // Uppercase symbol if present
      const symbol = (a.symbol || '').toString().trim().toUpperCase();

      // Convert numeric-looking strings to numbers
      const toNum = (v) => {
        if (v === null || v === undefined || v === '') return undefined;
        const n = Number(v);
        return Number.isFinite(n) ? n : undefined;
      };

      const doc = {
        id: a.id,
        activity_type: a.activity_type || null,
        type: a.type || null,
        order_id: a.order_id || null,
        order_status: a.order_status || null,
        symbol: symbol || undefined,
        side: a.side || null,

        // quantities and prices (strings -> numbers)
        qty: toNum(a.qty),
        cum_qty: toNum(a.cum_qty),
        leaves_qty: toNum(a.leaves_qty),
        price: toNum(a.price),
        gross_amount: toNum(a.gross_amount),
        net_amount: toNum(a.net_amount),
        per_share_amount: toNum(a.per_share_amount),

        // dates/times
        transaction_time: a.transaction_time ? new Date(a.transaction_time).toISOString() : null,
        date: a.date || null,

        // unified timestamp for sorting
        t: tIso,

        paper,
        _received_at: nowIso
      };

      // Remove undefined to avoid storing them
      Object.keys(doc).forEach((k) => {
        if (doc[k] === undefined) delete doc[k];
      });

      return {
        updateOne: {
          filter: { id: doc.id },
          update: { $set: doc },
          upsert: true
        }
      };
    });

    if (ops.length) {
      try {
        const res = await col.bulkWrite(ops, { ordered: false });
        total += (res.upsertedCount || 0) + (res.matchedCount || 0);
      } catch (e) {
        console.warn('activities bulkWrite failed:', e && e.message ? e.message : e);
      }
    }

    // Advance the cursor based on the latest timestamp we saw
    const maxTime = batch.reduce((max, a) => {
      const tt = a.transaction_time ? new Date(a.transaction_time) : (a.date ? new Date(`${a.date}T23:59:59.999Z`) : null);
      return tt && (!max || tt > max) ? tt : max;
    }, null);

    if (!maxTime) break;
    cursor = new Date(maxTime.getTime() + 1);
    if (cursor > until) break;

    // gentle pacing to respect rate limits
    await sleep(150);
  }

  return { processed: total };
}
