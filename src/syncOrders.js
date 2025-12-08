/**
 * Fetch and persist Alpaca orders into MongoDB.
 *
 * Creates/uses the `orders` collection and upserts by unique key (id).
 *
 * @param {import('@alpacahq/alpaca-trade-api').default} alpaca
 * @param {import('mongodb').Db} db
 * @param {{ since?: string|Date, until?: string|Date, daysBack?: number, status?: string, side?: string, symbols?: string[] }} options
 */
export async function syncOrders(alpaca, db, options = {}) {
  const status = options.status || 'all';
  const side = options.side || undefined;
  const symbols = Array.isArray(options.symbols)
    ? options.symbols.map(s => String(s).trim().toUpperCase()).filter(Boolean)
    : undefined;

  const now = new Date();
  const until = options.until ? new Date(options.until) : now;
  const daysBack = typeof options.daysBack === 'number' ? options.daysBack : 365;
  const since = options.since ? new Date(options.since) : new Date(until.getTime() - daysBack * 24 * 60 * 60 * 1000);

  const col = db.collection('orders');

  // Ensure indexes (non-blocking)
  col.createIndex({ id: 1 }, { unique: true }).catch(() => undefined);
  col.createIndex({ symbol: 1, updated_at: -1 }).catch(() => undefined);
  col.createIndex({ status: 1, symbol: 1 }).catch(() => undefined);

  const toNum = (v) => {
    if (v === null || v === undefined) return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  };
  const toIso = (v) => {
    if (!v) return null;
    const d = typeof v === 'string' ? new Date(v) : v instanceof Date ? v : new Date(v);
    return isNaN(d.getTime()) ? null : d.toISOString();
  };

  const paramsBase = {
    status,
    limit: 500,
    direction: 'asc',
    ...(side ? { side } : {}),
    ...(symbols && symbols.length ? { symbols: symbols.join(',') } : {}),
  };

  let cursor = since.toISOString();
  const endIso = until.toISOString();
  let total = 0;

  while (true) {
    let batch = [];
    try {
      batch = await alpaca.getOrders({ ...paramsBase, after: cursor, until: endIso });
    } catch (e) {
      console.warn('getOrders failed:', e && e.message ? e.message : e);
      break;
    }
    if (!Array.isArray(batch) || batch.length === 0) break;

    const nowIso = new Date().toISOString();

    const ops = batch
      .map((o) => {
        const doc = {
          id: o.id,
          client_order_id: o.client_order_id || o.clientOrderId || null,
          symbol: (o.symbol || '').toString().trim().toUpperCase(),
          status: o.status || null,
          side: o.side || null,
          type: o.type || o.order_type || null,
          order_type: o.order_type || o.type || null,
          order_class: o.order_class || null,
          time_in_force: o.time_in_force || null,
          position_intent: o.position_intent || null,
          source: o.source || null,
          asset_id: o.asset_id || null,
          asset_class: o.asset_class || null,
          extended_hours: typeof o.extended_hours === 'boolean' ? o.extended_hours : String(o.extended_hours) === 'true',
          hwm: o.hwm ?? null,

          // Numeric fields (strings from API -> numbers)
          qty: toNum(o.qty),
          filled_qty: toNum(o.filled_qty),
          limit_price: toNum(o.limit_price),
          stop_price: toNum(o.stop_price),
          filled_avg_price: toNum(o.filled_avg_price),
          notional: toNum(o.notional),
          trail_price: toNum(o.trail_price),
          trail_percent: toNum(o.trail_percent),

          // Timestamps normalized to ISO
          created_at: toIso(o.created_at),
          submitted_at: toIso(o.submitted_at),
          updated_at: toIso(o.updated_at),
          filled_at: toIso(o.filled_at),
          canceled_at: toIso(o.canceled_at),
          expired_at: toIso(o.expired_at),
          expires_at: toIso(o.expires_at),
          replaced_at: toIso(o.replaced_at),

          replaced_by: o.replaced_by || null,
          replaces: o.replaces || null,

          _received_at: nowIso,
        };

        // Remove undefined numeric fields so $set doesn't store them
        Object.keys(doc).forEach((k) => {
          if (doc[k] === undefined) delete doc[k];
        });

        return {
          updateOne: {
            filter: { id: doc.id },
            update: { $set: doc },
            upsert: true,
          },
        };
      });

    if (ops.length) {
      try {
        const res = await col.bulkWrite(ops, { ordered: false });
        total += (res.upsertedCount || 0) + (res.matchedCount || 0);
      } catch (e) {
        console.warn('orders bulkWrite failed:', e && e.message ? e.message : e);
      }
    }

    // Advance cursor by last submitted/updated time to continue pagination
    const last = batch[batch.length - 1];
    const nextCursor = toIso(last.updated_at || last.submitted_at || last.created_at);
    if (!nextCursor || nextCursor <= cursor) break;
    cursor = nextCursor;

    if (batch.length < (paramsBase.limit || 500)) break;
  }

  return { processed: total };
}
