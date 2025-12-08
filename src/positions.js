/**
 * Sync current Alpaca positions into MongoDB.
 *
 * - Upserts into `positions_current` uniquely by (paper, symbol)
 * - Inserts a snapshot copy into `positions_historical` on each refresh
 *
 * @param {import('@alpacahq/alpaca-trade-api').default} alpaca
 * @param {import('mongodb').Db} db
 * @param {{ paper?: boolean }} options
 */
export async function syncPositions(alpaca, db, options = {}) {
  const paper = !!options.paper;

  const colCur = db.collection('positions_current');
  const colHist = db.collection('positions_historical');

  // Ensure indexes (non-blocking)
  colCur.createIndex({ paper: 1, symbol: 1 }, { unique: true }).catch(() => undefined);
  colCur.createIndex({ updated_at: -1 }).catch(() => undefined);
  colHist.createIndex({ paper: 1, symbol: 1, snapshot_at: 1 }, { unique: true }).catch(() => undefined);
  colHist.createIndex({ snapshot_at: -1 }).catch(() => undefined);

  let positions = [];
  try {
    positions = await alpaca.getPositions();
  } catch (e) {
    console.warn('getPositions failed:', e && e.message ? e.message : e);
    return { upserted: 0, snapshotInserted: 0 };
  }

  const nowIso = new Date().toISOString();

  const toNum = (v) => {
    if (v === null || v === undefined) return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  };

  const docs = positions
    .map((p) => {
      const doc = {
        paper,
        symbol: (p.symbol || '').trim().toUpperCase(),
        asset_id: p.asset_id || p.assetId || null,
        asset_class: p.asset_class || p.assetClass || null,
        asset_marginable:
          typeof p.asset_marginable === 'boolean'
            ? p.asset_marginable
            : String(p.asset_marginable) === 'true',
        exchange: p.exchange || null,
        side: p.side || null,
        updated_at: nowIso,
        _received_at: nowIso,
      };

      const numericFields = {
        avg_entry_price: toNum(p.avg_entry_price ?? p.avgEntryPrice),
        change_today: toNum(p.change_today ?? p.changeToday),
        cost_basis: toNum(p.cost_basis ?? p.costBasis),
        current_price: toNum(p.current_price ?? p.currentPrice),
        lastday_price: toNum(p.lastday_price ?? p.lastdayPrice),
        market_value: toNum(p.market_value ?? p.marketValue),
        qty: toNum(p.qty),
        qty_available: toNum(p.qty_available ?? p.qtyAvailable),
        unrealized_intraday_pl: toNum(p.unrealized_intraday_pl ?? p.unrealizedIntradayPl),
        unrealized_intraday_plpc: toNum(p.unrealized_intraday_plpc ?? p.unrealizedIntradayPlpc),
        unrealized_pl: toNum(p.unrealized_pl ?? p.unrealizedPl),
        unrealized_plpc: toNum(p.unrealized_plpc ?? p.unrealizedPlpc),
      };
      for (const [k, v] of Object.entries(numericFields)) {
        if (v !== undefined) doc[k] = v;
      }
      return doc;
    })
    .filter((d) => d.symbol);

  // Ensure all position symbols are present in listeningsymbols for streaming coverage
  try {
    if (docs.length) {
      const ops = docs.map(d => ({
        updateOne: {
          filter: { symbol: d.symbol },
          update: { $setOnInsert: { symbol: d.symbol } },
          upsert: true
        }
      }));
      await db.collection('listeningsymbols').bulkWrite(ops, { ordered: false });
    }
  } catch (e) {
    console.warn('listeningsymbols upsert from positions failed:', e && e.message ? e.message : e);
  }

  // Keep positions_current in sync with what's actually open right now
  try {
    const currentSymbols = docs.map((d) => d.symbol);
    await colCur.deleteMany({ paper, symbol: { $nin: currentSymbols } });
  } catch (e) {
    console.warn('positions_current cleanup failed:', e && e.message ? e.message : e);
  }

  // Upsert current positions
  let upserted = 0;
  if (docs.length) {
    const opsCur = docs.map((d) => ({
      updateOne: {
        filter: { paper, symbol: d.symbol },
        update: { $set: d },
        upsert: true,
      },
    }));
    const resCur = await colCur.bulkWrite(opsCur, { ordered: false });
    upserted = resCur.upsertedCount || 0;
  }

  // Insert historical snapshot (dedupe by exact snapshot time)
  let snapshotInserted = 0;
  if (docs.length) {
    const opsHist = docs.map((d) => ({
      updateOne: {
        filter: { paper, symbol: d.symbol, snapshot_at: nowIso },
        update: { $setOnInsert: { ...d, snapshot_at: nowIso } },
        upsert: true,
      },
    }));
    const resHist = await colHist.bulkWrite(opsHist, { ordered: false });
    snapshotInserted = resHist.upsertedCount || 0;
  }

  return { upserted, snapshotInserted };
}
