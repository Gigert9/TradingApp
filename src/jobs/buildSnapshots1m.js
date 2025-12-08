/**
 * Build 1-minute market snapshots by joining bars, quotes, and trades.
 *
 * Collection: market_snapshots_1m
 * Unique key: (symbol, t)
 *
 * For each symbol and each 1m bar, we:
 *  - Pull quotes in [t, t+60s) to compute NBBO context (spreads, mids, counts)
 *  - Pull trades in [t, t+60s) to compute execution context (VWAP, counts, dollar volume)
 *  - Copy bar OHLCV, VWAP, TradeCount, and all enriched indicators
 *
 * @param {import('mongodb').Db} db
 * @param {{ since?: string|Date, until?: string|Date, daysBack?: number, incremental?: boolean }} [options]
 */
export async function buildSnapshots1m(db, options = {}) {
  const daysBack = typeof options.daysBack === 'number' ? options.daysBack : 30;
  const incremental = options.incremental !== false; // default true

  const listenCol = db.collection('listeningsymbols');
  const barsCol = db.collection('1m_bars');
  const quotesCol = db.collection('quotes_hist');
  const tradesCol = db.collection('trades_hist');
  const outCol = db.collection('market_snapshots_1m');

  // Ensure indexes (non-blocking)
  outCol.createIndex({ symbol: 1, t: 1 }, { unique: true }).catch(() => undefined);
  outCol.createIndex({ t: -1 }).catch(() => undefined);

  const symDocs = await listenCol.find({}, { projection: { symbol: 1, snapshot_1m: 1 } }).toArray();
  if (!symDocs.length) {
    console.warn('[snap1m] No symbols found in listeningsymbols – nothing to build');
    return;
  }

  const now = new Date();
  const until = options.until ? new Date(options.until) : now;

  const toIso = (d) => {
    const x = d instanceof Date ? d : new Date(d);
    return isNaN(x.getTime()) ? null : x.toISOString();
  };
  const toNum = (v) => {
    if (v === null || v === undefined) return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  };
  const pickNum = (obj, keys = []) => {
    for (const k of keys) {
      if (Object.prototype.hasOwnProperty.call(obj, k)) {
        const val = toNum(obj[k]);
        if (val !== undefined) return val;
      }
    }
    return undefined;
  };

  for (const symDoc of symDocs) {
    const symbol = String(symDoc?.symbol || '').trim().toUpperCase();
    if (!symbol) continue;

    const since =
      options.since
        ? new Date(options.since)
        : (incremental && symDoc?.snapshot_1m
            ? new Date(symDoc.snapshot_1m)
            : new Date(until.getTime() - daysBack * 24 * 60 * 60 * 1000));

    const sinceIso = toIso(since);
    const untilIso = toIso(until);
    if (!sinceIso || !untilIso) continue;

    console.log('[snap1m] Building snapshots for %s from %s to %s', symbol, sinceIso, untilIso);

    const bars = await barsCol
      .find({ symbol, t: { $gte: sinceIso, $lt: untilIso } })
      .project({
        t: 1,
        OpenPrice: 1, HighPrice: 1, LowPrice: 1, ClosePrice: 1,
        Volume: 1, VWAP: 1, TradeCount: 1,
        // indicators
        ema12: 1, ema26: 1, macd: 1, macdSignal: 1, macdHistogram: 1,
        rsi14: 1, atr14: 1, adx14: 1, plusDmi14: 1, minusDmi14: 1,
        sma20: 1, sma50: 1, bollingerLower: 1, bollingerMiddle: 1, bollingerUpper: 1,
        // tolerate alternative fields if they exist
        open: 1, high: 1, low: 1, close: 1, volume: 1, vwap: 1, trade_count: 1, o: 1, h: 1, l: 1, c: 1, vw: 1
      })
      .sort({ t: 1 })
      .toArray();

    if (!bars.length) {
      console.log('[snap1m] %s — no 1m bars to process in window', symbol);
      continue;
    }

    const ops = [];
    let lastProcessedT = null;

    for (const b of bars) {
      // Use the stored string value of b.t as the canonical minute start boundary
      const startIso = typeof b.t === 'string' ? b.t : new Date(b.t).toISOString();
      const start = new Date(startIso);
      const end = new Date(start.getTime() + 60_000);
      const endIso = end.toISOString();

      // Quotes in [start, end) with fallbacks to reduce missing data
      const quotesStrict = await quotesCol
        .find({ symbol, t: { $gte: startIso, $lt: endIso } })
        .project({
          t: 1,
          AskPrice: 1, BidPrice: 1, AskSize: 1, BidSize: 1,
          ap: 1, bp: 1, as: 1, bs: 1,
          ask: 1, bid: 1,
          A: 1, B: 1, a: 1, b: 1,
          ask_price: 1, bid_price: 1, askPrice: 1, bidPrice: 1,
          Ask: 1, Bid: 1
        })
        .sort({ t: 1 })
        .toArray();

      // Widen the window slightly if no quotes in the strict minute (for time-weighting only)
      let quotes = quotesStrict;
      if (!quotesStrict || quotesStrict.length === 0) {
        const widenStartIso = new Date(start.getTime() - 30000).toISOString();
        const widenEndIso = new Date(end.getTime() + 30000).toISOString();
        quotes = await quotesCol
          .find({ symbol, t: { $gte: widenStartIso, $lt: widenEndIso } })
          .project({
            t: 1,
            AskPrice: 1, BidPrice: 1, AskSize: 1, BidSize: 1,
            ap: 1, bp: 1, as: 1, bs: 1,
            ask: 1, bid: 1,
            A: 1, B: 1, a: 1, b: 1,
            ask_price: 1, bid_price: 1, askPrice: 1, bidPrice: 1,
            Ask: 1, Bid: 1
          })
          .sort({ t: 1 })
          .toArray();
      }

      let quote_count = quotesStrict.length;
      let bid_open, ask_open, bid_close, ask_close, mid_open, mid_close, spread_open, spread_close, spread_avg;

      // 1) Prefer values from quotes inside the minute window:
      if (quote_count > 0) {
        // Find first defined bid/ask within the window
        for (const q of quotesStrict) {
          if (bid_open === undefined) {
            const qb = pickNum(q, ['BidPrice','bp','bid','Bid','b','B','bid_price','bidPrice']);
            if (qb !== undefined) bid_open = qb;
          }
          if (ask_open === undefined) {
            const qa = pickNum(q, ['AskPrice','ap','ask','Ask','a','A','ask_price','askPrice']);
            if (qa !== undefined) ask_open = qa;
          }
          if (bid_open !== undefined && ask_open !== undefined) break;
        }

        // Find last defined bid/ask within the window
        for (let i = quotesStrict.length - 1; i >= 0; i--) {
          const q = quotesStrict[i];
          if (bid_close === undefined) {
            const qb = pickNum(q, ['BidPrice','bp','bid','Bid','b','B','bid_price','bidPrice']);
            if (qb !== undefined) bid_close = qb;
          }
          if (ask_close === undefined) {
            const qa = pickNum(q, ['AskPrice','ap','ask','Ask','a','A','ask_price','askPrice']);
            if (qa !== undefined) ask_close = qa;
          }
          if (bid_close !== undefined && ask_close !== undefined) break;
        }

        // Compute open/close mid and spread if both sides are present
        if (bid_open !== undefined && ask_open !== undefined) {
          mid_open = (bid_open + ask_open) / 2;
          spread_open = ask_open - bid_open;
        }
        if (bid_close !== undefined && ask_close !== undefined) {
          mid_close = (bid_close + ask_close) / 2;
          spread_close = ask_close - bid_close;
        }

        // spread_avg computed later as time-weighted over [start, end)
      }

      // 2) Fallbacks outside the window for any missing sides:
      if (bid_open === undefined || ask_open === undefined) {
        const prev = await quotesCol
          .find({ symbol, t: { $lt: startIso } })
          .project({
            t: 1,
            AskPrice: 1, BidPrice: 1, AskSize: 1, BidSize: 1,
            ap: 1, bp: 1, as: 1, bs: 1,
            ask: 1, bid: 1,
            A: 1, B: 1, a: 1, b: 1,
            ask_price: 1, bid_price: 1, askPrice: 1, bidPrice: 1,
            Ask: 1, Bid: 1
          })
          .sort({ t: -1 })
          .limit(50)
          .toArray();
        if (prev.length) {
          for (const qPrev of prev) {
            if (bid_open === undefined) {
              const qb = pickNum(qPrev, ['BidPrice','bp','bid','Bid','b','B','bid_price','bidPrice']);
              if (qb !== undefined) bid_open = qb;
            }
            if (ask_open === undefined) {
              const qa = pickNum(qPrev, ['AskPrice','ap','ask','Ask','a','A','ask_price','askPrice']);
              if (qa !== undefined) ask_open = qa;
            }
            if (bid_open !== undefined && ask_open !== undefined) break;
          }
          if (mid_open === undefined && bid_open !== undefined && ask_open !== undefined) {
            mid_open = (bid_open + ask_open) / 2;
            spread_open = ask_open - bid_open;
          }
        }
      }

      if (bid_close === undefined || ask_close === undefined) {
        const next = await quotesCol
          .find({ symbol, t: { $gte: endIso } })
          .project({
            t: 1,
            AskPrice: 1, BidPrice: 1, AskSize: 1, BidSize: 1,
            ap: 1, bp: 1, as: 1, bs: 1,
            ask: 1, bid: 1,
            A: 1, B: 1, a: 1, b: 1,
            ask_price: 1, bid_price: 1, askPrice: 1, bidPrice: 1,
            Ask: 1, Bid: 1
          })
          .sort({ t: 1 })
          .limit(50)
          .toArray();
        if (next.length) {
          for (const qNext of next) {
            if (bid_close === undefined) {
              const qb = pickNum(qNext, ['BidPrice','bp','bid','Bid','b','B','bid_price','bidPrice']);
              if (qb !== undefined) bid_close = qb;
            }
            if (ask_close === undefined) {
              const qa = pickNum(qNext, ['AskPrice','ap','ask','Ask','a','A','ask_price','askPrice']);
              if (qa !== undefined) ask_close = qa;
            }
            if (bid_close !== undefined && ask_close !== undefined) break;
          }
          if (mid_close === undefined && bid_close !== undefined && ask_close !== undefined) {
            mid_close = (bid_close + ask_close) / 2;
            spread_close = ask_close - bid_close;
          }
        }
      }

      // Time-weighted average spread over [start, end)
      try {
        const quotesInWindow = (quotes || [])
          .filter((q) => {
            const tIso = typeof q.t === 'string' ? q.t : new Date(q.t).toISOString();
            return tIso >= startIso && tIso < endIso;
          })
          .sort((a, b) => new Date(a.t) - new Date(b.t));

        let lastTs = start;
        let curBid = bid_open;
        let curAsk = ask_open;
        let weighted = 0;
        let totalMs = 0;

        for (const q of quotesInWindow) {
          const tq = new Date(q.t);
          if (curBid !== undefined && curAsk !== undefined && tq.getTime() > lastTs.getTime()) {
            const dt = tq.getTime() - lastTs.getTime();
            weighted += (curAsk - curBid) * dt;
            totalMs += dt;
          }
          const qb = pickNum(q, ['BidPrice','bp','bid','Bid','b','B','bid_price','bidPrice']);
          const qa = pickNum(q, ['AskPrice','ap','ask','Ask','a','A','ask_price','askPrice']);
          if (qb !== undefined) curBid = qb;
          if (qa !== undefined) curAsk = qa;
          if (tq.getTime() > lastTs.getTime()) lastTs = tq;
        }

        if (curBid !== undefined && curAsk !== undefined && end.getTime() > lastTs.getTime()) {
          const dt = end.getTime() - lastTs.getTime();
          if (dt > 0) {
            weighted += (curAsk - curBid) * dt;
            totalMs += dt;
          }
        }

        if (totalMs > 0) {
          spread_avg = weighted / totalMs;
        }

        // Fallback: derive close NBBO at end boundary from last seen bid/ask inside window
        if (bid_close === undefined && curBid !== undefined) bid_close = curBid;
        if (ask_close === undefined && curAsk !== undefined) ask_close = curAsk;
        if ((mid_close === undefined || spread_close === undefined) && bid_close !== undefined && ask_close !== undefined) {
          mid_close = (bid_close + ask_close) / 2;
          spread_close = ask_close - bid_close;
        }
      } catch (_) {
        // no-op
      }

      // Trades in [start, end)
      const trades = await tradesCol
        .find({ symbol, t: { $gte: startIso, $lt: endIso } })
        .project({ t: 1, Price: 1, Size: 1, p: 1, s: 1, price: 1, size: 1, P: 1, S: 1, qty: 1, quantity: 1, q: 1 })
        .toArray();

      const trades_exec_count = trades.length;
      let sizeSum = 0;
      let pxSizeSum = 0;
      let sizeCount = 0;

      for (const tr of trades) {
        const px = pickNum(tr, ['Price','p','price','P']);
        const sz = pickNum(tr, ['Size','s','size','S','qty','quantity','q']);
        if (px !== undefined && sz !== undefined) {
          sizeSum += sz;
          pxSizeSum += px * sz;
          sizeCount++;
        }
      }

      const dollar_volume = pxSizeSum || undefined;
      const vwap_exec = sizeSum > 0 ? pxSizeSum / sizeSum : undefined;
      const avg_trade_size = trades_exec_count > 0 ? (sizeSum / trades_exec_count) : undefined;

      // Bar values with robust field mapping
      const open =
        toNum(b.OpenPrice ?? b.open ?? b.o ?? b.Open);
      const high =
        toNum(b.HighPrice ?? b.high ?? b.h ?? b.High);
      const low =
        toNum(b.LowPrice ?? b.low ?? b.l ?? b.Low);
      const close =
        toNum(b.ClosePrice ?? b.close ?? b.c ?? b.Close);
      const volume =
        pickNum(b, ['Volume','volume','v']);
      const vwap_bar =
        pickNum(b, ['VWAP','vwap','vw']);
      const trades_bar =
        pickNum(b, ['TradeCount','trade_count','n','trades']);

      const snapshot = {
        symbol,
        t: b.t,
        timestamp: new Date(startIso),
        // bar
        open, high, low, close, volume,
        vwap_bar, trades_bar,
        // indicators
        ...(b.ema12 !== undefined && { ema12: toNum(b.ema12) }),
        ...(b.ema26 !== undefined && { ema26: toNum(b.ema26) }),
        ...(b.macd !== undefined && { macd: toNum(b.macd) }),
        ...(b.macdSignal !== undefined && { macdSignal: toNum(b.macdSignal) }),
        ...(b.macdHistogram !== undefined && { macdHistogram: toNum(b.macdHistogram) }),
        ...(b.rsi14 !== undefined && { rsi14: toNum(b.rsi14) }),
        ...(b.atr14 !== undefined && { atr14: toNum(b.atr14) }),
        ...(b.adx14 !== undefined && { adx14: toNum(b.adx14) }),
        ...(b.plusDmi14 !== undefined && { plusDmi14: toNum(b.plusDmi14) }),
        ...(b.minusDmi14 !== undefined && { minusDmi14: toNum(b.minusDmi14) }),
        ...(b.sma20 !== undefined && { sma20: toNum(b.sma20) }),
        ...(b.sma50 !== undefined && { sma50: toNum(b.sma50) }),
        ...(b.bollingerLower !== undefined && { bollingerLower: toNum(b.bollingerLower) }),
        ...(b.bollingerMiddle !== undefined && { bollingerMiddle: toNum(b.bollingerMiddle) }),
        ...(b.bollingerUpper !== undefined && { bollingerUpper: toNum(b.bollingerUpper) }),
        // quotes
        ...(bid_open !== undefined && { bid_open }),
        ...(ask_open !== undefined && { ask_open }),
        ...(bid_close !== undefined && { bid_close }),
        ...(ask_close !== undefined && { ask_close }),
        ...(mid_open !== undefined && { mid_open }),
        ...(mid_close !== undefined && { mid_close }),
        ...(spread_open !== undefined && { spread_open }),
        ...(spread_close !== undefined && { spread_close }),
        ...(spread_avg !== undefined && { spread_avg }),
        quote_count,
        // trades
        trades_exec_count,
        ...(vwap_exec !== undefined && { vwap_exec }),
        ...(dollar_volume !== undefined && { dollar_volume }),
        ...(avg_trade_size !== undefined && { avg_trade_size }),
        _received_at: new Date().toISOString()
      };

      ops.push({
        updateOne: {
          filter: { symbol, t: b.t },
          update: { $set: snapshot },
          upsert: true
        }
      });

      lastProcessedT = b.t;

      if (ops.length >= 1000) {
        await outCol.bulkWrite(ops, { ordered: false });
        ops.length = 0;
      }
    }

    if (ops.length) {
      await outCol.bulkWrite(ops, { ordered: false });
    }

    // Watermark: latest processed minute
    if (lastProcessedT) {
      await listenCol.updateOne(
        { symbol },
        { $set: { snapshot_1m: lastProcessedT } },
        { upsert: true }
      );
    }

    console.log('[snap1m] %s — snapshots built up to %s', symbol, symDoc?.snapshot_1m || 'start');
  }

  console.log('[snap1m] Finished building 1m market snapshots');
}
