/**
 * Enrich bar collections with common technical-analysis indicators.
 *
 * Collections processed:
 *   • 1d_bars
 *   • 1h_bars
 *   • 15m_bars
 *   • 1m_bars
 *
 * New fields added per document
 *   sma20, sma50,
 *   ema12, ema26,
 *   macd, macdSignal, macdHistogram,
 *   rsi14,
 *   adx14, plusDmi14, minusDmi14,
 *   atr14,
 *   bollingerUpper, bollingerMiddle, bollingerLower
 *
 * Run manually with:
 *   node src/jobs/enrichIndicators.js
 */

import { connectDb, getDb, closeDb } from '../db.js';
import ti from 'technicalindicators';

const { SMA, EMA, MACD, RSI, BollingerBands, ATR: ATR_TI, ADX: ADX_TI } = ti;

const BAR_COLLECTIONS = ['1d_bars', '1h_bars', '15m_bars', '1m_bars'];

export async function enrichIndicators(dbOverride) {
  let db;
  let shouldClose = false;

  if (dbOverride) {
    db = dbOverride;
  } else {
    await connectDb();
    db = await getDb();
    shouldClose = true;
  }
  const BULK_BATCH_SIZE = 5000; // split very large updates into safe chunks

  for (const colName of BAR_COLLECTIONS) {
    const col = db.collection(colName);
    const symbols = await col.distinct('symbol');
    console.log(`[ind] ${colName}: ${symbols.length} symbols`);

    for (const symbol of symbols) {
      console.log(`[ind] ${colName}:${symbol} — computing indicators`);

      const bars = await col
        .find({ symbol })
        .sort({ t: 1 })
        .toArray();

      if (bars.length === 0) continue;

      // Initialise indicators (technicalindicators streaming)
      const sma20 = new SMA({ period: 20, values: [] });
      const sma50 = new SMA({ period: 50, values: [] });

      // Exponential moving averages we persist
      const ema12 = new EMA({ period: 12, values: [] });
      const ema26 = new EMA({ period: 26, values: [] });

      // MACD via technicalindicators (EMA-based)
      const macd = new MACD({
        fastPeriod: 12,
        slowPeriod: 26,
        signalPeriod: 9,
        SimpleMAOscillator: false,
        SimpleMASignal: false,
        values: [],
      });

      const rsi14 = new RSI({ period: 14, values: [] });

      // ATR & ADX streaming indicators (technicalindicators)
      const atr14 = new ATR_TI({ period: 14, high: [], low: [], close: [] });
      const adx14 = new ADX_TI({ period: 14, high: [], low: [], close: [] });

      const bb20 = new BollingerBands({ period: 20, stdDev: 2, values: [] });

      let ops = [];
      let totalUpdated = 0;

      for (const bar of bars) {
        // tolerate different field casings that exist in various collections
        const closeRaw =
          bar.close ?? bar.ClosePrice ?? bar.c ?? bar.Close ?? null;
        const highRaw =
          bar.high ?? bar.HighPrice ?? bar.h ?? bar.High ?? null;
        const lowRaw =
          bar.low ?? bar.LowPrice ?? bar.l ?? bar.Low ?? null;

        // Skip bars where essential prices are missing / not numeric
        const close = Number(closeRaw);
        const high = Number(highRaw);
        const low = Number(lowRaw);
        if (!Number.isFinite(close) || !Number.isFinite(high) || !Number.isFinite(low)) {
          // optional: uncomment for noisy datasets
          // console.warn(`[ind] Skipping bar with invalid numbers: close=${close}, high=${high}, low=${low}, barId=${bar._id}`);
          continue;
        }

        // sequentially feed indicators
        const sma20v = sma20.nextValue(close);
        const sma50v = sma50.nextValue(close);
        const ema12v = ema12.nextValue(close);
        const ema26v = ema26.nextValue(close);
        const macdRes = macd.nextValue(close);
        const rsi14v = rsi14.nextValue(close);
        /* -------- ATR & ADX via streaming indicators -------- */
        const atr14v = atr14.nextValue({ high, low, close });

        const adxRaw = adx14.nextValue({ high, low, close });
        let adxRes;
        if (adxRaw) {
          const { adx, pdi, mdi } = adxRaw;
          adxRes = { adx, plusDI: pdi, minusDI: mdi };
        }

        const bbRes = bb20.nextValue(close);

        ops.push({
          updateOne: {
            filter: { _id: bar._id },
            update: {
              $set: {
                ...(sma20v !== undefined && { sma20: Number(sma20v) }),
                ...(sma50v !== undefined && { sma50: Number(sma50v) }),
                ...(ema12v !== undefined && { ema12: Number(ema12v) }),
                ...(ema26v !== undefined && { ema26: Number(ema26v) }),
                ...(macdRes && {
                  macd: Number((macdRes.MACD ?? macdRes.macd)),
                  macdSignal: Number(macdRes.signal),
                  macdHistogram: Number(macdRes.histogram),
                }),
                ...(rsi14v !== undefined && { rsi14: Number(rsi14v) }),
                ...(atr14v !== undefined && { atr14: Number(atr14v) }),
                ...(adxRes && {
                  adx14: Number(adxRes.adx),
                  plusDmi14: Number(adxRes.plusDI),
                  minusDmi14: Number(adxRes.minusDI),
                }),
                ...(bbRes && {
                  bollingerUpper: Number(bbRes.upper),
                  bollingerMiddle: Number(bbRes.middle),
                  bollingerLower: Number(bbRes.lower),
                }),
              },
            },
          },
        });

        if (ops.length === BULK_BATCH_SIZE) {
          await col.bulkWrite(ops, { ordered: false });
          totalUpdated += ops.length;
          ops = [];
        }
      }

      if (ops.length) {
        await col.bulkWrite(ops, { ordered: false });
        totalUpdated += ops.length;
      }
      console.log(`[ind] ${colName}:${symbol} — updated ${totalUpdated} bars`);
    }
  }

  console.log('[ind] Finished enriching indicators');
  if (shouldClose) {
    await closeDb().catch(() => undefined);
  }
}

/* Allow direct execution */
if (process.argv[1] && process.argv[1].endsWith('enrichIndicators.js')) {
  enrichIndicators()
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
