import express from 'express';
import { getDb } from './db.js';
const router = express.Router();


router.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});




/**
 * GET /health/db
 * Quick DB connectivity check.
 */
router.get('/health/db', async (req, res) => {
  try {
    const db = await getDb();
    const ping = await db.command({ ping: 1 });
    res.json({ status: 'ok', db: db.databaseName, ping });
  } catch (e) {
    res.status(500).json({ status: 'error', error: String(e && e.message || e) });
  }
});

/**
 * GET /db-stats
 * Returns counts and latest document IDs for known collections.
 * Useful to verify that streaming is writing data.
 */
router.get('/db-stats', async (req, res) => {
  try {
    const db = await getDb();
    const names = ['bars', 'trades', 'quotes', 'trade_updates', 'diagnostics'];
    const result = { ok: true, db: db.databaseName, collections: {} };

    await Promise.all(
      names.map(async (name) => {
        const col = db.collection(name);
        const count = await col.estimatedDocumentCount().catch(() => 0);
        let latestId = null;
        try {
          const doc = await col.find().sort({ _id: -1 }).limit(1).next();
          latestId = doc ? String(doc._id) : null;
        } catch (_) {}
        result.collections[name] = { count, latestId };
      })
    );

    res.json(result);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e && e.message || e) });
  }
});

/**
 * POST /debug/sentinel
 * Inserts a small document to ensure the database becomes visible.
 * Safe to call if you need to confirm writes.
 */
router.post('/debug/sentinel', async (req, res) => {
  try {
    const db = await getDb();
    const doc = { type: 'sentinel', createdAt: new Date().toISOString() };
    const r = await db.collection('diagnostics').insertOne(doc);
    res.json({ ok: true, insertedId: String(r.insertedId) });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e && e.message || e) });
  }
});


export default router;
