import { MongoClient } from 'mongodb';

/**
 * @typedef {Object} ListeningSymbol
 * @property {string} symbol                     Ticker symbol, e.g. "AAPL"
 * @property {Date}   ['1min_bar']?              ISO timestamp of most recent 1-minute bar
 * @property {Date}   ['15min_bar']?             ISO timestamp of most recent 15-minute bar
 * @property {Date}   ['1h_bar']?                ISO timestamp of most recent 1-hour bar
 * @property {Date}   ['1d_bar']?                ISO timestamp of most recent 1-day bar
 */

/**
 * Returns the `listeningsymbols` collection and ensures a unique index on
 * the `symbol` field.
 *
 * @param {MongoClient} client Connected MongoClient instance
 * @returns {import('mongodb').Collection<ListeningSymbol>}
 */
export function getListeningSymbolsCollection(client) {
  const db = client.db(); // default database from connection string
  const collection = db.collection('listeningsymbols');

  // Ensure a unique index on `symbol` (ignore error if it already exists)
  collection.createIndex({ symbol: 1 }, { unique: true }).catch(() => undefined);

  return collection;
}
