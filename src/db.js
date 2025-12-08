import { MongoClient } from 'mongodb';
import { settings } from './config.js';

let client = null;
let db = null;

async function connectDb() {
  if (!client) {
    client = new MongoClient(settings.mongodbUri, { ignoreUndefined: true });
    await client.connect();
    db = client.db(settings.mongodbDb);
  }
  return db;
}

async function getDb() {
  if (db) return db;
  return connectDb();
}

async function closeDb() {
  if (client) {
    await client.close();
    client = null;
    db = null;
  }
}

export { connectDb, getDb, closeDb };
