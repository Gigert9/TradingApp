import os
import argparse
from pymongo import MongoClient


def main():
    parser = argparse.ArgumentParser(
        description="Print documents from a MongoDB collection, optionally filtered by symbol."
    )
    parser.add_argument(
        "--db-name",
        default=os.getenv("MONGODB_DB") or "TradingApp",
        help="MongoDB database name (default from $MONGODB_DB or 'TradingApp').",
    )
    parser.add_argument(
        "--collection",
        default="HistoricalData",
        help="MongoDB collection name (default: HistoricalData).",
    )
    parser.add_argument(
        "--symbol",
        help="Filter by symbol value (uppercased).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of documents returned.",
    )
    args = parser.parse_args()

    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise SystemExit("MONGODB_URI environment variable is not set.")

    mongo_client = MongoClient(uri)
    db = mongo_client[args.db_name]
    col = db[args.collection]

    query = {}
    if args.symbol:
        query["symbol"] = str(args.symbol).strip().upper()

    cursor = col.find(query)
    if args.limit and args.limit > 0:
        cursor = cursor.limit(args.limit)

    for document in cursor:
        if "_id" in document:
            document["_id"] = str(document["_id"])
        print(document)


if __name__ == "__main__":
    main()
