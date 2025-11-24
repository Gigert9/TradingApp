import os
from pymongo import MongoClient

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client["TradingApp"]
collection = db["HistoricalData"]

all_documents = collection.find()
for document in all_documents:
    print(document)