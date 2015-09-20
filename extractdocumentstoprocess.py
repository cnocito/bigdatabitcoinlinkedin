__author__ = 'cnocito'

from pymongo import MongoClient, ASCENDING

collectionName = "MarketData"

client = MongoClient()
db = client.get_database('local')
collection = db.get_collection(collectionName)

items = collection.find({"source":"btceUSD"},{"_id":1})

with open("output.txt","w") as f:
    for item in items:
        f.write(str(item['_id'])+"\n")

f.close()