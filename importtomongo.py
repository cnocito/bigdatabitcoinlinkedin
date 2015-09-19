__author__ = 'cnocito'

from pymongo import MongoClient, ASCENDING, DESCENDING
import pandas as pd
import os
import traceback

directory = "rawfiles"
collectionName = "MarketData"

client = MongoClient()
db = client.get_database('local')
db.drop_collection(collectionName)
collection = db.get_collection(collectionName)
collection.create_index([("timestamp",ASCENDING)])
collection.create_index([("source",ASCENDING)])
collection.create_index([("timestamp",ASCENDING),("source",ASCENDING)])

for dirpath, dirnames, files in os.walk(directory):
    for file in files:
        if(len(file.split(".csv"))>1):
            print("Inserting %s" % file)
            try:
                source = file.split(".csv")[0]
                data = pd.read_csv(directory+"/"+file,header=None)
                data[3] = source
                data.columns = ["timestamp","price","quantity","source"]
                dicts = data.to_dict('records')
                collection.insert_many(dicts)
                print("Done importing %s!" % file)
            except:
                print(traceback.format_exc())
