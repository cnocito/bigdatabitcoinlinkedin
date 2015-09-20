__author__ = 'cnocito'

from pymongo import MongoClient,ASCENDING
from bson.objectid import ObjectId
import pandas as pd
import sys
import bson
import traceback
import time

def getMaxPrice(df):
    return df[df['price'] == df['price'].max(axis=0)]

def getNumTrades(df):
    return len(df)

def getMinPrice(df):
    return df[df['price'] == df['price'].min(min=0)]

def getMeanPrice(df):
    return df['price'].mean(axis=0)

def getStdPrice(df):
    return df['price'].std(axis=0)

def getMeanQty(df):
    return df['quantity'].mean(axis=0)

def getStdQty(df):
    return df['quantity'].std(axis=0)

def getTotalVolume(df):
    return df['quantity'].sum()

def getTotalCurrencyVolume(df):
    return (df['quantity']*df['price']).sum()

def getQuantityLastMinute(df,source,time):
    return df[(df['timestamp'] > (time - 60)) & (df['source'] == source)]['quantity'].sum()

def getLargestTrade(df):
    return df['quantity'].max(axis=0)

def setLabel(df,source,price):
    avgFuturePrice = float(df[df['source'] == source]['price'].mean())
    result = "flat"
    if price > avgFuturePrice * (1 + 0.005):
        result = "up"
    elif price < avgFuturePrice * (1 - 0.005):
        result = 'down'
    return result


collectionName = "MarketData"

client = MongoClient()
db = client.get_database('local')
collection = db.get_collection(collectionName)

i = 0

start_time = time.time()

bulk = db.test.initialize_unordered_bulk_op()

for arg in sys.argv:
    if i > 0:
        try:
            item = collection.find_one({"_id":ObjectId(arg)})
            timestamp = item['timestamp']
            source = item['source']
            price =  item['price']
            previousItems = collection.find({"timestamp":{"$in":list(reversed(range(timestamp-10800,timestamp-1)))}})
            df = pd.DataFrame(list(previousItems))
            dfUSD = df[df['source'].str.contains("USD")]
            futureItems = collection.find({"timestamp":{"$in":list(reversed(range(timestamp+1,timestamp+3600)))}})
            dfFuture = pd.DataFrame(list(futureItems))
            updateValues = {"numTrades":getNumTrades(df),
                            "maxPriceUSD":getMaxPrice(dfUSD)['price'].values[0],
                            "minPriceUSD":getMinPrice(dfUSD)['price'].values[0],
                            "maxPriceUSDSource":getMaxPrice(dfUSD)['source'].values[0],
                            "minPriceUSDSource":getMinPrice(dfUSD)['source'].values[0],
                            "maxPriceUSDTimeFrom":int(timestamp-getMaxPrice(dfUSD)['timestamp'].values[0]),
                            "minPriceUSDTimeFrom":int(timestamp-getMinPrice(dfUSD)['timestamp'].values[0]),
                            "quantityLastMinute":getQuantityLastMinute(df,source,timestamp),
                            "largestTrade":getLargestTrade(df),
                            "stdDevPriceUSD":getStdPrice(dfUSD),
                            "stdDevQuantityUSD":getStdQty(dfUSD),
                            "stdDevQuantity":getStdQty(df),
                            "totalVolumeUSD":getTotalCurrencyVolume(dfUSD),
                            "totalVolume":getTotalVolume(df),
                            "label": setLabel(dfFuture,source,price)}
            bulk.find({"_id":ObjectId(arg)}).update({"$set":updateValues})
        except:
            print(traceback.format_exc())
    i += 1

try:
    bulk.execute()
except:
    print(traceback.format_exc())
    exit(1)

print(" %s items in %s seconds ---" % (str(i), time.time() - start_time))