__author__ = 'cnocito'

import numpy as np
import pandas as pd
import math
import datetime
from pymongo import MongoClient
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
import pickle as pk
import traceback
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from scipy import stats
from sklearn.decomposition import pca


def DateToBar(date):
    return math.floor((date - datetime.datetime(1970, 1, 1)).total_seconds() / 60)


def FlattenBarData(data):
    result = {
        "bar": data["_id"]["bar"],
        "source": data["_id"]["source"],
        "volBTC": data["volBTC"],
        "volUSD": data["volUSD"],
        "minPrice": data["minPrice"],
        "maxPrice": data["maxPrice"],
        "numberTrades": data["numberTrades"],
        "open": data["open"],
        "close": data["close"],
    }
    return result


def GetDataSet(startbar, endbar, db):
    cursor = db.BarData.find({"$and": [{"_id.bar": {"$gte": startbar}}, {"_id.bar": {"$lte": endbar}}]})
    data = []
    for document in cursor:
        data.append(FlattenBarData(document))
    vec = DictVectorizer()
    dataSet = vec.fit_transform(data).toarray()
    columnNames = vec.get_feature_names()
    return pd.DataFrame(dataSet, columns=columnNames)


def LabelRow(dataSet):
    if dataSet['maxPrice'] >= dataSet['close_m1'] * 1.005:
        return 2
    elif dataSet['minPrice'] <= dataSet['close_m1'] * 0.995:
        return 0
    else:
        return 1


def ScoreConfusionMatrix(m):
    i = 0
    j = 0
    score = 0
    for row in m:
        for item in row:
            if j == 1:
                score += 0 * item
            elif i == 1:
                score += 0 * item
            elif i == j:
                score += 1 * item
            else:
                score += (-1) * item
            j += 1
        j = 0
        i += 1
    return score


client = MongoClient()
db = client.local

minutesPerDay = 1440
startBar = DateToBar(datetime.datetime(2013, 1, 1))
endBar = DateToBar(datetime.datetime(2015, 1, 1))
days = math.floor((endBar - startBar) / minutesPerDay)
learningPeriod = 30
testingPeriod = 8
evaluationPeriod = 1

for startDay in range(0, days - evaluationPeriod, 1):
    learningStartBar = startBar + (startDay * minutesPerDay)
    learningEndBar = learningStartBar + (learningPeriod * minutesPerDay)
    testingStartBar = learningEndBar + 1
    testingEndBar = testingStartBar + (testingPeriod * minutesPerDay)
    evaluationStartBar = testingEndBar + 1
    evaluationEndBar = evaluationStartBar + (evaluationPeriod * minutesPerDay)
    dataSet = GetDataSet(learningStartBar, evaluationEndBar, db)

    learningStartIdx = 0
    learningEndIdx = len(dataSet[dataSet['bar'] <= learningEndBar])
    testingStartIdx = learningEndIdx + 1
    testingEndIdx = testingStartIdx + len(
        dataSet[(dataSet['bar'] > testingStartBar) & (dataSet['bar'] <= testingEndBar)])
    evaluationStartIdx = testingEndIdx + 1
    evaluationEndIdx = evaluationStartIdx + len(
        dataSet[(dataSet['bar'] > evaluationStartBar) & (dataSet['bar'] <= evaluationEndBar)])

    print(learningStartIdx, learningEndIdx, testingStartIdx, testingEndIdx, evaluationStartIdx, evaluationEndIdx)

    X = dataSet['bar']
    dataSet.set_index('bar')
    tmp1 = dataSet
    tmp1['bar'] = tmp1['bar'] + 1
    tmp1.set_index('bar')
    X = dataSet.join(tmp1, how='left', rsuffix="_m1")
    tmp2 = dataSet
    tmp2['bar'] = tmp2['bar'] + 2
    tmp2.set_index('bar')
    X = X.join(tmp2, how='left', rsuffix="_m2")
    tmp2 = None
    tmp3 = dataSet
    tmp3['bar'] = tmp3['bar'] + 3
    X = X.join(tmp3, how='left', rsuffix="_m3")
    tmp3 = None
    tmp4 = dataSet
    tmp4['bar'] = tmp4['bar'] + 4
    X = X.join(tmp4, how='left', rsuffix="_m4")
    tmp4 = None
    tmp5 = dataSet
    tmp5['bar'] = tmp5['bar'] + 5
    X = X.join(tmp5, how='left', rsuffix="_m5")
    tmp5 = None
    labelTmp = dataSet.join(tmp1, how='left', rsuffix="_m1")
    Y = labelTmp.apply(LabelRow, axis=1)
    labelTmp = None
    tmp1 = None

    c0 = len(Y[Y == 0])
    c1 = len(Y[Y == 1])
    c2 = len(Y[Y == 2])

    w0 = 1 / (c0 / (c0 + c1 + c2))
    w1 = 1 / (c1 / (c0 + c1 + c2))
    w2 = 1 / (c2 / (c0 + c1 + c2))

    clf = RandomForestClassifier(
        n_estimators=1000,
        min_samples_leaf=10,
        n_jobs=-1,
        class_weight={0: w0, 1: w1, 2: w2},
        #oob_score=True,
    )

    reducedFeatureVector = pca.PCA(n_components=64)
    reducedFeatureVector.fit(X.values[learningStartIdx:learningEndIdx])

    clf = clf.fit(reducedFeatureVector.transform(X.values[learningStartIdx:learningEndIdx]), Y.values[learningStartIdx:learningEndIdx])
    Y_Pred = clf.predict(reducedFeatureVector.transform(X.values[learningStartIdx:learningEndIdx]))
    print("Training Performance")
    print(classification_report(Y.values[learningStartIdx:learningEndIdx], Y_Pred,
                                labels=[0, 1, 2], target_names=['down', 'flat', 'up']))
    #print(confusion_matrix(Y.values[learningStartIdx:learningEndIdx], Y_Pred[learningStartIdx:learningEndIdx]))
    print("Net winning trade rate: ", ScoreConfusionMatrix(confusion_matrix(Y.values[learningStartIdx:learningEndIdx], Y_Pred[learningStartIdx:learningEndIdx]))/len(Y_Pred))
    print("******************* OVER *******************")
    print("Testing Performance")
    Y_Test = clf.predict(reducedFeatureVector.transform(X.values[testingStartIdx:testingEndIdx]))
    print(classification_report(Y.values[testingStartIdx:testingEndIdx], Y_Test,
                                target_names=['down', 'flat', 'up']))
    #print(confusion_matrix(Y.values[testingStartIdx:testingEndIdx], Y_Test))
    print("Net winning trade rate: ", ScoreConfusionMatrix(confusion_matrix(Y.values[testingStartIdx:testingEndIdx], Y_Test))/len(Y_Test))
    print("Wilcoxon rank-sum: ", stats.wilcoxon(Y.values[testingStartIdx:testingEndIdx], Y_Test))
    print("******************* OVER *******************")
