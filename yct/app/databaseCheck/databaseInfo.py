# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
import pandas as pd

from yct.common.vtConstant import DATABASE_NAMES



def printDBInfo(setting):
    MONGO_HOST = setting['globalsetting']['mongoHost']
    MONGO_PORT = setting['globalsetting']['mongoPort']

    mc = MongoClient(MONGO_HOST, MONGO_PORT)  # Mongo连接
    for database in DATABASE_NAMES:
        print('DATABASE [' + database + ']' + ' INFO:')
        db = mc[database]  # 数据库
        print("COLLECTION:")
        print(db.collection_names())
        for collection in db.collection_names():
            print('  ' + collection + ': %d' % (db[collection].find().count()))
            for u in db[collection].find():
                print(u)
                break
                # df = pd.DataFrame(list(db[collection].find()))
                # print(df)





