from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from datetime import datetime, timedelta

##############################################################
class MongoDBOperator:
    def __init__(self, mongoHost="localhost", mongoPort=27017):
        self.mongoHost = mongoHost
        self.mongoPort = mongoPort
        self.dbClient = None
        #self.mc = MongoClient(self.mongoHost, self.mongoPort)        # Mongo连接

    # ----------------------------------------------------------------------
    def dbConnect(self):
        """连接MongoDB数据库"""
        if not self.dbClient:
            # 读取MongoDB的设置
            try:
                # 设置MongoDB操作的超时时间为0.5秒
                self.dbClient = MongoClient(self.mongoHost, self.mongoPort,connectTimeoutMS=500)

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()


            except ConnectionFailure:
                self.dbClient = None

                #self.writeLog(text.DATABASE_CONNECTING_FAILED)

    # ----------------------------------------------------------------------
    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.insert_one(d)


    # ----------------------------------------------------------------------
    def dbQuery(self, dbName, collectionName, flt, sortKey='', sortDirection=ASCENDING, limitnum=None):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]

            if limitnum:
                if sortKey:
                    cursor = collection.find(flt).sort(sortKey, sortDirection).limit(limitnum)  # 对查询出来的数据进行排序
                else:
                    cursor = collection.find(flt).limit(limitnum)
            else:
                if sortKey:
                    cursor = collection.find(flt).sort(sortKey, sortDirection)  # 对查询出来的数据进行排序
                else:
                    cursor = collection.find(flt)

            if cursor:
                return list(cursor)
            else:
                return []
        else:
            return []

    # ----------------------------------------------------------------------
    def dbUpdate(self, dbName, collectionName, d, flt={}, upsert=False):
        """向MongoDB中更新数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.replace_one(flt, d, upsert)
            # collection.update_one(flt, d, upsert)


    # ----------------------------------------------------------------------
    def dbDelete(self, dbName, collectionName, flt={}):
        """向MongoDB中删除数据，flt是过滤条件"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.delete_many(flt)

            # ----------------------------------------------------------------------

    def loadDataRange(self, dbName, collectionName, start_time, end_time, forward_days=0):
        '''
        加载选定日期范围[start_time-forward_days, end_time]的数据
        '''
        if self.dbClient:
            collection = self.dbClient[dbName][collectionName]
            r_start_time = start_time - timedelta(forward_days)
            flt = {'datetime': {'$gte': r_start_time,
                                '$lte': end_time}}
            return collection.find(flt).sort('datetime')
        else:
            return None


    def dbEnsureIndex(self, dbName, collectionName, index, sortDirection=ASCENDING, unique=True):
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.ensure_index([(index, sortDirection)], unique=unique)  # 添加索引



    def getdbCollection(self, dbName, collectionName):
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]

            return collection