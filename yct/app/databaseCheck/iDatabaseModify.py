# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
import pandas as pd
import talib as ta

from vnpy.trader.app.ctaStrategy.ctaBase import *



# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']

mc = MongoClient(MONGO_HOST, MONGO_PORT)        # Mongo连接

NODE_DB_NAME_DICT = {
'1MIN': '',
'5MIN':CHT_NODE_5_DB_NAME,
'15MIN':'',
'30MIN':CHT_NODE_30_DB_NAME,
'60MIN':'',
'D':CHT_NODE_D_DB_NAME,
'W':''
}

CB_DB_NAME_DICT = {
'1MIN': '',
'5MIN':CHT_CB_5_DB_NAME,
'15MIN':'',
'30MIN':CHT_CB_30_DB_NAME,
'60MIN':'',
'D':CHT_CB_D_DB_NAME,
'W':''
}

DB_NAME_DICT = {
'1MIN': MINUTE_DB_NAME,
'5MIN':MINUTE_5_DB_NAME,
'15MIN':MINUTE_15_DB_NAME,
'30MIN':MINUTE_30_DB_NAME,
'60MIN':MINUTE_60_DB_NAME,
'D':DAILY_DB_NAME,
'W':WEEKLY_DB_NAME
}

#SYMBOLS = ['601333', '601100', '002230','600115','002352','600233','000002','600104','300146']
SYMBOLS = ['000001']

FREQS = ['5MIN', '30MIN', 'D']

def indexGeneratorAndStore(data, collection):

    data['SMA5'] = ta.SMA(data['close'].values, timeperiod = 5)  #5日均线
    data['SMA10'] = ta.SMA(data['close'].values, timeperiod = 10)  #10日均线
    macd_talib, signal, hist = ta.MACD(data['close'].values,fastperiod=12, slowperiod=26, signalperiod=9 )
    data['DIF'] = macd_talib #DIF
    data['DEA'] = signal #DEA
    data['MACD'] = hist #MACD 
    
    macd_talib2, signal2, hist2 = ta.MACD(data['close'].values,fastperiod=24, slowperiod=52, signalperiod=9 )
    data['DIF2'] = macd_talib2 #DIF
    data['DEA2'] = signal2 #DEA
    data['MACD2'] = hist2 #MACD     
    
    for item in data.index:
        collection.update_one({'_id':data.ix[item, '_id']}, {'$set':{'SMA5':data.ix[item, 'SMA5'],\
                                                                     'SMA10':data.ix[item, 'SMA10'],\
                                                                     'DIF':data.ix[item, 'DIF'],\
                                                                     'DEA':data.ix[item, 'DEA'],\
                                                                     'MACD':data.ix[item, 'MACD'],\
                                                                     'DIF2':data.ix[item, 'DIF2'],\
                                                                     'DEA2':data.ix[item, 'DEA2'],\
                                                                     'MACD2':data.ix[item, 'MACD2']                                                                     }})

        



for symbol in SYMBOLS:
    for freq in FREQS:
        print u'开始加载合约%s 周期%s' %(symbol, freq)
        
        db = mc[DB_NAME_DICT[freq]]
        collection = db[symbol]
        data =  pd.DataFrame(list(collection.find()))
        
        indexGeneratorAndStore(data, collection)
        

        
        


