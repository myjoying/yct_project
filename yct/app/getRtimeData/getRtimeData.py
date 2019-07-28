# encoding: UTF-8

import tushare as ts
from datetime import datetime
import numpy as np
from pymongo import MongoClient, ASCENDING
from threading import Thread
from queue import Queue, Empty
#from pymongo.errors import DuplicateKeyError
import pandas as pd


from yct.common.vtConstant import *
from yct.common.vtObject import *
from yct.common.vtCommon import *
from yct.event.eventEngine import Event

tick_tushare_map = {
    # 代码相关
    "symbol": "code",  # 合约代码
    # "exchange" : EMPTY_STRING            # 交易所代码
    "vtSymbol": "code",  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

    # 成交数据
    "lastPrice": "price",  # 最新成交价
    "lastVolume": "volume",  # 最新成交量
    # "volume" : EMPTY_INT                 # 今天总成交量
    # openInterest = EMPTY_INT           # 持仓量
    "time": "time",  # 时间 11:20:56.5
    "date": "date",  # 日期 20151009
    # self.datetime = None                    # python的datetime时间对象

    # 常规行情
    "openPrice": "open",  # 今日开盘价
    "highPrice": "high",  # 今日最高价
    "lowPrice": "low",  # 今日最低价
    "preClosePrice": "pre_close",

    # self.upperLimit = EMPTY_FLOAT           # 涨停价
    # self.lowerLimit = EMPTY_FLOAT           # 跌停价

    # 五档行情
    "bidPrice1": "b1_p",
    "bidPrice2": "b2_p",
    "bidPrice3": "b3_p",
    "bidPrice4": "b4_p",
    "bidPrice5": "b5_p",

    "askPrice1": "a1_p",
    "askPrice2": "a2_p",
    "askPrice3": "a3_p",
    "askPrice4": "a4_p",
    "askPrice5": "a5_p",

    "bidVolume1": "b1_v",
    "bidVolume2": "b2_v",
    "bidVolume3": "b3_v",
    "bidVolume4": "b4_v",
    "bidVolume5": "b5_v",

    "askVolume1": "a1_v",
    "askVolume2": "a2_v",
    "askVolume3": "a3_v",
    "askVolume4": "a4_v",
    "askVolume5": "a5_v",

}


class GetRtimeData:
    def __init__(self, setting, dboperator, eventEngine):
        self.setting = setting
        #self.MONGO_HOST = self.setting['globalsetting']['mongoHost']
        #self.MONGO_PORT = self.setting['globalsetting']['mongoPort']
        self.SYMBOLS = self.setting['realtime']['symbols']
        self.FREQS = self.setting['realtime']["freq"]
        #self.mc = MongoClient(self.MONGO_HOST, self.MONGO_PORT)        # Mongo连接
        self.dbop = dboperator
        self.eventEngine = eventEngine

        self.thread = Thread(target=self.run)  # 线程
        self.queue = Queue()  # 队列

        # K线合成器字典
        self.bgDict = {}

        # 指标计算
        self.symbolFreqData = {}


        for symbol in self.SYMBOLS:
            # 创建BarManager对象
            self.bgDict[symbol] = BarGenerator(self.onBar)
            self.addSymbolFreq(symbol, "1MIN")
        for freq in self.FREQS:
            if freq == "5":
                self.bgDict[symbol].addXminBarGenerator(5, self.onFiveBar)
                self.addSymbolFreq(symbol, "5MIN")
            elif freq == "30":
                self.bgDict[symbol].addXminBarGenerator(30, self.onThirtyBar)
                self.addSymbolFreq(symbol, "30MIN")

        self.start()


    # ----------------------------------------------------------------------
    def qryRealtimeData(self,event):
        """获取实时数据"""
        for symbol in self.SYMBOLS:

            ts_symbol = symbol.split('.')

            df = ts.get_realtime_quotes(ts_symbol[-1])


            if not df.empty:
                tick = VtTickData()

                for key in tick_tushare_map.keys():
                    tick.__dict__[key] = df.ix[0, tick_tushare_map[key]]

                tick.vtSymbol = symbol
                tick.datetime = datetime.strptime(df.ix[0, 'date'] + ' ' + df.ix[0, 'time'], "%Y-%m-%d %H:%M:%S")
                print("数据%s -- %s" % (symbol, tick.datetime))

                #self.onTick(tick)

                self.bgDict[symbol].updateTick(tick)

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """Tick更新"""
        vtSymbol = tick.vtSymbol

        if vtSymbol in self.SYMBOLS:
            self.insertData(TICK_DB_NAME, vtSymbol, tick)


    '''
    self.writeDrLog(text.TICK_LOGGING_MESSAGE.format(symbol=tick.vtSymbol,
                                             time=tick.time,
                                             last=tick.lastPrice,
                                             bid=tick.bidPrice1,
                                             ask=tick.askPrice1))
    '''




        # ----------------------------------------------------------------------

    def onBar(self, bar):
        """分钟线更新"""
        vtSymbol = bar.vtSymbol

        insert_data = self.generate_data_with_index(vtSymbol, "1MIN", bar)
        self.insertData(MINUTE_DB_NAME, vtSymbol, insert_data)


    '''
        self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
                                                        time=bar.time,
                                                        open=bar.open,
                                                        high=bar.high,
                                                        low=bar.low,
                                                        close=bar.close))
    '''



        # ----------------------------------------------------------------------

    def onFiveBar(self, bar):
        """分钟线更新"""
        vtSymbol = bar.vtSymbol
        insert_data = self.generate_data_with_index(vtSymbol, "5MIN", bar)
        self.insertData(MINUTE_5_DB_NAME, vtSymbol, insert_data)

        event = Event(EVENT_BAR_5+vtSymbol)
        event.dict_['data'] = bar
        self.eventEngine.put(event)

        '''
        self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
                                                        time=bar.time,
                                                        open=bar.open,
                                                        high=bar.high,
                                                        low=bar.low,
                                                        close=bar.close))  
        '''



        # ----------------------------------------------------------------------

    def onThirtyBar(self, bar):
        """分钟线更新"""
        vtSymbol = bar.vtSymbol
        insert_data = self.generate_data_with_index(vtSymbol, "30MIN", bar)
        self.insertData(MINUTE_30_DB_NAME, vtSymbol, insert_data)

        '''
        self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
                                                        time=bar.time,
                                                        open=bar.open,
                                                        high=bar.high,
                                                        low=bar.low,
                                                        close=bar.close))
        '''




    #----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """插入数据到数据库（这里的data可以是VtTickData或者VtBarData）"""
        if isinstance(data, dict):
            self.queue.put((dbName, collectionName, data))
        else:
            self.queue.put((dbName, collectionName, data.__dict__))

    # ----------------------------------------------------------------------
    def run(self):
        """运行插入线程"""
        while True:
            try:
                dbName, collectionName, d = self.queue.get(block=True, timeout=1)

                # 这里采用MongoDB的update模式更新数据，在记录tick数据时会由于查询
                # 过于频繁，导致CPU占用和硬盘读写过高后系统卡死，因此不建议使用
                # flt = {'datetime': d['datetime']}
                # self.mainEngine.dbUpdate(dbName, collectionName, d, flt, True)

                # 使用insert模式更新数据，可能存在时间戳重复的情况，需要用户自行清洗
                self.dbop.dbInsert(dbName, collectionName, d)

            except Empty:
                pass


    def start(self):
        self.thread.start()

    def stop(self):
        self.thread.join()

    def addSymbolFreq(self, symbol, freq, size=50):
        key = symbol + '_' + freq
        self.symbolFreqData[key] = ArrayManager(size=size)
        data_list = []

        collection = self.dbop.getdbCollection(DB_NAME_DICT[freq], symbol)

        data_list = collection.find({}).sort('datetime', ASCENDING).limit(size)

        if np.size(data_list) >= size:
            data_list = data_list[-1 * size:]

        for data in data_list:
            bar = VtBarData()
            bar.close = data['close']
            bar.open = data['open']
            bar.high = data['high']
            bar.low = data['low']
            self.symbolFreqData[key].updateBar(bar)

    def generate_data_with_index(self, symbol, freq, data):
        """生成对应的指标"""

        key = symbol + '_' + freq

        insertdata = data.__dict__
        if key in self.symbolFreqData.keys():
            self.symbolFreqData[key].updateBar(data)

            insertdata['SMA5'] = self.symbolFreqData[key].sma(5)
            insertdata['SMA10'] = self.symbolFreqData[key].sma(10)
            insertdata['DIF'], insertdata['DEA'], insertdata['MACD'] = self.symbolFreqData[key].macd(fastPeriod=12, slowPeriod=26,
                                                                                         signalPeriod=9)
            insertdata['DIF2'], insertdata['DEA2'], insertdata['MACD2'] = self.symbolFreqData[key].macd(fastPeriod=24,
                                                                                            slowPeriod=52,
                                                                                            signalPeriod=9)

        return insertdata


