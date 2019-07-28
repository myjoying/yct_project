# encoding: UTF-8

import json
from datetime import datetime
from time import time, sleep
import baostock as bs
import pandas as pd
import talib as ta

from yct.common.vtConstant import *
from yct.common.vtObject import *


class GetHistoryData:
    def __init__(self, setting, dboperator):
        self.setting = setting
        self.SYMBOLS = self.setting['backtesting']['symbols']
        self.START = self.setting['backtesting']["start"]
        self.END = self.setting['backtesting']["end"]
        self.FREQS = self.setting['backtesting']["freq"]
        self.dbop = dboperator


    # ----------------------------------------------------------------------
    def generateVtBar(self,row):
        """生成K线"""
        bar = VtBarData()

        bar.symbol = row['code']
        # bar.exchange = generateExchange(bar.symbol)
        bar.vtSymbol = '.'.join([bar.symbol])
        bar.open = row['open']
        bar.high = row['high']
        bar.low = row['low']
        bar.close = row['close']
        # bar.volume = row['vol']
        bar.volume = row['volume']

        if 'date' in row:
            bar.date = row['date']
        if 'time' in row:
            bar.time = row['time']
            bar.datetime = datetime.strptime(row['time'], "%Y%m%d%H%M%S%f")
        else:
            bar.datetime = datetime.strptime(row['date'], "%Y-%m-%d")

        # bar.datetime = row.name

        # bar.date = bar.datetime.strftime("%Y%m%d")
        # bar.time = bar.datetime.strftime("%H:%M:%S")

        return bar


    # ----------------------------------------------------------------------
    def downBarBySymbol(self, symbol, start_date=None, end_date=None, freq='d'):
        """下载某一合约的分钟线数据"""
        start = time.time()

        # 登陆系统
        lg = bs.login()
        # 显示登陆返回信息
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)

        db_freq = freq
        req_colume = "date,code,open,high,low,close,volume,amount,adjustflag"
        if (freq[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']):
            if (freq[-1] not in ['N']):
                db_freq = freq + 'MIN'
            else:
                freq = freq[0:-3]
            req_colume = "date,time,code,open,high,low,close,volume,amount,adjustflag"

        if (freq.startswith('d')):
            db_freq = 'D'

        self.dbop.dbEnsureIndex(DB_NAME_DICT[db_freq],symbol, 'datetime' )



        # 时间字符串
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')


        rs = bs.query_history_k_data_plus(symbol, req_colume, start_date, end_date, frequency=freq, adjustflag="2")

        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)

        for ix, row in df.iterrows():
            bar = self.generateVtBar(row)
            d = bar.__dict__
            flt = {'datetime': bar.datetime}
            self.dbop.dbUpdate(DB_NAME_DICT[db_freq], symbol, d, flt, upsert=True)


        end = time.time()
        cost = (end - start) * 1000

        print(u'合约%s 周期%s数据下载完成%s - %s，耗时%s毫秒' % (symbol, freq, df.index[0], df.index[-1], cost))


    # ----------------------------------------------------------------------
    def downloadAllSymbols(self):
        """下载所有配置中的合约的K线数据"""
        print('-' * 50)
        print(u'开始下载合约K线数据')
        print('-' * 50)

        # 添加下载任务
        start_date = datetime.strptime(self.START, "%Y-%m-%d")
        end_date = datetime.strptime(self.END, "%Y-%m-%d")

        failure_dict = {}
        for symbol in self.SYMBOLS:
            for freq in self.FREQS:
                try:
                    self.downBarBySymbol(str(symbol), start_date, end_date, freq)
                except Exception as e:
                    print('exception:\t', repr(e))
                    print('首次下载失败：%s--%s' % (symbol, freq))
                    if symbol not in failure_dict.keys():
                        freq_list = []
                        freq_list.append(freq)
                        failure_dict[symbol] = freq_list
                    else:
                        freq_list = failure_dict[symbol]
                        freq_list.append(freq)
                        failure_dict[symbol] = freq_list

        for symbol, freq_list in failure_dict.items():
            for freq in freq_list:
                print('补充下载:%s--%s' % (symbol, freq))
                try:
                    self.downBarBySymbol(str(symbol), start_date, end_date, freq)
                except:
                    print('下载失败：%s--%s' % (symbol, freq))

        print('-' * 50)
        print(u'合约K线数据下载完成')
        print('-' * 50)


    def indexGenerateAndStore(self):
        for symbol in self.SYMBOLS:
            for freq in self.FREQS:
                print(u'开始指标计算合约%s 周期%s' % (symbol, freq))

                db_freq = freq
                if (freq[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']):
                    if (freq[-1] not in ['N']):
                        db_freq = freq + 'MIN'

                if (freq.startswith('d')):
                    db_freq = 'D'

                try:
                    #db = self.mc[DB_NAME_DICT[db_freq]]
                    collection = self.dbop.getdbCollection(DB_NAME_DICT[db_freq], symbol)
                    data = pd.DataFrame(list(collection.find()))

                    data['open'] = data['open'].astype('float64')
                    data['close'] = data['close'].astype('float64')
                    data['high'] = data['high'].astype('float64')
                    data['low'] = data['low'].astype('float64')

                    data['SMA5'] = ta.SMA(data['close'].values, timeperiod=5)  # 5日均线
                    data['SMA10'] = ta.SMA(data['close'].values, timeperiod=10)  # 10日均线
                    macd_talib, signal, hist = ta.MACD(data['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
                    data['DIF'] = macd_talib  # DIF
                    data['DEA'] = signal  # DEA
                    data['MACD'] = hist  # MACD

                    macd_talib2, signal2, hist2 = ta.MACD(data['close'].values, fastperiod=24, slowperiod=52,
                                                          signalperiod=9)
                    data['DIF2'] = macd_talib2  # DIF
                    data['DEA2'] = signal2  # DEA
                    data['MACD2'] = hist2  # MACD

                    for item in data.index:
                        collection.update_one({'_id': data.ix[item, '_id']}, {'$set': {'SMA5': data.ix[item, 'SMA5'], \
                                                                                       'SMA10': data.ix[item, 'SMA10'], \
                                                                                       'DIF': data.ix[item, 'DIF'], \
                                                                                       'DEA': data.ix[item, 'DEA'], \
                                                                                       'MACD': data.ix[item, 'MACD'], \
                                                                                       'DIF2': data.ix[item, 'DIF2'], \
                                                                                       'DEA2': data.ix[item, 'DEA2'], \
                                                                                       'MACD2': data.ix[item, 'MACD2']}})
                except Exception as e:
                    print(repr(e))
                    print(u'%s计算%s失败。' % (symbol, db_freq))


    def getHistoryDataHandler(self):
        self.downloadAllSymbols()
        self.indexGenerateAndStore()
