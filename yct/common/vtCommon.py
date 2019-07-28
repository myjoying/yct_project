# encoding: UTF-8

import numpy as np
import talib
from .vtObject import *

########################################################################
class BarGenerator(object):
    """
    K线合成器，支持：
    1. 基于Tick合成1分钟K线
    2. 基于1分钟K线合成X分钟K线（X可以是2、3、5、10、15、30	）
    """

    # ----------------------------------------------------------------------
    def __init__(self, onBar, xmin=0, onXminBar=None):
        """Constructor"""
        self.bar = None  # 1分钟K线对象
        self.onBar = onBar  # 1分钟K线回调函数

        # self.xminBar = None         # X分钟K线对象
        # self.xmin = xmin            # X的值
        # self.onXminBar = onXminBar  # X分钟K线的回调函数

        self.xminBar_dict = {}  # X分钟处理对象所需要的字典（X分钟K线对象，X的值，X分钟K线的回调函数 ）
        if xmin > 0:
            self.xminBar_dict[str(xmin)] = {'bar': None, 'freq': xmin, 'func': onXminBar}

        self.lastTick = None  # 上一TICK缓存对象

    # ----------------------------------------------------------------------
    def addXminBarGenerator(self, xmin, onXminBar):
        if xmin > 0:
            self.xminBar_dict[str(xmin)] = {'bar': None, 'freq': xmin, 'func': onXminBar}

            # ----------------------------------------------------------------------

    def updateTick(self, tick):
        """TICK更新"""
        newMinute = False  # 默认不是新的一分钟

        # 尚未创建对象
        if not self.bar:
            self.bar = VtBarData()
            newMinute = True
        # 新的一分钟
        elif self.bar.datetime.minute != tick.datetime.minute:
            # 生成上一分钟K线的时间戳
            self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

            # 推送已经结束的上一分钟K线
            self.onBar(self.bar)
            self.updateBar(self.bar)

            # 创建新的K线对象
            self.bar = VtBarData()
            newMinute = True

        # 初始化新一分钟的K线数据
        if newMinute:
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange

            self.bar.open = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice
        # 累加更新老一分钟的K线数据
        else:
            self.bar.high = max(self.bar.high, tick.lastPrice)
            self.bar.low = min(self.bar.low, tick.lastPrice)

        # 通用更新部分
        self.bar.close = tick.lastPrice
        self.bar.datetime = tick.datetime
        self.bar.openInterest = tick.openInterest

        if self.lastTick:
            self.bar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量

        # 缓存Tick
        self.lastTick = tick

    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """1分钟K线更新"""
        for xminkey in self.xminBar_dict.keys():
            xminBar = self.xminBar_dict[xminkey]['bar']  # X分钟K线对象
            xmin = self.xminBar_dict[xminkey]['freq']  # X的值
            onXminBar = self.xminBar_dict[xminkey]['func']  # X分钟K线的回调函数

            # 尚未创建对象
            if not xminBar:
                xminBar = VtBarData()

                xminBar.vtSymbol = bar.vtSymbol
                xminBar.symbol = bar.symbol
                xminBar.exchange = bar.exchange

                xminBar.open = bar.open
                xminBar.high = bar.high
                xminBar.low = bar.low

                xminBar.datetime = bar.datetime  # 以第一根分钟K线的开始时间戳作为X分钟线的时间戳
            # 累加老K线
            else:
                xminBar.high = max(xminBar.high, bar.high)
                xminBar.low = min(xminBar.low, bar.low)

            # 通用部分
            xminBar.close = bar.close
            xminBar.openInterest = bar.openInterest
            xminBar.volume += int(bar.volume)

            # X分钟已经走完
            if not (bar.datetime.minute + 1) % xmin:  # 可以用X整除
                # 生成上一X分钟K线的时间戳
                new_minute = bar.datetime.minute + 1
                new_hour = bar.datetime.hour
                if new_minute >= 60:
                    new_minute = 0
                    new_hour = new_hour + 1

                bar.datetime = bar.datetime.replace(hour=new_hour, minute=new_minute, second=0,
                                                    microsecond=0)  # 将秒和微秒设为0
                xminBar.datetime = bar.datetime
                xminBar.date = bar.datetime.strftime('%Y%m%d')
                xminBar.time = bar.datetime.strftime('%H:%M:%S.%f')

                # 推送
                onXminBar(xminBar)

                # 清空老K线缓存对象
                xminBar = None

            self.xminBar_dict[xminkey]['bar'] = xminBar

        '''
        # 尚未创建对象
        if not self.xminBar:
            self.xminBar = VtBarData()

            self.xminBar.vtSymbol = bar.vtSymbol
            self.xminBar.symbol = bar.symbol
            self.xminBar.exchange = bar.exchange

            self.xminBar.open = bar.open
            self.xminBar.high = bar.high
            self.xminBar.low = bar.low            

            self.xminBar.datetime = bar.datetime    # 以第一根分钟K线的开始时间戳作为X分钟线的时间戳
        # 累加老K线
        else:
            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)

        # 通用部分
        self.xminBar.close = bar.close        
        self.xminBar.openInterest = bar.openInterest
        self.xminBar.volume += int(bar.volume)                

        # X分钟已经走完
        if not (bar.datetime.minute + 1) % self.xmin:   # 可以用X整除
            # 生成上一X分钟K线的时间戳
            self.xminBar.datetime = self.xminBar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.xminBar.date = self.xminBar.datetime.strftime('%Y%m%d')
            self.xminBar.time = self.xminBar.datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onXminBar(self.xminBar)

            # 清空老K线缓存对象
            self.xminBar = None
        '''


########################################################################
class ArrayManager(object):
    """
    K线序列管理工具，负责：
    1. K线时间序列的维护
    2. 常用技术指标的计算
    """

    # ----------------------------------------------------------------------
    def __init__(self, size=100):
        """Constructor"""
        self.count = 0  # 缓存计数
        self.size = size  # 缓存大小
        self.inited = False  # True if count>=size

        self.openArray = np.zeros(size)  # OHLC
        self.highArray = np.zeros(size)
        self.lowArray = np.zeros(size)
        self.closeArray = np.zeros(size)
        self.volumeArray = np.zeros(size)

    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        self.count += 1
        if not self.inited and self.count >= self.size:
            self.inited = True

        self.openArray[0:self.size - 1] = self.openArray[1:self.size]
        self.highArray[0:self.size - 1] = self.highArray[1:self.size]
        self.lowArray[0:self.size - 1] = self.lowArray[1:self.size]
        self.closeArray[0:self.size - 1] = self.closeArray[1:self.size]
        self.volumeArray[0:self.size - 1] = self.volumeArray[1:self.size]

        self.openArray[-1] = bar.open
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low
        self.closeArray[-1] = bar.close
        self.volumeArray[-1] = bar.volume

    # ----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self.openArray

    # ----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self.highArray

    # ----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self.lowArray

    # ----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self.closeArray

    # ----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self.volumeArray

    # ----------------------------------------------------------------------
    def sma(self, n, array=False):
        """简单均线"""
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def std(self, n, array=False):
        """标准差"""
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def cci(self, n, array=False):
        """CCI指标"""
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def rsi(self, n, array=False):
        """RSI指标"""
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def macd(self, fastPeriod, slowPeriod, signalPeriod, array=False):
        """MACD指标"""
        macd, signal, hist = talib.MACD(self.close, fastPeriod,
                                        slowPeriod, signalPeriod)
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    # ----------------------------------------------------------------------
    def adx(self, n, array=False):
        """ADX指标"""
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def boll(self, n, dev, array=False):
        """布林通道"""
        mid = self.sma(n, array)
        std = self.std(n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

        # ----------------------------------------------------------------------

    def keltner(self, n, dev, array=False):
        """肯特纳通道"""
        mid = self.sma(n, array)
        atr = self.atr(n, array)

        up = mid + atr * dev
        down = mid - atr * dev

        return up, down

    # ----------------------------------------------------------------------
    def donchian(self, n, array=False):
        """唐奇安通道"""
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)

        if array:
            return up, down
        return up[-1], down[-1]


