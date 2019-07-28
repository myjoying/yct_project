# encoding: UTF-8


from __future__ import division

from interval import Interval
import traceback
import json
import pandas as pd
import numpy as np
import datetime as dt
from bson import ObjectId
from datetime import datetime, timedelta
from pymongo import MongoClient
from threading import Thread
from time import sleep
try:
    import cPickle as pickle
except ImportError:
    import pickle

import shelve
    
from yct.common.vtCommon import *
from yct.common.vtConstant import *
from yct.common.vtMongoDB import *
from .yctCentralbase import CentralBaseSet,Centralbase
from yct.event import *

class YctStrategy:

    #----------------------------------------------------------------------
    def __init__(self, symbol, eventEngine, dboperator, last_trading_day=None):
        """Constructor"""
        
        #最近交易日期
        self.last_trading_day = last_trading_day

        #最近的买入时机
        self.last_buy_time = None
        
        #最近的卖出时机
        self.last_sell_time = None
        
        self.vtSymbol = symbol

        self.eventEngine = eventEngine
        self.dbop = dboperator

        
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略"""
  
        self.writeLog(u'策略初始化')
        
        currentTime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  
        
        self.writeLog(u'上个交易日:' + str(self.last_trading_day))
        self.writeLog(u'当前日期:' + str(currentTime))
        
        self.writeLog(u'初始化5MIN数据')

        try:
            d = shelve.open('./dbshelve/history_5min/' + self.vtSymbol)
            self.central_base5 = d[str(self.last_trading_day)]
            self.central_base5.strategy = self
            self.central_base5.dbop = self.dbop
        except:
            self.central_base5 = CentralBaseSet('5MIN', self)
        finally:
            pass
        
        
        self.writeLog(u'初始化30MIN数据')

        try:
            d = shelve.open('./dbshelve/history_30min/' + self.vtSymbol)
            self.central_base30 = d[str(self.last_trading_day)]
            self.central_base30.strategy = self
            #self.central_base30.engine = self.ctaEngine
            self.central_base30.dbop = self.dbop
        except:
            self.central_base30 = CentralBaseSet('30MIN', self)
        finally:
            pass

        
        self.writeLog(u'初始化Daily数据')

        try:
            d = shelve.open('./dbshelve/history_d/' + self.vtSymbol)
            self.central_baseD = d[str(self.last_trading_day)]
            self.central_baseD.strategy = self
            #self.central_baseD.engine = self.ctaEngine
            self.central_baseD.dbop = self.dbop
        except:
            self.central_baseD = CentralBaseSet('D', self)
        finally:
            pass
 
            
            
        self.central_base5.setupperCBset(self.central_base30)
        self.central_base30.setlowerCBset(self.central_base5)
        self.central_base30.setupperCBset(self.central_baseD)
        self.central_baseD.setlowerCBset(self.central_base30) 
        
        #数据库配置
        self.central_base5.setDBInfo(CHT_NODE_5_DB_NAME, CHT_CB_5_DB_NAME, self.vtSymbol)
        self.central_base30.setDBInfo(CHT_NODE_30_DB_NAME, CHT_CB_30_DB_NAME, self.vtSymbol)
        self.central_baseD.setDBInfo(CHT_NODE_D_DB_NAME, CHT_CB_D_DB_NAME, self.vtSymbol)

    
    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeLog(u'%s策略启动' % self.vtSymbol)

        # 注册5分钟处理策略
        self.eventEngine.register(EVENT_BAR_5 + self.vtSymbol, self.processFiveBarEvent)
    
    #----------------------------------------------------------------------
    def onStop(self, stopdate):
        """停止策略（必须由用户继承实现）"""
        self.writeLog(u'策略停止')
        self.writeLog(u'保存5MIN数据')
        '''
        if self.ctaEngine.runmode == self.ctaEngine.RUN_REALTIME_MODE:
            recordTime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  
        else:
            recordTime = self.ctaEngine.dataEndDate
        '''
        d = shelve.open('./dbshelve/history_5min/' + self.vtSymbol)
        d[str(stopdate)] = self.central_base5
        d.close()
        
        self.writeLog(u'保存30MIN数据')
        d = shelve.open('./dbshelve/history_30min/'  + self.vtSymbol)
        d[str(stopdate)] = self.central_base30
        d.close()        
        
        self.writeLog(u'保存Daily数据')
        d = shelve.open('./dbshelve/history_d/' + self.vtSymbol)
        d[str(stopdate)] = self.central_base30
        d.close()   

    #----------------------------------------------------------------------
    
    def onFiveBar(self, bar):
        #判断风险控制点
        if self.central_baseD.bottom_bc_is_ongoing:
            if bar.close <=  self.central_baseD.control_price:
                self.central_baseD.init_low_beichi_trend()
            
        flag = self.central_base5.am.updateBarAndTime(bar, bar.datetime)
        if flag>0:
            self.central_base5.getNodeList_KLine_Step()

    #----------------------------------------------------------------------

    def processFiveBarEvent(self, event):
        print('data')
        bar_data = event.dict_['data']
        self.onFiveBar(bar_data)
    

    def writeLog(self, content):
        """记录CTA日志"""
        content = self.vtSymbol + ':' + content
        #self.ctaEngine.writeLog(content)




