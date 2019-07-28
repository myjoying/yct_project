from yct.common.vtCommon import *
from yct.common.vtConstant import *
from yct.common.vtMongoDB import *
from yct.app.getHistoryData.getHistoryData import GetHistoryData
from yct.app.getRtimeData.getRtimeData import GetRtimeData
from yct.app.dataAnalysis.yctStrategy import *
from yct.event import *
from datetime import datetime, time

class MainEngine:
    def __init__(self, setting):

        self.setting = setting
        self.yctstratgylist = {}
        #self.thread = Thread(target=self.run)  # 线程

        self.eventEngine =  EventEngine2()

        self.dbop = MongoDBOperator(self.setting['globalsetting']['mongoHost'],
                               self.setting['globalsetting']['mongoPort'])
        self.dbop.dbConnect()


    def startAnalysis(self):
        if self.setting['isrealtime']:
            self.realtimeAnalysis()
        else:
            self.historyAnalysis()

    def realtimeAnalysis(self):
        SYMBOLS = self.setting['realtime']['symbols']

        realtime = GetRtimeData(self.setting, self.dbop, self.eventEngine)
        self.eventEngine.register(EVENT_TIMER, realtime.qryRealtimeData)


        DAY_START = time(8, 45)  # 日盘启动和停止时间
        DAY_END = time(15, 30)

        NIGHT_START = time(20, 45)  # 夜盘启动和停止时间
        NIGHT_END = time(2, 45)

        isactive = False

        # 判断当前处于的时间段
        today = datetime.now().strftime('%Y-%m-%d')

        if today in self.setting['trading_day']:
            pos = self.setting['trading_day'].index(today)
            if pos>0:
                last_trading_day = self.setting['trading_day'][pos-1]
            else:
                last_trading_day =None
            for symbol in SYMBOLS:
                self.yctstratgylist[symbol] = YctStrategy(symbol, self.eventEngine, self.dbop, last_trading_day=last_trading_day)
                self.yctstratgylist[symbol].onInit()
                self.yctstratgylist[symbol].onStart()

        while True:
            currentTime = datetime.now().time()
            if (currentTime >= DAY_START and currentTime <= DAY_END):
                if not isactive:
                    self.eventEngine.start()
                    isactive = True
            else:
                if isactive:
                    self.eventEngine.stop()
                    for symbol in SYMBOLS:
                        self.yctstratgylist[symbol].onStop()
                    isactive = False
            sleep(1)



    def historyAnalysis(self):
        SYMBOLS = self.setting['backtesting']['symbols']
        START = self.setting['backtesting']["start"]
        END = self.setting['backtesting']["end"]

        if self.setting['backtesting']['download']:
            history = GetHistoryData(self.setting, self.dbop)
            history.getHistoryDataHandler()

        for symbol in SYMBOLS:
            index = 0
            self.yctstratgylist[symbol] = YctStrategy(symbol, self.eventEngine,self.dbop)
            self.yctstratgylist[symbol].onInit()
            start_time = datetime.strptime(START, "%Y-%m-%d")
            end_time = datetime.strptime(END, "%Y-%m-%d")
            flt = {'datetime': {'$gte': start_time,
                                '$lte': end_time}}

            data_list = self.dbop.dbQuery(DB_NAME_DICT['5MIN'], symbol,flt,'datetime')

            for data in data_list:
                if(index % 50==0):
                    print('...progressing: ', (symbol, index))
                bar_data = VtBarData()
                bar_data.__dict__ = data
                bar_data.open = float(bar_data.open)
                bar_data.close = float(bar_data.close)
                bar_data.high = float(bar_data.high)
                bar_data.low = float(bar_data.low)
                bar_data.volume = float(bar_data.volume)
                self.yctstratgylist[symbol].onFiveBar(bar_data)
                index+=1
            self.yctstratgylist[symbol].onStop(END)


