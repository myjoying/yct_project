import traceback
import json
import sys

from yct.app.getHistoryData.getHistoryData import GetHistoryData
from yct.app.getRtimeData.getRtimeData import GetRtimeData
from yct.app.databaseCheck.databaseInfo import *
from yct.event import *
from yct.common.vtMongoDB import *
from yct.run.mainEngine import MainEngine

settingFileName = "../setting/g_setting.json"

globalSetting = {}      # 全局配置字典

try:
    with open(settingFileName, 'rb') as f:
        setting = f.read()
        if type(setting) is not str:
            setting = str(setting, encoding='utf8')
        globalSetting = json.loads(setting)
except:
    traceback.print_exc()

#----------------------------------------------
#获取数据库信息
#----------------------------------------------
def getDatabaseInfo():
    printDBInfo(globalSetting)

#----------------------------------------------
#清除数据库信息
#----------------------------------------------
def deleteDatabase():
    dbop = MongoDBOperator(globalSetting['globalsetting']['mongoHost'], globalSetting['globalsetting']['mongoPort'])
    dbop.dbConnect()

    for dbname in DATABASE_NAMES:
        for symbol in globalSetting['backtesting']['symbols']:
            dbop.dbDelete(dbname,symbol)



if __name__ == '__main__':

    mainEngine = MainEngine(globalSetting)

    mainEngine.startAnalysis()

    #deleteDatabase()

    getDatabaseInfo()
