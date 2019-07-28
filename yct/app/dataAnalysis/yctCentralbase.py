from interval import Interval
import pandas as pd
import datetime as dt
from bson import ObjectId
from datetime import datetime
from yct.common.vtCommon import *
from yct.common.vtConstant import *

# 方向
M_TO_UP = True
M_TO_DOWN = False

# 节点或中枢是否正式形成
M_FORMAL = True
M_TEMP = False

# 顶点或底点
M_TOP = 1
M_BOTTOM = -1
M_NOBEICHI = 0

# 常量
M_MAX_VAL = 5000
M_MIN_VAL = -5000
M_INVALID_INDEX = -1

# 背驰点或买卖点的判定
M_NODECIDE = 0
M_FALSE = -1
M_TRUE = 1

# 背驰点分类
M_BEICHI_LASTTWO = 1  # 正式节点判定的背驰
M_BEICHI_SHARE = 2  # 与低级别共享背驰点
M_BEICHI_PANZH = 3  # 盘整背驰点
M_BEICHI_PZ_SHARE = 4  # 与低级别共享盘整背驰点

# 交易控制
M_BEICHI_SHARE_TOP = 1
M_BEICHI_SHARE_BOTTOM = 2
M_BEICHI_PZ_SHARE_TOP = 3
M_BEICHI_PZ_SHARE_BOTTOM = 4

# 交易方向
M_BUY = 1
M_SELL = -1

# 最高使用次节点生成背驰的级别的祖父级别
GRANDPA_CB_LEVER = []


class CtCycleArray():
    """
    支持下标操作，但不支持切片操作
    """

    def __init__(self, size=100):
        self.arraysize = size
        self.realsize = 0
        self.data = []

    def __str__(self):
        return "realsize=" + str(self.realsize) + "capacity=" + str(self.arraysize) + " " + str(self.data)

    def append(self, obj):
        if self.realsize >= self.arraysize:
            first = self.data[0]
            self.data.remove(self.data[0])
        self.data.append(obj)
        self.realsize += 1

    def size(self):
        return self.realsize

    def __getitem__(self, i):
        if i < 0 or self.realsize <= self.arraysize:
            return self.data[i]
        else:
            index = i - (self.realsize - self.arraysize)
            if index >= 0:
                return self.data[index]
            else:
                return self.data[self.realsize]

    def __len__(self):
        return self.realsize


class CtArrayManager(ArrayManager):
    def __init__(self, size=100):
        super(CtArrayManager, self).__init__(size)
        self.timeArray = [datetime(2000, 1, 1) for i in range(size)]

    def updateBarAndTime(self, bar, time):
        if time <= self.timeArray[-1]:
            return 0
        self.updateBar(bar)
        self.timeArray[0:self.size - 1] = self.timeArray[1:self.size]
        self.timeArray[-1] = time
        return 1

    def time(self):
        return self.timeArray


class Node:
    """
    趋势节点，包含（时间， 值）
    low_id:次级中枢编号
    low_count:当前点累计的次级中枢数目
    ntype: 顶点或低点
    isformal：正式或临时节点
    """

    def __init__(self, time, value, ntype, low_id=None, low_count=None, isformal=M_TEMP):
        self.datetime = time
        self.value = value
        self.ntype = ntype
        self.low_id = low_id
        self.low_count = low_count
        self.isformal = isformal

        # 背驰点描述相关
        self.btype = M_NOBEICHI  # 背驰类型，正数为顶背驰 负数为底背驰, 0--非背驰点
        self.classfier = M_NOBEICHI  # 背驰点分类
        self.real_beichi = M_NOBEICHI

        self._id = ObjectId()


class Centralbase:
    """
    中枢定义
    start：开始时间
    end：结束时间
    up：上边界
    down: 下边界
    start_node_id: 开始ID
    end_node_id: 结束ID
    ctype:类型计数
    isformal：正式或临时节点
    """

    def __init__(self, start, end, up, down, start_node_id=None, end_node_id=None, isformal=M_TEMP):
        self.start = start
        self.end = end
        self.up = up
        self.down = down
        self.start_node_id = start_node_id
        self.end_node_id = end_node_id
        self.ctype = 0
        self.isformal = isformal
        self.max_val = M_MIN_VAL
        self.min_val = M_MAX_VAL
        self.max_node_id = M_INVALID_INDEX
        self.min_node_id = M_INVALID_INDEX
        self._id = ObjectId()

    def getCBInterval(self):
        return Interval(lower_bound=self.down, upper_bound=self.up,
                        lower_closed=False, upper_closed=False)


class CentralBaseSet:
    def __init__(self, freq, strategy, isTop=False, datasize=20, nodesize=100, cbsize=20, beichisize=5):
        #self.engine = strategy.ctaEngine
        self.dbop = strategy.dbop
        self.strategy = strategy
        self.freq = freq
        self.isTop = isTop

        # 分析相关
        self.am = CtArrayManager(datasize)
        self.node_list = CtCycleArray(nodesize)
        self.centralbase_list = CtCycleArray(cbsize)

        # 背驰点
        self.beichi_list = CtCycleArray(beichisize)
        self.share_beichi_list = CtCycleArray(beichisize)
        self.beichi_pz_list = CtCycleArray(beichisize)
        self.share_beichi_pz_list = CtCycleArray(beichisize)

        self.seek_max = None
        self.low_CB_set = None
        self.up_CB_set = None

        # 数据库相关
        self.nodedbName = None  # 节点数据库
        self.cbdbName = None  # 中枢数据库
        self.bcdbName = None  # 背驰数据库
        self.dbSymbol = None

        # 中枢升级逻辑
        self.cur_cut_low_id = -1
        self.cur_cut_start_node_id = -1

        # 当前趋势中的极值信息
        self.cur_min_value = M_MAX_VAL
        self.cur_min_node_id = 0
        self.cur_max_value = M_MIN_VAL
        self.cur_max_node_id = 0

        # 底背驰后判断卖点相关
        self.share_bc_count = 0
        self.share_bc_flag = M_NODECIDE  # 共享背驰的分类
        self.bottom_bc_is_ongoing = False
        self.bc_start_node_id = 0
        self.bc_cur_max_value = M_MIN_VAL
        self.bc_cur_max_node_id = 0

        # 交易控制
        self.share_bottom_bc_is_ongoing = False
        self.share_top_bc_is_ongoing = False
        self.buy_already_flag = False
        self.step_pos = 100
        self.total_pos = 0
        self.control_price = 0  # 控制价格

    # ------------------------------------------------------------------------
    def __getstate__(self):

        attrs = ["freq", "isTop", "am", "node_list", "centralbase_list", "beichi_list", "share_beichi_list",
                 "beichi_pz_list", "share_beichi_pz_list", "seek_max", "low_CB_set", "up_CB_set", "nodedbName",
                 "cbdbName", "bcdbName",
                 "dbSymbol", "cur_cut_low_id", "cur_cut_start_node_id", "cur_min_value", "cur_min_node_id",
                 "cur_max_value", "cur_max_node_id",
                 "share_bc_count", "share_bc_flag", "bottom_bc_is_ongoing", "bc_start_node_id", "bc_cur_max_value",
                 "bc_cur_max_node_id", "share_bottom_bc_is_ongoing",
                 "share_top_bc_is_ongoing", "buy_already_flag", "step_pos", "total_pos", "control_price"]

        return dict((attr, getattr(self, attr)) for attr in attrs)

    def __setstate__(self, state):
        for name, value in state.items():
            setattr(self, name, value)

    # ------------------------------------------------------------------------
    def setDBInfo(self, nodedbName, cbdbName, dbSymbol):
        self.nodedbName = nodedbName
        self.cbdbName = cbdbName
        self.dbSymbol = dbSymbol

        self.dbop.dbDelete(self.nodedbName, self.dbSymbol)
        self.dbop.dbDelete(self.cbdbName, self.dbSymbol)


    # ------------------------------------------------------------------------
    def setlowerCBset(self, low_CB_set):
        '''
        设置下级中枢集
        '''
        self.low_CB_set = low_CB_set

    # ------------------------------------------------------------------------
    def setupperCBset(self, up_CB_set):
        '''
        设置上级中枢集
        '''
        self.up_CB_set = up_CB_set

    # ------------------------------------------------------------------------
    def getNodeList_KLine_Step(self):

        open_price = self.am.openArray[-1]
        close_price = self.am.closeArray[-1]
        time = self.am.timeArray[-1]

        up_flag = open_price <= close_price

        if self.seek_max == None:  # 初始数据
            if up_flag:
                self.seek_max = M_TO_UP
                self.node_list.append(Node(time, close_price, M_TOP, isformal=M_TEMP))
            else:
                self.seek_max = M_TO_DOWN
                self.node_list.append(Node(time, close_price, M_BOTTOM, isformal=M_TEMP))

        go_judge = True
        if self.seek_max == M_TO_UP:
            if abs(close_price - open_price) <= 0.001:  # 排查十字星的情况
                if close_price >= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
                else:
                    go_judge = False
            if up_flag and go_judge:
                if close_price >= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
            else:
                if close_price < self.node_list[-1].value:
                    # 新Node形成
                    self.node_list[-1].isformal = M_FORMAL

                    # 正式节点记入数据库
                    if len(self.node_list) > 0:
                        self.__updateData(self.nodedbName, self.dbSymbol, self.node_list[-1])


                    self.node_list.append(Node(time, close_price, M_BOTTOM, isformal=M_TEMP))
                    self.seek_max = M_TO_DOWN

                    # 触发中枢计算
                    self.get_Centralbase_Step()

        else:
            if abs(close_price - open_price) <= 0.001:  # 排查十字星的情况
                if close_price <= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
                else:
                    go_judge = False
            if (not up_flag) and go_judge:
                if close_price <= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
            else:
                if close_price > self.node_list[-1].value:
                    # 新Node形成
                    self.node_list[-1].isformal = M_FORMAL

                    # 正式节点记入数据库
                    if len(self.node_list) > 0:
                        self.__updateData(self.nodedbName, self.dbSymbol, self.node_list[-1])

                    self.node_list.append(Node(time, close_price, M_TOP, isformal=M_TEMP))
                    self.seek_max = M_TO_UP
                    # 触发中枢计算
                    self.get_Centralbase_Step()

    # ------------------------------------------------------------------------
    def getNodeList_Lower_Step(self):
        lower_CB_list = self.low_CB_set.centralbase_list
        length = len(lower_CB_list)
        index = length - 1

        if length < 2:
            return

        pre_base = lower_CB_list[-2]
        base = lower_CB_list[-1]

        if (length == 2) and len(self.node_list) == 0:
            self.seek_max = M_TO_UP
            if 1 == self.__get_CB_pos(pre_base, base):
                self.seek_max = M_TO_DOWN
            else:
                self.seek_max = M_TO_UP

            # 生成新临时节点
            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, base.start_node_id, base.end_node_id, index)
            return

        if self.cur_cut_low_id != index:
            self.cur_cut_low_id = index
            self.cur_cut_start_node_id = base.start_node_id

        cur_base_start_node_id = self.cur_cut_start_node_id
        cur_base_end_node_id = base.end_node_id

        '''
        #中枢升级逻辑

        if (cur_base_end_node_id - cur_base_start_node_id)==9:



            self.node_list.pop()
            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, cur_base_start_node_id+3, index)
            self.node_list[-1].isformal = M_FORMAL
            cur_base_start_node_id = cur_base_start_node_id+3

            self.seek_max=self.__reverse_direct(self.seek_max)

            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, cur_base_start_node_id+3, index)
            self.node_list[-1].isformal = M_FORMAL
            cur_base_start_node_id = cur_base_start_node_id+3

            #进行中枢计算
            self.get_Centralbase_Step() 
            self.update_max_min_value()

            self.seek_max=self.__reverse_direct(self.seek_max)
            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, cur_base_start_node_id+3, index)
            cur_base_start_node_id = cur_base_start_node_id+3
            self.cur_cut_start_node_id = cur_base_start_node_id
            return
        '''
        if self.node_list[-1].isformal == M_FORMAL and (
                base.start <= self.node_list[-1].datetime and base.end >= self.node_list[-1].datetime):
            return

        if self.seek_max == M_TO_UP:  # 向上
            # 当前中枢在前一中枢下或相交，当前趋势结束
            if ((0 < self.__get_CB_pos(pre_base, base)) and (index > self.node_list[-1].low_id)):
                # 更新正式节点信息
                # self.__Update_Last_Node_Lower_WithID(self.seek_max, pre_base.start, pre_base.end, isformal=M_FORMAL)
                self.node_list[-1].isformal = M_FORMAL

                # 更新底背弛趋势
                self.update_LowBeichi_Trend_With_FormalNode()

                # 正式节点记入数据库
                if len(self.node_list) > 0:
                    self.__updateData(self.nodedbName, self.dbSymbol, self.node_list[-1])

                # 生成新临时节点
                self.seek_max = M_TO_DOWN
                self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id, index)

                # 更新底背弛趋势
                self.update_LowBeichi_Trend_With_TempNode()

                # 触发中枢计算
                self.get_Centralbase_Step()

            else:  # 趋势延续
                self.__Update_Last_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id,
                                                     isformal=M_TEMP, low_id=index)
                # 更新底背弛趋势
                self.update_LowBeichi_Trend_With_TempNode()
        else:
            # 当前中枢在前一中枢上或相交，当前趋势结束
            if ((0 > self.__get_CB_pos(pre_base, base)) and (index > self.node_list[-1].low_id)):
                # 更新正式节点信息
                # self.__Update_Last_Node_Lower(self.seek_max, pre_base.start, pre_base.end, isformal=M_FORMAL)
                self.node_list[-1].isformal = M_FORMAL

                # 更新底背弛趋势
                self.update_LowBeichi_Trend_With_FormalNode()

                # 正式节点记入数据库
                if len(self.node_list) > 0:
                    self.__updateData(self.nodedbName, self.dbSymbol, self.node_list[-1])

                # 生成新临时节点
                self.seek_max = M_TO_UP
                self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id, index)

                # 更新底背弛趋势
                self.update_LowBeichi_Trend_With_TempNode()

                # 触发中枢计算
                self.get_Centralbase_Step()

            else:  # 趋势延续
                self.__Update_Last_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id,
                                                     isformal=M_TEMP, low_id=index)

                # 更新底背弛趋势
                self.update_LowBeichi_Trend_With_TempNode()

    # ------------------------------------------------------------------------
    def get_Centralbase_Step(self):
        '''
        有效逻辑时机：
        1.首个中枢；
        2.背驰处理；
        3.形成新的临时节点和正式节点
        '''
        seg_list = []
        start = None
        end = None
        start_id = -1
        end_id = -1
        cross_itval = Interval.none

        if len(self.centralbase_list) == 0:  # 首个中枢
            if len(self.node_list) > 3:
                cross_itval = self.__getSegment(0) & self.__getSegment(1)
                start = self.__getSegmentStart(0)
                end = self.__getSegmentEnd(1)
                newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, 0, 2,
                                       isformal=M_TEMP)
                newcbase.ctype = self.__getCBType(newcbase)
                newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(0, 2)
                newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(0, 2)
                self.centralbase_list.append(newcbase)
        else:
            end_node_id = self.centralbase_list[-1].end_node_id
            start_node_id = self.centralbase_list[-1].start_node_id
            if len(self.node_list) - 2 > end_node_id:  # 新临时NODE已经形成，新正式NODE形成
                cross_itval = self.centralbase_list[-1].getCBInterval() & self.__getSegment(end_node_id)
                if cross_itval != Interval.none():  # 新正式段与原中枢相交，更新中枢信息
                    # if end_node_id-start_node_id >=4 :
                    ##切割中枢
                    # self.centralbase_list[-1].isformal = M_FORMAL
                    # cross_itval = self.__getSegment(start_node_id) & self.__getSegment(start_node_id+2)
                    # self.centralbase_list[-1].up = cross_itval.upper_bound
                    # self.centralbase_list[-1].down = cross_itval.lower_bound
                    # self.centralbase_list[-1].end_node_id = start_node_id+3
                    # self.centralbase_list[-1].end = self.node_list[start_node_id+3].datetime
                    # self.centralbase_list[-1].max_node_id, self.centralbase_list[-1].max_val = self.__getMaxNode_Val(start_node_id, start_node_id+3)
                    # self.centralbase_list[-1].min_node_id, self.centralbase_list[-1].min_val = self.__getMinNode_Val(start_node_id, start_node_id+3)

                    ##添加新中枢
                    # cross_itval = self.centralbase_list[-1].getCBInterval() & self.__getSegment(start_node_id+3) & self.__getSegment(start_node_id+4)
                    # start = self.node_list[start_node_id+3].datetime
                    # end = self.node_list[end_node_id+1].datetime
                    # newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, start_node_id+3, end_node_id+1, isformal=M_TEMP)
                    # newcbase.setCType(self.__getCBType(newcbase))
                    # newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(start_node_id+3, end_node_id+1)
                    # newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(start_node_id+3, end_node_id+1)
                    # self.centralbase_list.append(newcbase)
                    # else:
                    self.centralbase_list[-1].up = cross_itval.upper_bound
                    self.centralbase_list[-1].down = cross_itval.lower_bound
                    self.centralbase_list[-1].end_node_id = end_node_id + 1
                    self.centralbase_list[-1].end = self.node_list[end_node_id + 1].datetime
                    # self.centralbase_list[-1].setCType(self.__getCBType(newcbase=None, isnew=False, cb_id=len(self.centralbase_list)-1))

                    # 判断是否是盘整背驰点
                    if self.isTop:
                        self.getBeichi_PanZheng_step()

                    # 更新本中枢极值信息
                    if self.node_list[end_node_id + 1].value > self.centralbase_list[-1].max_val:
                        self.centralbase_list[-1].max_val = self.node_list[end_node_id + 1].value
                        self.centralbase_list[-1].max_node_id = end_node_id + 1
                    if self.node_list[end_node_id + 1].value < self.centralbase_list[-1].min_val:
                        self.centralbase_list[-1].min_val = self.node_list[end_node_id + 1].value
                        self.centralbase_list[-1].min_node_id = end_node_id + 1



                else:
                    self.centralbase_list[-1].isformal = M_FORMAL
                    # 添加新中枢
                    cross_itval = self.__getSegment(end_node_id)
                    start = self.node_list[end_node_id].datetime
                    end = self.node_list[end_node_id + 1].datetime
                    newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, end_node_id,
                                           end_node_id + 1, isformal=M_TEMP)
                    newcbase.ctype = self.__getCBType(newcbase)
                    newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(end_node_id, end_node_id + 1)
                    newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(end_node_id, end_node_id + 1)
                    self.centralbase_list.append(newcbase)

                if self.share_bottom_bc_is_ongoing and not self.bottom_bc_is_ongoing:
                    self.__start_bc_calc(len(self.node_list) - 2, self.node_list[-2].value)
                    # self.share_bottom_bc_is_ongoing = False

                if self.share_top_bc_is_ongoing:
                    self.init_low_beichi_trend()
                    self.share_top_bc_is_ongoing = False



                    # 更新趋势极值信息
        if self.low_CB_set != None:
            self.update_max_min_value()

        # 驱动上层级别的节点计算
        if self.up_CB_set != None:
            self.up_CB_set.getNodeList_Lower_Step()

        # 每次计算更新数据库
        if len(self.centralbase_list) > 0:
            self.__updateData(self.cbdbName, self.dbSymbol, self.centralbase_list[-1])


    # ------------------------------------------------------------------------
    def getBeichi_LastTwo_Step(self, isMinOrNot=True):
        '''
        分步获取背驰节点
        返回当前中枢新加入节点是否为背驰点
        调用时机：
        新的正式节点是极值点，并未更新此中枢的极值信息
        '''

        cur_macd = 0
        pre_macd = 0
        cur_macd_lower = 0
        pre_macd_lower = 0
        isbeichi = False


        if isMinOrNot:  # 创新低
            isbeichi = self.__beichi_judge(self.node_list[self.cur_min_node_id - 1].datetime,
                                            self.node_list[self.cur_min_node_id].datetime, \
                                            self.node_list[-3].datetime, self.node_list[-2].datetime, False)
            if isbeichi:
                self.beichi_list.append(
                    Node(self.node_list[-2].datetime, self.node_list[-2].value, self.node_list[-2].ntype))
                self.beichi_list[-1].btype = M_BOTTOM  # 底背弛
                self.beichi_list[-1].classfier = M_BEICHI_LASTTWO

                # 底背弛后趋势判断
                if not self.bottom_bc_is_ongoing:
                    self.__start_bc_calc(len(self.node_list) - 2, self.node_list[-2].value)

                    # 触发买入交易
                    # if self.isTop:
                    # order_id = self.strategy.buy(10000, self.step_pos, False, self.node_list[-2].datetime)
                    # if order_id>=0:
                    # self.total_pos += self.step_pos


        else:  # 创新高
            isbeichi = self.__beichi_judge(self.node_list[self.cur_max_node_id - 1].datetime,
                                            self.node_list[self.cur_max_node_id].datetime, \
                                            self.node_list[-3].datetime, self.node_list[-2].datetime, True)
            if isbeichi:
                self.beichi_list.append(
                    Node(self.node_list[-2].datetime, self.node_list[-2].value, self.node_list[-2].ntype))
                self.beichi_list[-1].btype = M_TOP  # 顶背驰
                self.beichi_list[-1].classfier = M_BEICHI_LASTTWO

                # 触发卖出交易
                if self.isTop:
                    if self.total_pos > 0:
                        order_id = self.strategy.sell(0, self.total_pos, False, self.node_list[-2].datetime)
                        if order_id >= 0:
                            self.total_pos = 0

        if isbeichi:
            # 判定该背驰点是否是上级的背驰点
            if self.up_CB_set != None and self.up_CB_set.isTop:
                self.up_CB_set.getBeichi_Share_With_LowBeichi_Step(self.beichi_list[-1])

            # 背驰节点记入数据库
            self.__updateData(self.nodedbName, self.dbSymbol, self.beichi_list[-1])

        return isbeichi

    # ------------------------------------------------------------------------
    def getBeichi_Share_With_LowBeichi_Step(self, low_beichi):
        if self.low_CB_set == None or low_beichi == None:
            return

        if len(self.centralbase_list) <= 1:
            return

        base = self.centralbase_list[-1]

        low_node_time = low_beichi.datetime
        low_node_value = low_beichi.value
        isbeichi = False
        isbeichi_pz = False

        # 判定趋势背驰
        if (base.ctype <= -2):
            if low_node_value < self.cur_min_value:  # 创新低
                isbeichi = self.__beichi_judge2(self.node_list[self.cur_min_node_id - 1].datetime,
                                                self.node_list[self.cur_min_node_id].datetime, \
                                                self.node_list[-2].datetime, low_node_time, False)
                if isbeichi:
                    self.share_beichi_list.append(Node(low_beichi.datetime, low_beichi.value, low_beichi.ntype))
                    self.share_beichi_list[-1].btype = M_BOTTOM  # 底背弛
                    self.share_beichi_list[-1].classfier = M_BEICHI_SHARE
                    self.share_bottom_bc_is_ongoing = True
                    self.buy_already_flag = False

                    if self.share_bc_flag != M_BEICHI_SHARE_BOTTOM:
                        self.share_bc_count = 1
                        self.share_bc_flag = M_BEICHI_SHARE_BOTTOM
                    else:
                        self.share_bc_count += 1


                        ##触发买入交易
                        # if self.isTop:
                        # order_id = self.strategy.buy(10000, self.step_pos, False, low_beichi.datetime)
                        # if order_id>=0:
                        # self.total_pos += self.step_pos
                        # self.share_bottom_bc_is_ongoing = True

        # 判定趋势背驰
        if (base.ctype >= 2):
            if low_node_value > self.cur_max_value:  # 创新高
                isbeichi = self.__beichi_judge2(self.node_list[self.cur_max_node_id - 1].datetime,
                                                self.node_list[self.cur_max_node_id].datetime, \
                                                self.node_list[-2].datetime, low_node_time, True)
                if isbeichi:
                    self.share_beichi_list.append(Node(low_beichi.datetime, low_beichi.value, low_beichi.ntype))
                    self.share_beichi_list[-1].btype = M_TOP  # 顶背弛
                    self.share_beichi_list[-1].classfier = M_BEICHI_SHARE
                    self.share_top_bc_is_ongoing = True

                    if self.share_bc_flag != M_BEICHI_SHARE_TOP:
                        self.share_bc_count = 1
                        self.share_bc_flag = M_BEICHI_SHARE_TOP
                    else:
                        self.share_bc_count += 1

                    # 触发卖出交易
                    if self.isTop:
                        if self.total_pos >= self.step_pos:
                            order_id = self.strategy.sell(0, self.step_pos, False, low_beichi.datetime)
                            if order_id >= 0:
                                self.total_pos -= self.step_pos

        # 判定盘整背驰
        if (not isbeichi) and (self.centralbase_list[-1].end_node_id - self.centralbase_list[-1].start_node_id > 2):
            if low_node_value < self.centralbase_list[-1].min_val:  # 创新低
                isbeichi_pz = self.__beichi_judge2(self.node_list[self.centralbase_list[-1].min_node_id - 1].datetime,
                                                   self.node_list[self.centralbase_list[-1].min_node_id].datetime, \
                                                   self.node_list[-2].datetime, low_node_time, False)
                if isbeichi_pz:
                    self.share_beichi_pz_list.append(Node(low_beichi.datetime, low_beichi.value, low_beichi.ntype))
                    self.share_beichi_pz_list[-1].btype = M_BOTTOM  # 底背弛
                    self.share_beichi_pz_list[-1].classfier = M_BEICHI_PZ_SHARE
                    self.share_bottom_bc_is_ongoing = True
                    self.buy_already_flag = False

                    if self.share_bc_flag != M_BEICHI_PZ_SHARE_BOTTOM:
                        self.share_bc_count = 1
                        self.share_bc_flag = M_BEICHI_PZ_SHARE_BOTTOM
                    else:
                        self.share_bc_count += 1

                        ##触发买入交易
                        # if self.isTop:
                        # order_id = self.strategy.buy(10000, self.step_pos, False, low_beichi.datetime)
                        # if order_id>=0:
                        # self.total_pos += self.step_pos
                        # self.share_bottom_bc_is_ongoing = True

            if low_node_value > self.centralbase_list[-1].max_val:  # 创新高
                isbeichi_pz = self.__beichi_judge2(self.node_list[self.centralbase_list[-1].max_node_id - 1].datetime,
                                                   self.node_list[self.centralbase_list[-1].max_node_id].datetime, \
                                                   self.node_list[-2].datetime, low_node_time, True)
                if isbeichi_pz:
                    self.share_beichi_pz_list.append(Node(low_beichi.datetime, low_beichi.value, low_beichi.ntype))
                    self.share_beichi_pz_list[-1].btype = M_TOP  # 顶背弛
                    self.share_beichi_pz_list[-1].classfier = M_BEICHI_PZ_SHARE
                    self.share_top_bc_is_ongoing = True

                    if self.share_bc_flag != M_BEICHI_PZ_SHARE_TOP:
                        self.share_bc_count = 1
                        self.share_bc_flag = M_BEICHI_PZ_SHARE_TOP
                    else:
                        self.share_bc_count += 1

                    # 触发卖出交易
                    if self.isTop:
                        if self.total_pos >= self.step_pos:
                            order_id = self.strategy.sell(0, self.step_pos, False, low_beichi.datetime)
                            if order_id >= 0:
                                self.total_pos -= self.step_pos

        if isbeichi:
            # 背驰节点记入数据库
            self.__updateData(self.nodedbName, self.dbSymbol, self.share_beichi_list[-1])

        if isbeichi_pz:
            # 背驰节点记入数据库
            self.__updateData(self.nodedbName, self.dbSymbol, self.share_beichi_pz_list[-1])

        return isbeichi or isbeichi_pz

    # ------------------------------------------------------------------------
    def getBeichi_PanZheng_step(self):
        '''
        盘整背驰
        '''
        if (len(self.centralbase_list) <= 1) or len(self.node_list) < 1:
            return False

        base = self.centralbase_list[-1]
        start_node_id = base.start_node_id
        end_node_id = base.end_node_id
        isbeichi = False
        if end_node_id - start_node_id > 2:
            if self.node_list[end_node_id].value < base.min_val:  # 创新低
                min_node_id = base.min_node_id
                isbeichi = self.__beichi_judge2(self.node_list[min_node_id - 1].datetime,
                                                self.node_list[min_node_id].datetime, \
                                                self.node_list[end_node_id - 1].datetime,
                                                self.node_list[end_node_id].datetime, False)
                if isbeichi:
                    self.beichi_pz_list.append(
                        Node(self.node_list[end_node_id].datetime, self.node_list[end_node_id].value,
                             self.node_list[end_node_id].ntype))
                    self.beichi_pz_list[-1].btype = M_BOTTOM  # 底背弛
                    self.beichi_pz_list[-1].classfier = M_BEICHI_PANZH

                    # 底背弛后趋势判断
                    if not self.bottom_bc_is_ongoing:
                        self.__start_bc_calc(end_node_id, self.node_list[end_node_id].value)


            elif self.node_list[end_node_id].value > base.max_val:  # 创新高
                max_node_id = base.max_node_id
                isbeichi = self.__beichi_judge2(self.node_list[max_node_id - 1].datetime,
                                                self.node_list[max_node_id].datetime, \
                                                self.node_list[end_node_id - 1].datetime,
                                                self.node_list[end_node_id].datetime, True)
                if isbeichi:
                    self.beichi_pz_list.append(
                        Node(self.node_list[end_node_id].datetime, self.node_list[end_node_id].value,
                             self.node_list[end_node_id].ntype))
                    self.beichi_pz_list[-1].btype = M_TOP  # 顶背弛
                    self.beichi_pz_list[-1].classfier = M_BEICHI_PANZH

                    # 触发卖出交易
                    if self.isTop:
                        if self.total_pos > 0:
                            order_id = self.strategy.sell(0, self.total_pos, False,
                                                          self.node_list[end_node_id].datetime)
                            if order_id >= 0:
                                self.total_pos = 0

        if isbeichi:
            # 背驰节点记入数据库
            self.__updateData(self.nodedbName, self.dbSymbol, self.beichi_pz_list[-1])

        return isbeichi

    # ------------------------------------------------------------------------
    def update_LowBeichi_Trend_With_TempNode(self):

        if self.freq == '30MIN':
            a = 1
        if self.freq == 'D':
            a = 1

        if self.bottom_bc_is_ongoing:
            # 共享盘整背驰时，当上升趋势临时节点高过当前中枢的上界时，控制价格定为下界
            if (len(self.node_list) - 1 == self.bc_start_node_id + 1):
                if self.share_bc_flag == M_BEICHI_PZ_SHARE_BOTTOM:
                    if self.node_list[self.bc_start_node_id + 1].value >= self.centralbase_list[-1].up:
                        if self.centralbase_list[-1].down >= self.control_price:
                            self.control_price = self.centralbase_list[-1].down

            # 上升趋势临时节点高于高点时，控制价格定为当前次高点
            if (len(self.node_list) - 2 > self.bc_start_node_id + 1) and (
                self.node_list[-1].value >= self.node_list[-2].value):
                if self.node_list[-1].value > self.node_list[-3].value:
                    if self.node_list[-2].value >= self.control_price:
                        self.control_price = self.node_list[-2].value

            # 下降趋势临时节点，控制价格定为当前次高节点
            if (len(self.node_list) - 2 >= self.bc_start_node_id + 1) and (
                self.node_list[-1].value <= self.node_list[-2].value):
                node_id = (
                self.bc_start_node_id if len(self.node_list) - 4 <= self.bc_start_node_id else len(self.node_list) - 4)
                if self.node_list[node_id].value >= self.control_price:
                    self.control_price = self.node_list[node_id].value
                if self.node_list[-1].value < self.node_list[node_id].value:  # 比前峰值低
                    self.init_low_beichi_trend()

    # ------------------------------------------------------------------------
    def update_LowBeichi_Trend_With_FormalNode(self):

        if self.freq == '30MIN':
            a = 1
        if self.freq == 'D':
            a = 1

        if self.bottom_bc_is_ongoing:
            if len(self.node_list) - 1 == self.bc_start_node_id + 2:  # 背驰后的第二个正式节点
                isReal = self.node_list[-1].value >= self.node_list[self.bc_start_node_id].value  # 背驰点成立
                if self.up_CB_set != None and self.up_CB_set.isTop:
                    self.up_CB_set.update_Share_Beichi_Property(isReal, self.node_list[-1])

            if self.bc_cur_max_node_id == 0:
                self.bc_cur_max_node_id = len(self.node_list) - 1
                self.bc_cur_max_value = self.node_list[self.bc_start_node_id + 1].value
                if self.share_bc_flag == M_BEICHI_PZ_SHARE_BOTTOM:
                    if (self.node_list[self.bc_start_node_id + 1].value > self.centralbase_list[-1].down) \
                            and (self.node_list[self.bc_start_node_id + 1].value < self.centralbase_list[-1].up):
                        self.init_low_beichi_trend()

                self.share_bc_flag = M_NODECIDE
            else:
                if self.node_list[-1].value >= self.bc_cur_max_value:  # 创新高
                    isBeichi = self.__beichi_judge2(self.node_list[self.bc_cur_max_node_id - 1].datetime,
                                                    self.node_list[self.bc_cur_max_node_id].datetime,
                                                    self.node_list[-2].datetime, self.node_list[-1].datetime, M_TO_UP)
                    if isBeichi:
                        self.init_low_beichi_trend()
                    else:
                        self.bc_cur_max_node_id = len(self.node_list) - 1
                        self.bc_cur_max_value = self.node_list[len(self.node_list) - 1].value

        if self.up_CB_set != None and self.up_CB_set.share_bottom_bc_is_ongoing:
            self.up_CB_set.update_Share_Beichi_Property(low_node=self.node_list[-1])

    # ------------------------------------------------------------------------
    def update_Share_Beichi_Property(self, isReal=True, low_node=None):
        if self.share_bottom_bc_is_ongoing:  # 正处在共享背驰的判定过程中

            if not isReal:
                self.share_bottom_bc_is_ongoing = False
                self.buy_already_flag = False

            if low_node != None:
                isbeichi, rightside = self.__beichi_rightside_judge(low_node.datetime, self.seek_max)
                if not isbeichi:
                    self.share_bottom_bc_is_ongoing = False
                    self.buy_already_flag = False
                else:
                    # 触发买入交易
                    if self.isTop and rightside and not self.buy_already_flag:
                        order_id = -1
                        if self.share_bc_flag == M_BEICHI_PZ_SHARE_BOTTOM:
                            order_id = self.strategy.buy(10000, self.step_pos, False,
                                                         self.share_beichi_pz_list[-1].datetime)
                        elif self.share_bc_flag == M_BEICHI_SHARE_BOTTOM:
                            order_id = self.strategy.buy(10000, self.step_pos, False,
                                                         self.share_beichi_list[-1].datetime)
                        if order_id >= 0:
                            self.total_pos += self.step_pos
                        self.buy_already_flag = True

    # ------------------------------------------------------------------------
    def __beichi_judge(self, pre_start_time, pre_end_time, cur_start_time, cur_end_time, seek_dirct):

        isBeichi = False
        r_pre_start_time = self.__roundTime(pre_start_time, self.freq)
        r_pre_end_time = self.__roundTime(pre_end_time, self.freq)
        r_cur_start_time = self.__roundTime(cur_start_time, self.freq)
        r_cur_end_time = self.__roundTime(cur_end_time, self.freq)

        isallsame = self.__isMACD_Same_Direct(r_pre_start_time, r_cur_end_time)

        # isallsame = True
        if isallsame:
            pre_macd_lower = self.__getMACD_Sum_Lower(pre_start_time, pre_end_time, seekMax=seek_dirct)
            cur_macd_lower = self.__getMACD_Sum_Lower(cur_start_time, cur_end_time, seekMax=seek_dirct)
            if abs(cur_macd_lower) < abs(pre_macd_lower):
                isBeichi = True
            else:
                isBeichi = False
        else:
            # pre_vol = self.__getVolumn_Sum(r_pre_start_time, r_pre_end_time, seekMax=seek_dirct)
            # cur_vol = self.__getVolumn_Sum(r_cur_start_time, r_cur_end_time, seekMax=seek_dirct)
            pre_macd = self.__getMACD_Sum(r_pre_start_time, r_pre_end_time, seekMax=seek_dirct)
            cur_macd = self.__getMACD_Sum(r_cur_start_time, r_cur_end_time, seekMax=seek_dirct)
            if abs(cur_macd) < abs(pre_macd):
                isBeichi = True
            elif abs(pre_macd) == 0:
                forward = 10 if len(self.centralbase_list) >= 10 else len(self.centralbase_list)
                isBeichi = self.__is_Smaller_Progress(self.centralbase_list[-1 * forward].start, r_cur_end_time,
                                                      seekMax=seek_dirct)
            else:
                isBeichi = False

        if isBeichi and self.isTop:
            abs_pre_macd = self.__get_abs_MACD_Sum(r_pre_start_time, r_pre_end_time, seekMax=seek_dirct)
            abs_cur_macd = self.__get_abs_MACD_Sum(r_cur_start_time, r_cur_end_time, seekMax=seek_dirct)
            if abs_cur_macd <= abs_pre_macd:
                isBeichi = True
            else:
                isBeichi = False

        return isBeichi

        # if  abs(cur_macd) < abs(pre_macd) and abs(cur_vol) < abs(pre_vol):
        # isBeichi = True
        # elif abs(cur_macd) > abs(pre_macd):
        # isBeichi = False
        # else:
        # pre_macd_lower = self.__getMACD_Sum_Lower(pre_start_time, pre_end_time, seekMax=seek_dirct)
        # cur_macd_lower = self.__getMACD_Sum_Lower(cur_start_time, cur_end_time, seekMax=seek_dirct)
        # if abs(cur_macd_lower) < abs(pre_macd_lower) :
        # isBeichi = True
        # else:
        # isBeichi = False

        # return isBeichi

        # if cur_macd==0 or pre_macd==0 :
        # pre_macd_lower = self.__getMACD_Sum_Lower(pre_start_time, pre_end_time, seekMax=seek_dirct)
        # cur_macd_lower = self.__getMACD_Sum_Lower(cur_start_time, cur_end_time, seekMax=seek_dirct)
        # if abs(cur_macd_lower) < abs(pre_macd_lower) :
        # return True
        # else:
        # return False
        # else:
        # if abs(cur_macd) < abs(pre_macd) :
        # return True
        # else:
        # return False

    # ------------------------------------------------------------------------
    def __beichi_judge2(self, pre_start_time, pre_end_time, cur_start_time, cur_end_time, seek_dirct):

        isBeichi = False
        r_pre_start_time = self.__roundTime(pre_start_time, self.freq)
        r_pre_end_time = self.__roundTime(pre_end_time, self.freq)
        r_cur_start_time = self.__roundTime(cur_start_time, self.freq)
        r_cur_end_time = self.__roundTime(cur_end_time, self.freq)

        forward = 10 if len(self.centralbase_list) >= 10 else len(self.centralbase_list)
        macd_seg, data_seg = self.__get_macd_seg(self.centralbase_list[-1 * forward].start, r_cur_end_time, seg=2,
                                                 seekMax=seek_dirct)

        if len(macd_seg) >= 2:
            pre_macd_seg = data_seg.ix[(data_seg.index >= macd_seg[1][1]) & (data_seg.index <= macd_seg[1][0]), 'MACD2']
            cur_macd_seg = data_seg.ix[(data_seg.index >= macd_seg[0][1]) & (data_seg.index <= macd_seg[0][0]), 'MACD2']
            if abs(cur_macd_seg.mean()) < abs(pre_macd_seg.mean()) or macd_seg[0][3] < macd_seg[1][3]:
                isBeichi = True

        return isBeichi

    # ------------------------------------------------------------------------
    def __beichi_rightside_judge(self, cur_end_time, seek_dirct):

        isBeichi = False
        right = False
        r_cur_end_time = self.__roundTime(cur_end_time, self.freq)

        forward = 10 if len(self.centralbase_list) >= 10 else len(self.centralbase_list)
        macd_seg, data_seg = self.__get_macd_seg(self.centralbase_list[-1 * forward].start, r_cur_end_time, seg=2,
                                                 seekMax=seek_dirct)

        if len(macd_seg) >= 2:
            pre_macd_seg = data_seg.ix[(data_seg.index >= macd_seg[1][1]) & (data_seg.index <= macd_seg[1][0]), 'MACD2']
            cur_macd_seg = data_seg.ix[(data_seg.index >= macd_seg[0][1]) & (data_seg.index <= macd_seg[0][0]), 'MACD2']
            if abs(cur_macd_seg.mean()) < abs(pre_macd_seg.mean()) or macd_seg[0][3] < macd_seg[1][3]:
                isBeichi = True

        if len(macd_seg) >= 1 and r_cur_end_time > macd_seg[0][2]:
            right = True

        return (isBeichi, right)

    # ------------------------------------------------------------------------
    def __get_abs_MACD_Sum(self, start_time, end_time, seekMax=True):

        #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
        data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol, start_time, end_time)))

        if data_seg.empty:
            return 0
        else:
            data_seg = data_seg['MACD']

        data_seg = data_seg.apply(lambda x: abs(x))

        return data_seg.mean()

        # ------------------------------------------------------------------------

    def __get_macd_seg(self, start_time, end_time, seg=1, seekMax=True):
        """
        返回分段元组数组和原始数据，其中分段元组组成包括(开始时间，结束时间，最大值时间，最大值绝对值)
        """
        data_index = 'MACD2'

        '''
        加载选定日期范围[start_time-forward_days, end_time]的数据
        '''
        flt = {'datetime': {'$gte': start_time,
                            '$lte': end_time}}

        #data_list = self.engine.queryData(DB_NAME_DICT[self.freq], self.dbSymbol, flt, 'datetime')
        data_list = self.dbop.dbQuery(DB_NAME_DICT[self.freq], self.dbSymbol, flt, 'datetime')
        if len(data_list) > 0:
            data_seg = pd.DataFrame(data_list)
        else:
            data_seg = pd.DataFrame()

        macd_seg = []
        if not data_seg.empty:
            data_seg = data_seg.sort_values(by='datetime', ascending=False)

            if seekMax:
                data_seg = data_seg[data_seg[data_index] > 0]
            else:
                data_seg = data_seg[data_seg[data_index] < 0]

            if not data_seg.empty:
                data_seg.set_index(keys='datetime', inplace=True)
                t_start = data_seg.index[0]
                t_max = data_seg.index[0]
                v_max = abs(data_seg.ix[0, data_index])
                t_end = data_seg.index[0]
                ins_progress = True
                for i in range(np.size(data_seg, axis=0) - 1):
                    v0 = abs(data_seg.ix[i, data_index])
                    v1 = abs(data_seg.ix[i + 1, data_index])

                    if v0 < v1:
                        if ins_progress:
                            t_end = data_seg.index[i + 1]
                            if v1 >= v_max:
                                v_max = v1
                                t_max = data_seg.index[i + 1]
                        else:
                            macd_seg.append((t_start, t_end, t_max, v_max))
                            if (len(macd_seg) >= seg):
                                break
                            t_start = data_seg.index[i + 1]
                            t_max = data_seg.index[i + 1]
                            v_max = v1
                            t_end = data_seg.index[i + 1]
                            ins_progress = True
                    else:
                        t_end = data_seg.index[i + 1]
                        ins_progress = False
                if (len(macd_seg) < seg):
                    macd_seg.append((t_start, t_end, t_max, v_max))

            return (macd_seg, data_seg)

    # ------------------------------------------------------------------------
    def init_low_beichi_trend(self):
        self.bottom_bc_is_ongoing = False
        self.share_bottom_bc_is_ongoing = False
        self.bc_start_node_id = 0
        self.bc_cur_max_value = M_MIN_VAL
        self.bc_cur_max_node_id = 0
        self.control_price = 0;

        # 触发卖出交易
        if self.isTop:
            if self.total_pos > 0:
                order_id = self.strategy.sell(0, self.total_pos, False)
                if order_id >= 0:
                    self.total_pos = 0

    # ------------------------------------------------------------------------
    def __getSegment(self, i):
        """
        i from 0
        """
        if i < 0 or i > len(self.node_list) - 1:
            return None
        return Interval(lower_bound=self.node_list[i].value, upper_bound=self.node_list[i + 1].value,
                        lower_closed=False, upper_closed=False)

    # ------------------------------------------------------------------------
    def __getSegmentStart(self, i):
        """
        i from 0
        """
        if i < 0 or i > len(self.node_list) - 1:
            return None
        return self.node_list[i].datetime

        # ------------------------------------------------------------------------

    def __getSegmentEnd(self, i):
        """
        i from 0
        """
        if i < 0 or i > len(self.node_list) - 1:
            return None
        return self.node_list[i + 1].datetime

    # ------------------------------------------------------------------------
    def __getMaxNode_Val(self, start_in, end_in):
        val = M_MIN_VAL
        val_index = -1
        for index in range(start_in, end_in + 1):
            if self.node_list[index].value > val:
                val = self.node_list[index].value
                val_index = index
        return (val_index, val)

    def __getMaxLowerNode_Val(self, start_in, end_in):
        val = M_MIN_VAL
        val_index = -1
        for index in range(start_in, end_in + 1):
            if self.low_CB_set.node_list[index].value > val:
                val = self.low_CB_set.node_list[index].value
                val_index = index
        return (val_index, val)

        # ------------------------------------------------------------------------

    def __getMinNode_Val(self, start_in, end_in):
        val = M_MAX_VAL
        val_index = -1
        for index in range(start_in, end_in + 1):
            if self.node_list[index].value < val:
                val = self.node_list[index].value
                val_index = index
        return (val_index, val)

        # ------------------------------------------------------------------------

    def __getMinLowerNode_Val(self, start_in, end_in):
        val = M_MAX_VAL
        val_index = -1
        for index in range(start_in, end_in + 1):
            if self.low_CB_set.node_list[index].value < val:
                val = self.low_CB_set.node_list[index].value
                val_index = index
        return (val_index, val)

        # ------------------------------------------------------------------------

    def __getCBType(self, newcbase, isnew=True, cb_id=None):
        '''

        :param newcbase: 中枢
        :param isnew: 新建立标志
        :param cb_id: 指定比较的中枢id
        :return: 中枢层级标记：首个0；依次增加或减少1，向上增加，向下减少，反向首个为±2
        '''
        if isnew:
            if (len(self.centralbase_list) < 1):
                return 0
            r_pos = self.__get_CB_pos(self.centralbase_list[-1], newcbase)
            pre_ctype = self.centralbase_list[-1].ctype
            # if pre_ctype==0:#前一个是起点或背驰形成的第一中枢
            #    return (-2*r_pos)
        else:
            if cb_id - 1 < 0:
                return 0
            r_pos = self.__get_CB_pos(self.centralbase_list[cb_id - 1], self.centralbase_list[cb_id])
            pre_ctype = self.centralbase_list[cb_id - 1].ctype
            if self.centralbase_list[cb_id].ctype == 0:  # 前一个是起点或背驰形成的第一中枢
                return 0

        if (0 == r_pos):
            return pre_ctype
        else:
            if ((r_pos * pre_ctype) > 0):  # 转折

                if self.centralbase_list[-1].end_node_id - self.centralbase_list[-1].start_node_id > 9:
                    return (-2 * r_pos)
                else:
                    return (-2 * r_pos)
            elif ((r_pos * pre_ctype) < 0):  # 延续
                return (pre_ctype - r_pos)
            else:
                return (-1 * r_pos)

                # ------------------------------------------------------------------------

    def __get_CB_pos(self, first, second):
        """
        获取两个中枢的相对位置:1前在后上，-1前在后下，0相交
        """
        # if (first.up <=second.down):
        # return -1
        # elif (first.down >=second.up) :
        # return 1
        # else:
        # return 0

        if (first.up < second.up) and (first.down <= second.down):
            return -1
        elif (first.down > second.down) and (first.up >= second.up):
            return 1
        else:
            return 0

            # ------------------------------------------------------------------------

    def __Make_New_Temp_Node_Lower(self, seek_max, low_base, low_id=None):
        '''
        生成新的临时节点
        seek_max:该临时节点与上一节点的关系
        '''
        if seek_max == M_TO_UP:
            time = self.low_CB_set.node_list[low_base.max_node_id].datetime
            value = low_base.max_val
            top_bottom = M_TOP
        else:
            time = self.low_CB_set.node_list[low_base.min_node_id].datetime
            value = low_base.min_val
            top_bottom = M_BOTTOM

        self.node_list.append(Node(time, value, top_bottom, low_id=low_id, isformal=M_TEMP))

    # ------------------------------------------------------------------------
    def __Make_New_Temp_Node_Lower_WithID(self, seek_max, start_node_id, end_node_id, low_id=None):
        '''
        生成新的临时节点
        seek_max:该临时节点与上一节点的关系
        '''
        lower_node_list = self.low_CB_set.node_list
        if seek_max == M_TO_UP:
            node_id, value = self.__getMaxLowerNode_Val(start_node_id, end_node_id)
            top_bottom = M_TOP
        else:
            node_id, value = self.__getMinLowerNode_Val(start_node_id, end_node_id)
            top_bottom = M_BOTTOM

        self.node_list.append(
            Node(lower_node_list[node_id].datetime, value, top_bottom, low_id=low_id, isformal=M_TEMP))

    # ------------------------------------------------------------------------
    def __Update_Last_Node_Lower(self, seek_max, start_time, end_time, isformal=None, low_id=None):
        '''
        更新最后节点信息
        seek_max:该临时节点与上一节点的关系
        '''
        lower_data = self.low_CB_set.data
        if seek_max == M_TO_UP:
            time, value = self.__getMaxIndex_Val(lower_data, start_time, end_time)
        else:
            time, value = self.__getMinIndex_Val(lower_data, start_time, end_time)

        if time == None:
            time_seg = self.data.ix[self.data.index > end_time, 'close']
            time = time_seg.index[0]
            value = self.data.ix[0, 'close']
        if ((seek_max == M_TO_UP) and (value > self.node_list[-1].value)) \
                or ((seek_max == M_TO_DOWN) and (value < self.node_list[-1].value)):
            self.node_list[-1].datetime = time
            self.node_list[-1].value = value
            if low_id != None:
                self.node_list[-1].low_id = low_id
        if isformal != None:
            self.node_list[-1].isformal = isformal

    # ------------------------------------------------------------------------
    def __Update_Last_Node_Lower_WithID(self, seek_max, start_node_id, end_node_id, isformal=None, low_id=None):
        '''
        更新最后节点信息
        seek_max:该临时节点与上一节点的关系
        '''
        lower_node_list = self.low_CB_set.node_list
        if seek_max == M_TO_UP:
            node_id, value = self.__getMaxLowerNode_Val(start_node_id, end_node_id)
        else:
            node_id, value = self.__getMinLowerNode_Val(start_node_id, end_node_id)

        if ((seek_max == M_TO_UP) and (value > self.node_list[-1].value)) \
                or ((seek_max == M_TO_DOWN) and (value < self.node_list[-1].value)):
            self.node_list[-1].datetime = lower_node_list[node_id].datetime
            self.node_list[-1].value = value
            if low_id != None:
                self.node_list[-1].low_id = low_id
        if isformal != None:
            self.node_list[-1].isformal = isformal

    # ------------------------------------------------------------------------
    def update_max_min_value(self):
        '''
        根据正式节点的值更新最大和最小值
        '''
        if (len(self.centralbase_list) < 2):
            return

        pre_base = self.centralbase_list[-2]
        base = self.centralbase_list[-1]

        if (self.cur_min_node_id == len(self.node_list) - 2) \
                or (self.cur_max_node_id == len(self.node_list) - 2):
            return

        if base.ctype == 0 or (pre_base.ctype * base.ctype < 0 and 1 == base.end_node_id - base.start_node_id):
            self.cur_max_node_id, self.cur_max_value = self.__getMaxNode_Val(pre_base.start_node_id, base.end_node_id)
            self.cur_min_node_id, self.cur_min_value = self.__getMinNode_Val(pre_base.start_node_id, base.end_node_id)
        else:
            if self.node_list[-2].value <= self.cur_min_value:  # 创新低

                # 背驰判断
                if base.ctype <= -2:
                    self.getBeichi_LastTwo_Step(isMinOrNot=True)

                self.cur_min_node_id = len(self.node_list) - 2
                self.cur_min_value = self.node_list[-2].value

            if self.node_list[-2].value >= self.cur_max_value:  # 创新高

                # 背驰判断
                if base.ctype >= 2:
                    self.getBeichi_LastTwo_Step(isMinOrNot=False)

                self.cur_max_node_id = len(self.node_list) - 2
                self.cur_max_value = self.node_list[-2].value

                # ------------------------------------------------------------------------

    def __reverse_direct(self, seek_max):
        if seek_max == M_TO_UP:
            return M_TO_DOWN
        else:
            return M_TO_UP

    # ------------------------------------------------------------------------
    def __getMACD_Sum(self, start_time, end_time, seekMax=True):

        #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
        data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol, start_time, end_time)))

        if data_seg.empty:
            return 0
        else:
            data_seg = data_seg['MACD']

        if seekMax:
            data_seg = data_seg[data_seg > 0]
        else:
            data_seg = data_seg[data_seg < 0]
        # return data_seg.sum()
        if data_seg.empty:
            return 0
        else:
            return data_seg.mean()

    # ------------------------------------------------------------------------
    def __get_abs_MACD_Sum(self, start_time, end_time, seekMax=True):

        #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
        data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol,  start_time, end_time)))

        if data_seg.empty:
            return 0
        else:
            data_seg = data_seg['MACD']

        data_seg = data_seg.apply(lambda x: abs(x))

        return data_seg.mean()

        # ------------------------------------------------------------------------

    def __is_Smaller_Progress(self, start_time, end_time, seekMax=True):

        #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
        data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol, start_time, end_time)))

        if data_seg.empty:
            return False
        else:
            data_seg = data_seg.sort_values(by='datetime', ascending=False)
            data_seg = np.array(data_seg['MACD2'])
            if seekMax:
                cov = 1
            else:
                cov = -1
            f_start = -1
            f_end = -1
            t_start = -1
            t_end = -1
            for i in range(data_seg.size):
                if cov * data_seg[i] >= 0:
                    if f_start == -1:
                        f_start = i
                    if f_end >= 0 and t_start == -1:
                        t_start = i
                else:
                    if f_start >= 0 and f_end == -1:
                        f_end = i
                    if t_start >= 0 and t_end == -1:
                        t_end = i
                        break

            if t_start >= 0 and t_end == -1:
                t_end = i

            if f_start >= 0 and f_end >= 0 and t_start >= 0 and t_end >= 0:
                avg1 = np.average(data_seg[f_start:f_end])
                avg2 = np.average(data_seg[t_start:t_end])
                if abs(avg1) < abs(avg2):
                    return True
                else:
                    return False
            else:
                return False

        return False

        # ------------------------------------------------------------------------

    def __getMACD_Sum_Lower(self, start_time, end_time, seekMax=True):
        if self.low_CB_set != None:
            #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.low_CB_set.freq, start_time, end_time)))
            data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.low_CB_set.freq], self.dbSymbol, start_time, end_time)))
        else:
            #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
            data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol, start_time, end_time)))

        if data_seg.empty:
            return 0
        else:
            data_seg = data_seg['MACD']

        if seekMax:
            data_seg = data_seg[data_seg > 0]
        else:
            data_seg = data_seg[data_seg < 0]
        # return data_seg.sum()
        if data_seg.empty:
            return 0
        else:
            return data_seg.mean()

            # ------------------------------------------------------------------------

    def __getVolumn_Sum(self, start_time, end_time, seekMax=True):
        #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
        data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol, start_time, end_time)))
        if data_seg.empty:
            return 0
        else:
            data_seg = data_seg['volume']

        if data_seg.empty:
            return 0
        else:
            return data_seg.mean()

            # ------------------------------------------------------------------------

    def __isMACD_Same_Direct(self, start_time, end_time, seekMax=True):
        #data_seg = pd.DataFrame(list(self.engine.loadDataRange(self.freq, start_time, end_time)))
        data_seg = pd.DataFrame(list(self.dbop.loadDataRange(DB_NAME_DICT[self.freq], self.dbSymbol, start_time, end_time)))

        if data_seg.empty:
            return True
        else:
            data_seg = data_seg['MACD']

        if data_seg.empty:
            return True
        else:
            data1 = data_seg > 0
            data2 = data_seg < 0
            if data1.all() or data2.all():
                return True
            else:
                return False

                # ------------------------------------------------------------------------

    def __get_lowest_current_price(self, freq):
        low_cb_set = self.low_CB_set
        while (low_cb_set != None):
            if low_cb_set.freq == freq:
                return low_cb_set.am.closeArray[-1]
            else:
                low_cb_set = low_cb_set.low_CB_set
        return self.am.closeArray[-1]
        # ------------------------------------------------------------------------

    def __roundTime(self, time, freq):
        if freq == '30MIN':
            return time.replace(minute=(30 if time.minute >= 30 else 0)) + dt.timedelta(minutes=30)
        elif freq == 'D':
            return time.replace(hour=0, minute=0)
        else:
            return time

    # ------------------------------------------------------------------------
    def __start_bc_calc(self, start_node_id, control_price):
        self.bottom_bc_is_ongoing = True
        self.bc_start_node_id = start_node_id
        self.control_price = control_price

    def __updateData(self, dbName, collectionName, data):
        self.dbop.dbUpdate(dbName, collectionName, data.__dict__, {'_id': data.__dict__['_id']}, True)

