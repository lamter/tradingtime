# coding: utf-8
from itertools import chain
import datetime
import os
import requests
import functools
import copy
import re
import json
import pytz

import arrow
import calendar
import pandas as pd

pwd = os.path.split(__file__)[0]

__all__ = [
    'is_any_trading',
    'is_tradingday',
    'get_trading_status',
    'load_futures_tradingtime',
    'get_tradingday',
    'closed',
    'call_auction',
    'match',
    'continuous_auction',
    'contract2name',
]
LOCAL_TZINFO = pytz.timezone('Asia/Shanghai')
__inited = False

# 交易日类型
TRADE_DAY_TYPE_FIRST = u"首交"  # 假期后第一个交易日
TRADE_DAY_TYPE_NORMAL = u"正常"  # 正常交易日
TRADE_DAY_TYPE_FRI = u"周五"  # 正常周五
TRADE_DAY_TYPE_SAT = u"周六"  # 正常周六
TRADE_DAY_TYPE_HOLIDAY = u"假期"  # 假期
TRADE_DAY_TYPE_LAST = u"末日"  # 长假前最后一个交易日
TRADE_DAY_TYPE_HOLIDAY_FIRST = u"首假"  # 长假第一天

closed = 0
call_auction = 1  # 集合竞价
match = 2  # 撮合
continuous_auction = 3  # 连续竞价

t = datetime.time

# 中金所股指期货日盘
CFFEX_sid = (
    [t(9, 25), t(9, 29), call_auction],  # 集合竞价
    [t(9, 29), t(9, 30), match],  # 撮合
    [t(9, 30), t(11, 30), continuous_auction],  # 连续竞价
    [t(13, 0), t(15, 0), continuous_auction],  # 连续竞价
)

# 中金所国债期货日盘
CFFEX_ndd = (
    [t(9, 10), t(9, 14), call_auction],  # 集合竞价
    [t(9, 14), t(9, 15), match],  # 撮合
    [t(9, 15), t(11, 30), continuous_auction],  # 连续竞价
    [t(13, 0), t(15, 15), continuous_auction],  # 连续竞价
)

# 郑商所日盘
CZCE_d = (
    [t(8, 55), t(8, 59), call_auction],  # 集合竞价
    [t(8, 59), t(9, 0), match],  # 撮合
    [t(9, 0), t(10, 15, 1), continuous_auction],  # 连续竞价
    [t(10, 30, 0, 500000), t(11, 30), continuous_auction],  # 连续竞价
    [t(13, 30), t(15, 0, 0, 500000), continuous_auction],  # 连续竞价
)

# 郑商所夜盘
CZCE_n = (
    [t(20, 55), t(20, 59), call_auction],  # 集合竞价
    [t(20, 59), t(21, 0), match],  # 撮合
    [t(21, 0), t(23, 30), continuous_auction],  # 连续竞价
)

# 大商所日盘
DCE_d = CZCE_d
# 大商所夜盘
DCE_n = CZCE_n

# 上期所日盘
SHFE_d = CZCE_d
# 上期所夜盘
SHFE_n1 = (
    [t(20, 55), t(20, 59), call_auction],  # 集合竞价
    [t(20, 59), t(21, 0), call_auction],  # 撮合
    [t(21, 0), t(2, 30), continuous_auction],  # 连续竞价
)
# 上期所深夜盘2
SHFE_n2 = (
    [t(20, 55), t(20, 59), call_auction],  # 集合竞价
    [t(20, 59), t(21, 0), call_auction],  # 撮合
    [t(21, 0), t(1, 0), continuous_auction],  # 连续竞价
)
# 上期所深夜盘3
SHFE_n3 = (
    [t(20, 55), t(20, 59), call_auction],  # 集合竞价
    [t(20, 59), t(21, 0), call_auction],  # 撮合
    [t(21, 0), t(23, 0), continuous_auction],  # 连续竞价
)

# 能源所日盘
INE_d = CZCE_d
# 能源所夜盘
INE_n = SHFE_n1


class UnknowUnlyingsymbol(TypeError, ValueError):
    pass


def _get_futures_tradeing_time():
    """
    加载交易时间段的数据

    >>> _get_futures_tradeing_time()

    :return:
    """
    dic = {}
    r = requests.get('http://www.slavett.club:30030/static/futures_tradingtime.json')
    futures_tradingtime = json.loads(r.text)

    for future, trading_time in futures_tradingtime.items():
        dic[future] = tuple(chain(*map(globals().get, trading_time)))

    return dic


# 期货交易时间
_futures_tradeing_time = _get_futures_tradeing_time()

futures_tradeing_time = {
    # 中金所
    "IC": CFFEX_sid,  # 中证500股指
    "IF": CFFEX_sid,  # 沪深300股指
    "IH": CFFEX_sid,  # 上证50股指
    "TF": CFFEX_ndd,  # 5年国债
    "T": CFFEX_ndd,  # 10年国债

    # 郑商所
    "CF": tuple(chain(CZCE_d, CZCE_n)),  # 棉花
    "ZC": tuple(chain(CZCE_d, CZCE_n)),  # 动力煤
    "SR": tuple(chain(CZCE_d, CZCE_n)),  # 白糖
    "RM": tuple(chain(CZCE_d, CZCE_n)),  # 菜籽粕
    "MA": tuple(chain(CZCE_d, CZCE_n)),  # 甲醇
    "TA": tuple(chain(CZCE_d, CZCE_n)),  # PTA化纤
    "FG": tuple(chain(CZCE_d, CZCE_n)),  # 玻璃
    "OI": tuple(chain(CZCE_d, CZCE_n)),  # 菜籽油
    "CY": tuple(chain(CZCE_d, CZCE_n)),  # 棉纱
    "WH": CZCE_d,  # 强筋麦709
    "SM": CZCE_d,  # 锰硅709
    "SF": CZCE_d,  # 硅铁709
    "RS": CZCE_d,  # 油菜籽709
    "RI": CZCE_d,  # 早籼稻709
    "PM": CZCE_d,  # 普通小麦709
    "LR": CZCE_d,  # 晚籼稻709
    "JR": CZCE_d,  # 粳稻709
    "AP": CZCE_d,  # 苹果

    # 大商所
    "j": tuple(chain(DCE_d, DCE_n)),  # 焦炭
    "i": tuple(chain(DCE_d, DCE_n)),  # 铁矿石
    "jm": tuple(chain(DCE_d, DCE_n)),  # 焦煤
    "a": tuple(chain(DCE_d, DCE_n)),  # 黄大豆1号
    "y": tuple(chain(DCE_d, DCE_n)),  # 豆油
    "m": tuple(chain(DCE_d, DCE_n)),  # 豆粕
    "b": tuple(chain(DCE_d, DCE_n)),  # 黄大豆2号
    "p": tuple(chain(DCE_d, DCE_n)),  # 棕榈油
    ###################
    "jd": DCE_d,  # 鲜鸡蛋1709
    "l": DCE_d,  # 聚乙烯1709
    "v": DCE_d,  # 聚氯乙烯1709
    "pp": DCE_d,  # 聚丙烯1709
    "fb": DCE_d,  # 纤维板1709
    "cs": DCE_d,  # 玉米淀粉1709
    "c": DCE_d,  # 黄玉米1709
    "bb": DCE_d,  # 胶合板1709

    # 上期所
    "ag": tuple(chain(SHFE_d, SHFE_n1)),  # 白银1709
    "au": tuple(chain(SHFE_d, SHFE_n1)),  # 黄金1710
    ##################
    "pb": tuple(chain(SHFE_d, SHFE_n2)),  # 铅1709
    "ni": tuple(chain(SHFE_d, SHFE_n2)),  # 镍1709
    "zn": tuple(chain(SHFE_d, SHFE_n2)),  # 锌1709
    "al": tuple(chain(SHFE_d, SHFE_n2)),  # 铝1709
    "sn": tuple(chain(SHFE_d, SHFE_n2)),  # 锡1709
    "cu": tuple(chain(SHFE_d, SHFE_n2)),  # 铜1709
    #########
    "ru": tuple(chain(SHFE_d, SHFE_n3)),  # 天然橡胶1709
    "rb": tuple(chain(SHFE_d, SHFE_n3)),  # 螺纹钢1709
    "hc": tuple(chain(SHFE_d, SHFE_n3)),  # 热轧板1709
    "bu": tuple(chain(SHFE_d, SHFE_n3)),  # 沥青1809
    ##############
    "wr": SHFE_d,  # 线材1709
    "fu": SHFE_d,  # 燃料油1709

    # 能源所
    "sc": tuple(chain(INE_d, INE_n)),  # 螺纹钢1709
}

futures_tradeing_time = _futures_tradeing_time

# 日盘开始
DAY_LINE = datetime.time(3)
# 夜盘开始
NIGHT_LINE = datetime.time(20, 30)
# 午夜盘
MIDNIGHT_LINE = datetime.time(20, 30)


class FutureTradeCalendar(object):
    """
    期货的交易日生成
    """

    def __init__(self, begin=None, end=None):
        """
        self.calendar 格式如下
            type  weekday    next_td   tradeday day_trade night_trade  midnight_trade
date
2016-01-01     2        5 2016-01-04 2016-01-01      True        True           True
2016-01-02     3        6 2016-01-04 2016-01-04     False       False           True

        :param begin:
        :param end:
        """

        # 每天的假期, pd.Sereis, date: type
        self.holidays = self.get_holiday_json()

        self.begin = begin or self.yearbegin()
        self.end = end or self.yearend()  # 次年1月10日
        if self.holidays.shape[0]:
            end = max(self.holidays.index)
            end = pd.to_datetime(end)
            self.end = self.end.replace(end.year + 1)

        # 交易日历
        self.calendar = None

    def load(self):
        self.calendar = self.getCalendar()

    @staticmethod
    def yearbegin():
        now = arrow.now()
        return arrow.get("%s-01-01 00:00:00" % now.year).date()

    @staticmethod
    def yearend():
        now = arrow.now()
        return arrow.get("%s-01-10 00:00:00" % (now.year + 1)).date()

    def get_holiday_json(self):
        """
        读取假期时间表
        :return:
        """
        # 使用本地文件调试
        # with open('futures_holiday.json', 'r') as f:
        #     return pd.read_json(f.read(), typ="series").sort_index()

        r = requests.get('http://www.slavett.club:30030/static/futures_holiday.json')
        return pd.read_json(r.text, typ="series").sort_index()

    def getCalendar(self):
        """
        生成交易日
        :return:
        """
        # 加载日历的年份
        tradecalendar = pd.DataFrame(data=pd.date_range(self.begin, self.end), columns=['date'])

        # 标记普周末的日期类型
        types, weekdays = self._weekend_trade_day_type(tradecalendar["date"])
        tradecalendar["type"] = types
        tradecalendar["weekday"] = weekdays
        tradecalendar["weekday"] += 1
        tradecalendar = tradecalendar.set_index("date", drop=False)

        # 标记长假的日期类型
        tradecalendar = self._holiday_trade_day_type(tradecalendar)

        # 标记交易状态
        tradecalendar = self._tradestatus(tradecalendar)

        # 是否是结算日, 有日盘的那天就是结算日
        tradecalendar['is_tradingday'] = tradecalendar.day_trade

        return tradecalendar

    def _weekend_trade_day_type(self, dates):
        types = []
        weekdays = []
        for dt in dates:
            weekday = dt.date().weekday()
            # 后处理正常交易日
            if weekday == calendar.MONDAY:
                # 假期后第一个交易日
                _types = TRADE_DAY_TYPE_FIRST
            elif weekday == calendar.FRIDAY:
                # 正常周五
                _types = TRADE_DAY_TYPE_FRI
            elif weekday == calendar.SATURDAY:
                # 正常周六
                _types = TRADE_DAY_TYPE_SAT
            elif weekday == calendar.SUNDAY:
                # 假期
                _types = TRADE_DAY_TYPE_HOLIDAY
            else:
                # 正常交易日
                _types = TRADE_DAY_TYPE_NORMAL

            types.append(_types)
            weekdays.append(weekday)
        return types, weekdays

    def _holiday_trade_day_type(self, tradecalendar):
        """
        节假日的日期类型
        :param dates:
        :return:
        """
        tradecalendar = tradecalendar.copy()
        _type = tradecalendar["type"].to_dict()
        _type.update(self.holidays.to_dict())
        tradecalendar["type"] = pd.Series(_type)

        return tradecalendar

    def _tradestatus(self, tradecalendar):
        tradecalendar = tradecalendar.copy()
        # 下一交易日
        next_td = tradecalendar[(tradecalendar["type"] == TRADE_DAY_TYPE_FIRST)
                                | (tradecalendar["type"] == TRADE_DAY_TYPE_NORMAL)
                                | (tradecalendar["type"] == TRADE_DAY_TYPE_FRI)
                                | (tradecalendar["type"] == TRADE_DAY_TYPE_LAST)].copy()
        # 向后移一天
        next_td['next_td'] = next_td["date"].shift(-1)

        # 获得下一个交易日, 并且向前填充, 即周六周日的下一交易日为下周一
        tradecalendar['next_td'] = next_td['next_td']
        tradecalendar['next_td'] = tradecalendar['next_td'].fillna(method='pad')

        # 当前交易日
        # 假期的当前交易日为下一交易日
        td = tradecalendar[(tradecalendar["type"] == TRADE_DAY_TYPE_SAT)
                           | (tradecalendar["type"] == TRADE_DAY_TYPE_HOLIDAY)
                           | (tradecalendar["type"] == TRADE_DAY_TYPE_HOLIDAY_FIRST)
                           ]

        tradecalendar['tradeday'] = td["next_td"]
        tradecalendar["tradeday"] = tradecalendar['tradeday'].fillna(tradecalendar["date"])

        # 最后一天的数据不完整,去掉
        tradecalendar = tradecalendar[:-1]

        # 日盘 *能* 交易的
        day_trade = pd.Series(False, index=tradecalendar.index)
        day_trade[
            (tradecalendar.type == TRADE_DAY_TYPE_FIRST)  # 假期后第一个交易日
            | (tradecalendar.type == TRADE_DAY_TYPE_NORMAL)  # 正常交易日
            | (tradecalendar.type == TRADE_DAY_TYPE_FRI)  # 正常周五
            # | (tradecalendar.type == TRADE_DAY_TYPE_SAT)  # 正常周六
            # | (tradecalendar.type == TRADE_DAY_TYPE_HOLIDAY)  # 假期
            | (tradecalendar.type == TRADE_DAY_TYPE_LAST)  # 长假前最后一个交易日
            # | (tradecalendar.type == TRADE_DAY_TYPE_HOLIDAY_FIRST)  # 长假第一天
            ] = True
        tradecalendar["day_trade"] = day_trade

        # 夜盘 *能* 易的
        night_trade = pd.Series(False, index=tradecalendar.index)
        night_trade[
            (tradecalendar.type == TRADE_DAY_TYPE_FIRST)  # 假期后第一个交易日
            | (tradecalendar.type == TRADE_DAY_TYPE_NORMAL)  # 正常交易日
            | (tradecalendar.type == TRADE_DAY_TYPE_FRI)  # 正常周五
            # | (tradecalendar.type == TRADE_DAY_TYPE_SAT)  # 正常周六
            # | (tradecalendar.type == TRADE_DAY_TYPE_HOLIDAY)  # 假期
            # | (tradecalendar.type == TRADE_DAY_TYPE_LAST)  # 长假前最后一个交易日
            # | (tradecalendar.type == TRADE_DAY_TYPE_HOLIDAY_FIRST)  # 长假第一天
            ] = True
        tradecalendar["night_trade"] = night_trade

        # 午夜盘不能交易的, 所有假日
        midnight_trade = pd.Series(False, index=tradecalendar.index)
        midnight_trade[
            # (tradecalendar.type == TRADE_DAY_TYPE_FIRST)  # 假期后第一个交易日
            (tradecalendar.type == TRADE_DAY_TYPE_NORMAL)  # 正常交易日
            | (tradecalendar.type == TRADE_DAY_TYPE_FRI)  # 正常周五
            | (tradecalendar.type == TRADE_DAY_TYPE_SAT)  # 正常周六
            # | (tradecalendar.type == TRADE_DAY_TYPE_HOLIDAY)  # 假期
            | (tradecalendar.type == TRADE_DAY_TYPE_LAST)  # 长假前最后一个交易日
            # | (tradecalendar.type == TRADE_DAY_TYPE_HOLIDAY_FIRST)  # 长假第一天
            ] = True
        tradecalendar["midnight_trade"] = midnight_trade

        return tradecalendar

    def _holiday_tradestatus(self, tradecalendar):
        """
        长假的交易状态
        :param tradecalendar:
        :return:
        """

    def get_tradeday(self, now):
        """
        返回一个日期的信息
        :param now:
        :return: bool(是否交易时段), 当前交易日
        >>> now = datetime.datetime(2016,10, 25, 0, 0, 0) # 周五的交易日是下周一
        >>> futureTradeCalendar.get_tradeday(now)
        (True, Timestamp('2016-10-25 00:00:00'))

        """

        t = now.time()
        day = self.calendar.ix[now.date()]
        if DAY_LINE < t < NIGHT_LINE:
            # 日盘, 当前交易日
            return day.day_trade, day.tradeday
        elif NIGHT_LINE < t:
            # 夜盘，下一个交易日
            return day.night_trade, day.next_td
        else:
            # 午夜盘，已经过了零点了，当前交易日
            return day.midnight_trade, day.tradeday

    def is_tradingday(self, dt):
        """

        :param dt:
        :return:
        """
        date = dt.date()
        return self.calendar.is_tradingday[date]

    def get_tradeday_opentime(self, tradeday):
        """
        获得交易日的起始日，比如长假后第一个交易日的起始日应该为节前的最后一个交易日
        :param tradeday: date() OR datetime(2016, 12, 12)
        :return:
        """

        calendar = self.calendar[self.calendar.next_td == tradeday]
        return calendar.index[0].date()


# 期货交易日历实例
futureTradeCalendar = FutureTradeCalendar()
futures = list(futures_tradeing_time.keys())


# 交易日历
def load_futures_tradingtime():
    global futureTradeCalendar, futures, __inited

    futureTradeCalendar.load()

    futures.sort()
    __inited = True


def inited(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        if not __inited:
            load_futures_tradingtime()
        return func(*args, **kw)

    return wrapper


@inited
def get_trading_status(future, now=None, ahead=0, delta=0):
    """
    >>> get_trading_status('AP', now=datetime.time(10,0,0), delta=10) == continuous_auction
    True
    >>> get_trading_status('AP', now=datetime.time(10,14,0, 500000))
    3
    >>> get_trading_status('AP', now=datetime.time(10,14,0, 500000)) == continuous_auction
    True
    >>> future = 'ag'
    >>> today = datetime.date.today()
    >>> for b, e, s in futures_tradeing_time[future]:
    ...     now = datetime.datetime.combine(today, b) + datetime.timedelta(seconds=30)
    ...     s == get_trading_status(future, now.time(), ahead=10, delta=10)
    ...
    True
    True
    True
    True
    True
    True
    True
    True
    >>> get_trading_status(future, datetime.time(0,5,10), ahead=10, delta=10)
    3
    >>> get_trading_status(future, datetime.time(23,59,59), ahead=10, delta=10)
    3
    >>> get_trading_status('ag', datetime.time(0, 0, 0), ahead=10, delta=10)
    3
    >>> get_trading_status('IC', delta=10)
    3

    :param future: 'rb' OR 'rb1801'
    :param now:
    :param ahead: 提前结束 10, 提前 10秒结束
    :param delta: 延迟开始 5, 延迟5秒开始
    :return:

    """
    future = contract2name(future)
    if future not in futures_tradeing_time:
        raise UnknowUnlyingsymbol('future {}'.format(future))

    if now is None:
        now = arrow.now().time()
    elif isinstance(now, datetime.datetime):
        now = arrow.get(now).time()

    # 时间列表
    trading_time = futures_tradeing_time[future]
    for b, e, s in trading_time:
        # 计算延迟
        if delta != 0:
            b = datetime.datetime.combine(datetime.date.today(), b) + datetime.timedelta(seconds=delta)
            b = b.time()
        if ahead != 0:
            e = datetime.datetime.combine(datetime.date.today(), e) - datetime.timedelta(seconds=ahead)
            e = e.time()

        if b <= now <= e:
            # 返回对应的状态
            return s
        elif (e < b <= now or now <= e < b):
            # 这一种情况跨天了
            return s
    else:
        # 不在列表中则为休市状态
        return closed


@inited
def is_any_trading(now=None, delta=0, ahead=0):
    """
    >>> import datetime
    >>> is_any_trading(datetime.datetime(2016, 10, 22, 15, 56 ,24))
    False

    至少有一个品种在交易中
    :return:
    """
    now = now or datetime.datetime.now()
    is_trade, tradeday = futureTradeCalendar.get_tradeday(now)
    if not is_trade:
        # 当前日/夜/午夜盘不开盘
        return False

    for f in futures:
        if get_trading_status(f, now.time(), delta, ahead) != closed:
            return True
    else:
        return False


@inited
def get_tradingday(dt):
    """
    >>> import arrow
    >>> is_tradingday, tradingday = get_tradingday(arrow.get('2018-09-28 23:00:00+08:00').datetime)
    >>> is_tradingday
    True
    >>> tradingday
    datetime.datetime(2018, 10, 8, 0, 0, tzinfo=tzoffset(None, 28800))

    :param dt: 给定时间点，用于判定该时间点对应的交易日
    :return: bool(是否交易时段), 当前交易日
    """
    assert isinstance(dt, datetime.datetime)

    tzInfo = None
    if dt.tzinfo:
        # 有时区,将时区转为东八区
        tzInfo = dt.tzinfo
        dt = arrow.get(dt).to(LOCAL_TZINFO)

    is_tradingtime, tradeday = futureTradeCalendar.get_tradeday(dt)

    if tzInfo:
        tradeday = LOCAL_TZINFO.localize(tradeday)
        tradeday = arrow.get(tradeday).to(tzInfo).datetime

    return is_tradingtime, tradeday


@inited
def is_tradingday(dt):
    return futureTradeCalendar.is_tradingday(dt)


def get_tradingtime_by_status(futures, status):
    """
    根据品种名获得交易时间段，判定时间段的方式为
    开始时间 <= now < 结束时间，是半开放区间。

    :param futures: 品种名，如螺纹钢为 'rb'
    :param status: 交易状态
    :return: [[datetime.time(开始时间), datetime.time(结束时间), 时间段状态]]
    """
    t = []
    for b, e, s in futures_tradeing_time[futures]:
        if s == status:
            t.append([b, e, s])

    return copy.deepcopy(t)


# def get_not_ticktime(futures):
#     """
#     不在连续竞价时间段内的，都不是 Tick 时间
#     :param futures:
#     :return:
#     """
#     tt = futures_tradeing_time[futures]
#     # 连续竞价时间
#     continuous_auction_time = []
#     for t in futures_tradeing_time:
#         if t[2] == continuous_auction:
#             continuous_auction_time.append(t)
#
#     # 按照开始时间排序
#     continuous_auction_time.sort(key=lambda x: x[0])
#
#     # 构建 not_ticktime
#     not_ticktime = []
#     def foo():
#         for i in continuous_auction_time:
#             yield i
#
#     timelist = foo()
#     pre = next(timelist）
#     next_one = next(timelist)

pattern = re.compile(r'\D*')


def contract2name(contract):
    """
    将合约代码解析成品种缩写，如：'rb1801' 解析返回 'rb'
    :param contract:
    :return:
    """
    return pattern.match(contract).group()
