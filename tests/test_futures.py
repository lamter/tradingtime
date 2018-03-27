# coding:utf-8

def test_load_tradingtime_json(tt):
    print(tt)


def test_sc(tt):
    """
    测试上海能源交易所的品种
    :param tt:
    :return:
    """
    print(tt.get_trading_status('sc'))
