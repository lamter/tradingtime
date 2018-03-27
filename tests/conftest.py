#coding:utf-8

import pytest

@pytest.fixture(scope='session')
def tt():
    import tradingtime as tt
    return tt

