import tradingtime as tt
import arrow
import datetime

tt.get_trading_status('SA')

from tradingtime import future
print(future.get_trading_status('lu'))

tt.load_futures_tradingtime(arrow.get('2011-01-01').date())

td = arrow.get('2018-11-01 09:03:00+08:00').datetime
# for i in range(1):
#     td += datetime.timedelta(days=1)
#     print(td, tt.is_tradingday(td))


if __name__ == '__main__':
    # tt.future.futureTradeCalendar
    print(tt.future.futureTradeCalendar.holidays)
    print(tt.get_tradingday(arrow.now().datetime))
