import tradingtime as tt
import arrow
import datetime

tt.get_trading_status('SA')

from tradingtime import future
print(future.CZCE_n)

# tt.load_futures_tradingtime(arrow.get('2011-01-01').date())
#
# td = arrow.get('2018-11-01 09:03:00+08:00').datetime
# for i in range(1):
#     td += datetime.timedelta(days=1)
#     print(td, tt.is_tradingday(td))
