"""
    종목 추출과 백테스팅을 시현.
"""

from getStockResult import getStockResult
from Backtest import Backtest

GSR = getStockResult()
BT = Backtest()

"""
    * 지원하는 지표 종류
        - 'PER', 'PBR', 'PSR', 'PCR', 'EV/EBITDA', 'EV/Sales', #6
          'SafetyMargin', 'Liability/Equity', 'Debt/Equity', #3
          'GrossMargin', 'OperatingMargin', 'ProfitMargin' #3
"""
stock_list = GSR.resultStockList('2022Q1', ['PER', 'PSR', 'GrossMargin', 'OperatingMargin', 'ProfitMargin'], 'Consolidated_T', 10, 0.2)
print(stock_list)

# stock_list = ['005930', '373220', '000660', '035420', '207940', '035720', '006400', '005380', '051910', '000270']
#
# BT.back_test(10_000_000, 0.002, 12, stock_list,
#              [1 / len(stock_list) for stock in stock_list],
#              '2017-01-01', '2022-01-01')