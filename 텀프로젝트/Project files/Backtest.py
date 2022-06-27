from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
import pymysql
import warnings
import matplotlib.pyplot as plt
from getStockData import getStockData

"""
    * Backtest 클래스
        - 데이터베이스로부터 데이터를 가져와 백테스팅을 수행.
"""
class Backtest:
    server = '127.0.0.1:3306'  # local server
    user = 'root'  # user name
    password = 'js0815'  # 개인 password
    db = '16010980_my1st_db'  # DB 이름

    GSD = getStockData()

    def loadStockData(self, code, start_date, end_date):
        """수정 주가 데이터 불러오는 함수

        Parameters
        ==========
        code       : str, 종목 코드
        start_date : str, 시작 날짜
        end_date   : str, 종료 날짜
        """
        # db에 연결
        conn = pymysql.connect(host='localhost', port=3306, db=self.db,
                               user=self.user, passwd=self.password, autocommit=True)
        cursor = conn.cursor()

        # start_date = self.GSD.getRecentOpenDateReverse(start_date)
        # end_date = self.GSD.getRecentOpenDate(end_date)

        # 종목과 날짜에 따라 데이터 수집
        print('{} 종목 데이터 수집 중...'.format(code))
        try:
            # stock_code의 start_date와 end_date 데이터 불러오기
            sql = "SELECT * FROM STOCK_ADJUST_TABLE WHERE\
                               stock_code='{}' AND DATE(date) BETWEEN '{}' AND '{}'".format(code, start_date, end_date)

            cursor.execute(sql)

            df = pd.DataFrame(cursor.fetchall())
            df.columns = [col[0] for col in cursor.description]
            df.sort_values(by='date', inplace=True, ascending=True)
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        except:
            print('{} 데이터가 존재하지 않거나 잘못된 요청입니다.'.format(code))
            return
        print('{} 종목 데이터 수집 완료!'.format(code))

        cursor.close()
        conn.close()

        # 중복 데이터 제거
        df = df.drop_duplicates(['date'])
        df['date'] = df['date'].map(lambda date: datetime.combine(date, datetime.min.time()))
        df = df.set_index('date')

        return df['close']

    def buy_etf(self, money, etf_price, last_etf_num, fee_rate, etf_rate):
        """주식을 실제로 구입한 것처럼 시뮬레이션 하는 함수

        Parameters
        ==========
        money        : int, 소지 금액
        etf_price    : int, 주식 개당 가격
        last_etf_num : int, 소유하고 있던 주식 개수
        fee_rate     : float, 주식 매수세 비율
        etf_rate     : float, 해당 주식 비율
        """
        # 살 수 있는 etf 개수 계산
        etf_num = money * etf_rate // etf_price
        # 주식을 사는데 필요한 금액 계산
        etf_money = etf_num * etf_price
        # 주식 매수세 계산
        etf_fee = (last_etf_num - etf_num) * etf_price * fee_rate if last_etf_num > etf_num else 0

        # 주식을 구매하는 데 필요한 총 금액 계산
        while etf_num > 0 and money < (etf_money + etf_fee):
            etf_num -= 1
            etf_money = etf_num * etf_price
            etf_fee = (last_etf_num - etf_num) * etf_price * fee_rate if last_etf_num > etf_num else 0
        purchase_money = etf_money + etf_fee

        return purchase_money, etf_num, etf_money

    def return_acc_log_graph(self, df, codes, length):
        """주식의 상승세, 하락세를 그래프로 나타내는 함수

        Parameters
        ==========
        df     : DataFrame, 백테스팅 결과
        codes  : list_str, 종목 코드 리스트
        length : length, 길이
        """
        # 누적 로그 그래프로 표현
        profit_df = df.pct_change()

        profit_acc_df = (1 + profit_df).cumprod() - 1

        profit_log_df = np.log(profit_df + 1)

        profit_acc_log_df = profit_log_df.cumsum()

        final_acc_log_df = profit_acc_log_df * 100

        # 그래프 출력
        for i in range(length):
            plt.plot(final_acc_log_df[codes[i]].index, final_acc_log_df[codes[i]], label='{}'.format(codes[i]))
        plt.plot(final_acc_log_df['backtest'].index, final_acc_log_df['backtest'], label='Backtest')

        plt.legend(loc='upper left')
        plt.show()

    def last_day_of_month(self, any_date):
        """해당 날짜의 달의 마지막 날을 리턴하는 함수

        Parameters
        ==========
        any_date : datetime, 날짜
            '2021-01-01' -> '2021-01-31'
        """
        next_month = any_date.replace(day=28) + timedelta(days=4)  # this will never fail

        return next_month - timedelta(days=next_month.day)

    def make_new_df(self, df, interval, start_date, mode='none'):
        """새로운 재무 데이터프레임을 만드는 함수

        Parameters
        ==========
        df         : DataFrame, 재무 데이터
        interval   : int, 개월 주기(1달, 2달, 3달, ... , 1년, ... n년)
        start_date : str, 시작 날짜
        mode       : str, 모드
            'none'(현재 날짜부터 +1 씩), 'last'(달의 말부터 -1 씩)
        """
        new_df = pd.DataFrame()
        # 시작 날짜부터 반복
        while start_date <= df.index[-1]:
            if mode == 'last':
                # 달의 말부터 시작
                temp_date = self.last_day_of_month(start_date)
            else:
                # 시작 날짜부터 시작
                temp_date = start_date

            # 현재 temp_date 날짜가 재무 데이터에 존재하지 않는 경우(영업일이 아닌 경우),
            while temp_date not in df.index and temp_date < df.index[-1]:
                if mode == 'last':
                    temp_date -= timedelta(days=1)  # 영업일이 아닐 경우 1일씩 감소.
                else:
                    temp_date += timedelta(days=1)  # 영업일이 아닐 경우 1일씩 증가.
            # 가장 가까운 영업일을 데이터프레임에 추가
            if temp_date <= df.index[-1]:
                new_df = new_df.append(df.loc[temp_date])
            start_date += relativedelta(months=interval)  # interval 개월씩 증가.

        return new_df

    def Drawdowns(self, df):
        """Drawdowns를 그래프를 그리는 함수

        Parameters
        ==========
        df : DataFrame, 백테스팅 결과 데이터
        """
        # drawdowns 계산
        df['all-time_high'] = df['cur_profit'].cummax()
        df['DD'] = (df['cur_profit'] / df['all-time_high'] - 1) * 100

        # drawdowns 그래프
        plt.plot(df.index, df['DD'], label='Drawdowns')
        plt.legend()
        plt.show()

        MDD = df['DD'].min()
        print("MDD: {} %".format(MDD))

        return df

    def Drawdowns_for_HistoricalMarketStressPeriods(self, df):
        """주식 시장이 불황이었던 시기를 drawdonws 결과와 대조하는 함수

        Parameters
        ==========
        df : df, Drawdonws 데이터프레임
        """
        start_date = None
        StressPeriods_df = pd.DataFrame(columns=['Stress Period', 'Start', 'End', 'Portfolio'])

        for each in df.index:

            # Black Monday Period
            if date(1987, 9, 1) < each and each < date(1987, 12, 1):
                if start_date is None:
                    stressPeriod = 'Black Monday Period'
                    start_date = each
                end_date = each

            # Asian Crisis
            elif date(1997, 7, 1) < each and each < date(1998, 2, 1):
                if start_date is None:
                    stressPeriod = 'Asian Crisis'
                    start_date = each
                end_date = each

            # Russian Debt Default
            elif date(1998, 7, 1) < each and each < date(1998, 11, 1):
                if start_date is None:
                    stressPeriod = 'Russian Debt Default'
                    start_date = each
                end_date = each

            # Dotcom Crash
            elif date(2000, 3, 1) < each and each < date(2002, 11, 1):
                if start_date is None:
                    stressPeriod = 'Dotcom Crash'
                    start_date = each
                end_date = each

            # Subprime Crisis
            elif date(2007, 11, 1) < each and each < date(2009, 4, 1):
                if start_date is None:
                    stressPeriod = 'Subprime Crisis'
                    start_date = each
                end_date = each

            # COVID-19 Start
            elif date(2020, 1, 1) < each and each < date(2020, 4, 1):
                if start_date is None:
                    stressPeriod = 'COVID-19 Start'
                    start_date = each
                end_date = each

            # 데이터프레임 추가
            else:
                if start_date is not None:
                    part_MDD = df.loc[start_date:end_date]['DD'].min()
                    new_row = pd.DataFrame([(stressPeriod, datetime.strftime(start_date, '%Y-%m-%d'),
                                             datetime.strftime(start_date, '%Y-%m-%d'), '{:.2f} %'.format(part_MDD))],
                                           columns=['Stress Period', 'Start', 'End', 'Portfolio'])
                    # new_row = pd.DataFrame([(stressPeriod, start_date, end_date, part_MDD)], columns=['Stress Period', 'Start', 'End', 'Portfolio'])
                    StressPeriods_df = pd.concat([StressPeriods_df, new_row])
                start_date = None

        StressPeriods_df = StressPeriods_df.set_index('Stress Period')
        print(StressPeriods_df)

    def Date_Diff(self, end_date, start_date):
        """두 날짜의 차이를 구해서 기간으로 리턴하는 함수

        Parameters
        ==========
        end_date   : str, 종료 날짜
        start_date : str, 시작 날짜
        """
        # 두 날짜의 차이를 구함
        delta = relativedelta(end_date, start_date)
        result = ''

        # 두 날짜의 차이를 문자열 형태로 변환
        if delta.years > 1:
            result += '{} years'.format(delta.years)
        elif delta.years == 1:
            result += '1 year'

        if delta.years >= 1:
            result += ' '

        if delta.months > 1:
            result += '{} months'.format(delta.months)
        elif delta.months == 1:
            result += '1 month'

        return result

    def Drawdowns_for_Portfolio(self, df):
        """포트폴리오에서 Drawdowns가 있던 구간마다의 부분적 MDD를 구하는 함수

        Parameters
        ==========
        df = DataFrame, Drawdowns 데이터프레임
        """
        start_date = None
        Portfolio_df = pd.DataFrame(
            columns=['Start', 'End', 'Length', 'Recovery By', 'Recovery Time', 'Underwater Period', 'Drawdown'])

        # 모든 날짜에 대해서 부분적 MDD와 기간 등을 구함
        for each in df.index:
            if df['DD'][each] < 0:
                if start_date is None:
                    start_date = each
                    part_MDD = df['DD'][each]
                    MDD_date = each

                if part_MDD > df['DD'][each]:
                    part_MDD = df['DD'][each]
                    MDD_date = each

                end_date = each
            else:
                if start_date is not None:
                    # length = MDD_date - start_date
                    # recoveryTime = end_date - MDD_date
                    # underwaterPeriod = end_date - start_date
                    length = self.Date_Diff(MDD_date, start_date)
                    recoveryTime = self.Date_Diff(end_date, MDD_date)
                    underwaterPeriod = self.Date_Diff(end_date, start_date)

                    start_date = datetime.strftime(start_date, '%Y-%m-%d')
                    MDD_date = datetime.strftime(MDD_date, '%Y-%m-%d')
                    end_date = datetime.strftime(end_date, '%Y-%m-%d')

                    new_row = pd.DataFrame(
                        [(start_date, MDD_date, length, end_date, recoveryTime, underwaterPeriod, part_MDD)],
                        columns=['Start', 'End', 'Length', 'Recovery By', 'Recovery Time', 'Underwater Period',
                                 'Drawdown'])
                    Portfolio_df = pd.concat([Portfolio_df, new_row])
                start_date = None

        # 부분적 MDD 중 순위를 정하여 상위 10개의 부분적 MDD를 출력
        Portfolio_df = Portfolio_df.sort_values('Drawdown')
        Portfolio_df['Rank'] = Portfolio_df['Drawdown'].rank(method='min')
        Portfolio_df['Rank'] = Portfolio_df['Rank'].astype(int)
        Portfolio_df = Portfolio_df.set_index('Rank')
        print(Portfolio_df.iloc[0:10])

    def back_test(self, money: int, fee_rate: float, interval: int, codes: list, etf_rates: list, start_date: str,
                  end_date: str = datetime.today().strftime('%Y-%m-%d')):
        """해당 종목들에 대한 백테스팅을 진행하는 함수

        Parameters
        ==========
        money      : int, 초기 금액
        fee_rate   : float, 매수세
        interval   : int, 개월 주기
        codes      : list_str, 종목 코드
        etf_rates  : list_float, 종목 비율
        start_date : str, 시작 날짜
        end_date   : str, 종료 날짜
        """
        warnings.filterwarnings('ignore')
        pd.set_option('display.max_columns', None)

        start_date = datetime.strptime(start_date, '%Y-%m-%d')  # 조회시작일
        end_date = datetime.strptime(end_date, '%Y-%m-%d')  # 조회종료일

        etfs = []
        notExisted = []
        for i in range(len(codes)):
            df_oneStock = self.loadStockData(codes[i], start_date, end_date)
            # 해당 종목의 주식 정보를 가져오지 못한 경우,
            if df_oneStock is None:
                notExisted.append(i)
            # 해당 종목의 주식 정보를 가져온 경우,
            else:
                etfs.append(df_oneStock)

        # 주식 정보가 존재하지 않는 주식은 제거
        if notExisted:
            for i in notExisted:
                codes.pop(i)
                etf_rate = etf_rates.pop(i)
                plus_etf_rate = etf_rate / len(etf_rates)
                for j in range(len(etf_rates)):
                    etf_rates[j] += plus_etf_rate

        # 모든 주식의 종가를 하나의 데이터프레임으로 합침
        df = pd.concat(etfs, axis=1, keys=[code for code in codes])
        # Nan 데이터 처리
        df = df.fillna(method='ffill')

        # print(df.isnull().sum())

        # Drawdonws를 위한 데이터프레임
        monthly_df_for_dd = self.make_new_df(df, 1, start_date, 'last')

        # 리밸런싱 날짜의 데이터만 new_df에 남깁니다.
        new_df = self.make_new_df(df, interval, start_date)

        etf_nums = [0 for i in range(len(etfs))]    # etf 개수
        # etf_prices = [new_df[code][0] for code in codes]
        etf_prices = [0 for i in range(len(etfs))]  # etf 가격
        etf_moneys = [0 for i in range(len(etfs))]  # etf 구매 금액

        # 각 etf 마다의 backtest
        backtest_df = pd.DataFrame()  # 백테스트를 위한 데이터프레임
        profit_df_for_dd = pd.DataFrame()  # Drawdonws를 위한 기간별 이익 데이터프레임

        # 개월 주기마다 백테스팅
        months = 0
        monthly_last_date = monthly_df_for_dd.index[months]
        for each in new_df.index:
            for i in range(len(etfs)):
                etf_prices[i] = new_df[codes[i]][each]

            # 보유 etf 매도
            for i in range(len(etfs)):
                money += etf_nums[i] * etf_prices[i]

            # etf 매입
            init_money = money
            for i in range(len(etfs)):
                buy_money, etf_nums[i], etf_moneys[i] = self.buy_etf(init_money, etf_prices[i], etf_nums[i], fee_rate,
                                                                etf_rates[i])
                money -= buy_money

            # 현재 소유하고 있는 자산 규모
            total = money
            for i in range(len(etfs)):
                total += etf_moneys[i]

            # new_row = pd.DataFrame([tuple([money for money in etf_moneys] + [each])], columns=([code for code in codes] + ['Date']))
            # etf_backtest_df = pd.concat([etf_backtest_df, new_row])

            # Drawdowns를 구하기 위한 계산
            while months < len(monthly_df_for_dd.index) and monthly_last_date < each + relativedelta(months=interval):
                cur_profit = money
                for j in range(len(etfs)):
                    cur_profit += etf_nums[j] * monthly_df_for_dd[codes[j]][monthly_last_date]

                # profit_df_for_dd[monthly_last_date] = [int(cur_profit)]
                profit_df_for_dd[monthly_last_date] = [float(cur_profit)]

                months += 1
                if months >= len(monthly_df_for_dd.index):
                    break
                monthly_last_date = monthly_df_for_dd.index[months]

            # backtest_df[each] = [int(total)]
            backtest_df[each] = [float(total)]

        # 행열을 바꿈
        backtest_df = backtest_df.transpose()
        backtest_df.columns = ['backtest', ]

        # Drawdowns를 위한 데이터프레임 정리
        profit_df_for_dd = profit_df_for_dd.transpose()
        profit_df_for_dd.columns = ['cur_profit', ]

        # 백테스트 결과 출력
        # print(backtest_df)

        # etf_backtest_df = etf_backtest_df.set_index('Date')
        # print(etf_backtest_df)

        # 최종 데이터 프레임, 3개의 지표와 백테스트 결과
        # final_df = pd.concat([etf_backtest_df, backtest_df], axis=1)
        final_df = pd.concat([new_df, backtest_df], axis=1)
        print(final_df)

        # CAGR 계산
        CAGRs = []
        for i in range(len(etfs)):
            delta = final_df.index[-1] - final_df.index[0]
            years = delta.days / 365
            CAGR = (final_df[codes[i]][-1] / final_df[codes[i]][0]) ** (1 / years) - 1
            CAGRs.append(CAGR)
        delta = final_df.index[-1] - final_df.index[0]
        years = delta.days / 365
        CAGR = (final_df['backtest'][-1] / final_df['backtest'][0]) ** (1 / years) - 1
        CAGRs.append(CAGR)

        # CAGR 출력
        for i in range(len(etfs)):
            print("{} CAGR: {} %".format(codes[i], CAGRs[i] * 100))
        print("Total CAGR: {} %".format(CAGRs[-1] * 100))

        # 복리수익률 그래프
        self.return_acc_log_graph(final_df, codes, len(etfs))
        # Drawdonws 그래프와 MDD
        dd_df = self.Drawdowns(profit_df_for_dd)
        self.Drawdowns_for_HistoricalMarketStressPeriods(dd_df)
        self.Drawdowns_for_Portfolio(dd_df)