import pymysql
import pandas as pd
import numpy as np
from getValueIndicator import getValueIndicator

"""
    * getStockResult
        - 사용자가 지정한 파라미터에 따라 종목을 추출하여 리턴.
"""
class getStockResult:
    server = '127.0.0.1:3306'  # local server
    user = 'root'  # user name
    password = 'js0815'  # 개인 password
    db = '16010980_my1st_db'  # DB 이름
    GVI = getValueIndicator()

    def PeriodToDate(self, period):
        """기간을 해당 기간의 마지막 날짜로 변환하는 함수

        Parameters
        ==========
        period : str, 기간
            '2021Q1', '2021Q2', '2021Q3', '2021Q4'
        """
        if period[5] == '1': # 1분기 (1월~3월)
            date = period[0:4] + '-03-31'
        elif period[5] == '2': # 2분기 (4월~6월)
            date = period[0:4] + '-06-30'
        elif period[5] == '3': # 3분기 (7월~9월)
            date = period[0:4] + '-09-30'
        elif period[5] == '4': # 4분기 (10월~12월)
            date = period[0:4] + '-12-31'
        else:
            print('잘못된 입력입니다: 기간\n - 예시: 2021Q1')

        return date

    def ascTrue(self, factor):
        """오름차순인지 내림차순인지 구별하는 함수

        Parameters
        ==========
        factor : str, 지표
        """
        asc = ['PER', 'PBR', 'PSR', 'PCR', 'EV/EBITDA', 'EV/Sales', 'Liability/Equity', 'Debt/Equity']
        if factor in asc:
            return True
        else:
            return False

    def get_stat(self, date, stat_type, rpt_type, stock_code='ALL'):
        """해당 날짜와 재무제표를 리턴하는 함수

        Parameters
        ==========
        date       : str, 날짜
        stat_type  : str, 재무제표종류
            'INCOME'(손익계산서), 'BALANCE'(재무상태표), 'CASHFLOW'(현금흐름표)
        rpt_type   : str, 연결 및 별도 / 연간 및 분기 종류
            'Consolidated_A'(연결, 연간), 'Unconsolidated_A'(별도, 연간),
            'Consolidated_Q'(연결, 분기), 'Unconsolidated_Q'(별도, 분기),
            'Consolidated_T'(연결, 트레일링), 'Unconsolidated_T'(별도, 트레일링)
        stock_code : str, 종목 코드
        """
        # db에 연결
        conn = pymysql.connect(host='localhost', port=3306, db=self.db,
                               user=self.user, passwd=self.password, autocommit=True)
        cursor = conn.cursor()

        # 파라미터에 따라 가져오는 재무제표가 달라짐
        try:
            if stock_code.upper() == 'ALL':
                sql = "SELECT * FROM {}_TABLE WHERE \
                                기간='{}' AND 재무제표종류='{}'".format(stat_type, date, rpt_type)
            else:
                sql = "SELECT * FROM {}_TABLE WHERE \
                                종목='{}' AND 기간='{}' AND 재무제표종류='{}'".format(stat_type, stock_code, date, rpt_type)

            cursor.execute(sql)

            df_st = pd.DataFrame(cursor.fetchall())
            df_st.columns = [col[0] for col in cursor.description]

            # df_st = df_st.iloc[:, 1:-1]

            cursor.close()
            conn.close()
        except:
            print('데이터가 존재하지 않거나 잘못된 요청입니다.')
            return

        return df_st

    def calculateFactor(self, df, factor):
        if factor == 'PER':
            df = self.GVI.getPER(df)
        elif factor == 'PBR':
            df = self.GVI.getPBR(df)
        elif factor == 'PSR':
            df = self.GVI.getPSR(df)
        elif factor == 'PCR':
            df = self.GVI.getPCR(df)
        elif factor == 'EV/EBITDA':
            df = self.GVI.getEV_EBITDA(df)
        elif factor == 'EV/Sales':
            df = self.GVI.getEV_Sales(df)
        elif factor == 'SafetyMargin':
            df = self.GVI.getSafetyMargin(df)
        elif factor == 'Liability/Equity':
            df = self.GVI.getLiability_Equity(df)
        elif factor == 'Debt/Equity':
            df = self.GVI.getDebt_Equity(df)
        elif factor == 'GrossMargin':
            df = self.GVI.getGrossMargin(df)
        elif factor == 'OperatingMargin':
            df = self.GVI.getOperatingMargin(df)
        elif factor == 'ProfitMargin':
            df = self.GVI.getProfitMargin(df)
        else:
            print('지원하지 않는 지표입니다')
            return

        return df

    def stock_select(self, df_factor, MKTCAP_top, n, factor_list):
        """파라미터에 따라 특정 종목들을 추출하는 함수

        Parameters
        ==========
        df_factor   : DataFrame, 재무 데이터
        MKTCAP_top  : float, 시가총액 기준 상위 %
        n           : int, 추출한 종목 상위 n개
        factor_list : list_str, 지표 리스트
        """
        basic_list = ['종목', '기간', '시가총액']
        # factor_list = ['PER', 'PBR']

        basic_list.extend(factor_list)

        df_select = df_factor.copy()
        df_select = df_select[basic_list]

        df_select['score'] = 0

        # 무한대로 표시되는 경우, Nan 처리
        df_select = df_select.replace([np.inf, -np.inf], np.nan)
        # Nan 제거
        df_select = df_select.dropna()

        # 시가총액 상위 MKTCAP_top% 산출
        # MKTCAP_top = 0.2
        df_select = df_select.sort_values(by=['시가총액'], ascending=False).head(int(len(df_select) * MKTCAP_top))

        # 팩터간의 점수 계산
        for i in range(len(factor_list)):
            # 낮은 값에 높은 점수 부여
            if self.ascTrue(factor_list[i]):
                df_select[factor_list[i] + '_score'] = max(df_select[factor_list[i]]) - df_select[factor_list[i]]
                df_select[factor_list[i] + '_score'] = df_select[factor_list[i] + '_score'] / (max(df_select[factor_list[i]]) - min(df_select[factor_list[i]]))
            # 높은 값에 높은 점수 부여
            else:
                df_select[factor_list[i] + '_score'] = df_select[factor_list[i]] - min(df_select[factor_list[i]])
                df_select[factor_list[i] + '_score'] = df_select[factor_list[i] + '_score'] / (max(df_select[factor_list[i]]) - min(df_select[factor_list[i]]))

            df_select['score'] += (df_select[factor_list[i] + '_score'] / len(factor_list))

        # 상위 n개 종목 추출
        # n = 30
        df_select = df_select.sort_values(by=['score'], ascending=False).head(n)

        # 종목 선택
        stock_select = list(df_select['종목'])

        return stock_select

    def resultStockList(self, period, factor_list, rpt_type, n, MKTCAP_top):
        """지표와 이외의 파라미터에 따라 필요한 재무제표 로드, 지표 계산, 종목 추출 함수를 종합적으로 불러오는 함수

        Parameters
        ==========
        period      : str, 기간
            '2021Q1'
        factor_list : list_str, 지표 리스트
            ['PER', 'PBR']
        rpt_type    : str, 재무제표 종류
            'Consolidated_A'(연결, 연간), 'Unconsolidated_A'(별도, 연간),
            'Consolidated_Q'(연결, 분기), 'Unconsolidated_Q'(별도, 분기),
            'Consolidated_T'(연결, 트레일링), 'Unconsolidated_T'(별도, 트레일링)
        n           : int, 추출할 종목 상위 n개
        MKTCAP_top  : float, 시가총액 기준 상위 %
        """
        date = self.PeriodToDate(period)

        # 모든 재무 데이터 가져옴
        is_df = self.get_stat(date, 'INCOME', rpt_type)
        # 재무상태표는 트레일링 데이터가 없기 때문에 분기 데이터로 대신함
        if rpt_type == 'Consolidated_T':
            bs_df = self.get_stat(date, 'BALANCE', 'Consolidated_Q')
            bs_df['재무제표종류'] = 'Consolidated_T'
        elif rpt_type == 'Unconsolidated_T':
            bs_df = self.get_stat(date, 'BALANCE', 'Unconsolidated_Q')
            bs_df['재무제표종류'] = 'Consolidated_T'
        else:
            bs_df = self.get_stat(date, 'BALANCE', rpt_type)
        cf_df = self.get_stat(date, 'CASHFLOW', rpt_type)

        # 모든 재무 데이터를 하나의 데이터프레임으로 합침
        st_df = pd.merge(is_df, bs_df, how='left', on=['종목', '기간', '재무제표종류'])
        st_df = pd.merge(st_df, cf_df, how='left', on=['종목', '기간', '재무제표종류'])

        # 계산에 필요한 데이터를 원시 주가, 수정 주가 데이터로부터 가져와 합침
        print('계산에 필요한 데이터 수집 중...')
        st_df = pd.merge(st_df, self.GVI.getShares(date), how='left', on=['종목'])
        print('상장 주식 수 수집 완료!')
        st_df = pd.merge(st_df, self.GVI.getClose(date), how='left', on=['종목'])
        print('수정 주가 수집 완료!')
        st_df = pd.merge(st_df, self.GVI.getMKTCAP(date), how='left', on=['종목'])
        print('시가 총액 수집 완료!')
        print('필요한 모든 데이터 수집 완료!')

        # 중복 종목 제거
        st_df = st_df.drop_duplicates(['종목', '기간', '재무제표종류'])

        # factor_list에 있는 지표에 따라 계산
        for factor in factor_list:
            st_df = self.calculateFactor(st_df, factor)

        # 조건에 맞는 상위 종목들 선택
        stock_list = self.stock_select(st_df, MKTCAP_top, n, factor_list)

        return stock_list