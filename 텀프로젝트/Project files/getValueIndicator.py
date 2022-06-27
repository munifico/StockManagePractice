import pandas as pd
import numpy as np
import time, json, requests
import pymysql
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from getStockData import getStockData
from getFinancialData import getFinancialData

"""
    * getValueIndicator
        - 여러 지표를 계산하여 리턴.
        - 지원하는 지표는 아래와 같다.
        - 'PER', 'PBR', 'PSR', 'PCR', 'EV/EBITDA', 'EV/Sales', #6
          'SafetyMargin', 'Liability/Equity', 'Debt/Equity', #3
          'GrossMargin', 'OperatingMargin', 'ProfitMargin' #3
"""
class getValueIndicator:
    server = '127.0.0.1:3306'  # local server
    user = 'root'  # user name
    password = 'js0815'  # 개인 password
    db = '16010980_my1st_db'  # DB 이름
    GFD = getFinancialData()
    GSD = getStockData()

    def getShares(self, date):
        """상장주식수를 가져오는 함수

        Parameters
        ==========
        date : str, 날짜
            '2021-01-01'
        """
        # db에 연결
        conn = pymysql.connect(host='localhost', port=3306, db=self.db,
                               user=self.user, passwd=self.password, autocommit=True)
        cursor = conn.cursor()

        date = self.GSD.getRecentOpenDate(date)

        # 원시 주가 테이블을 가져옴
        try:
            # stock_code의 start_date와 end_date 데이터 불러오기
            sql = "SELECT * FROM STOCK_RAW_TABLE WHERE\
                       DATE(date) = '{}'".format(date)

            cursor.execute(sql)

            df = pd.DataFrame(cursor.fetchall())
            df.columns = [col[0] for col in cursor.description]
            df.sort_values(by='date', inplace=True, ascending=False)
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        except:
            print('데이터가 존재하지 않거나 잘못된 요청입니다.')
            return

        cursor.close()
        conn.close()

        # 종목과 상장주식수를 리턴
        df = df[['stock_code', 'LIST_SHRS']]
        df = df.rename(columns={
            'stock_code': '종목',
            'LIST_SHRS': '상장주식수'
        })
        return df

    def getClose(self, date):
        """종가(수정 주가)를 가져오는 함수

        Parameters
        ==========
        date : str, 날짜
            '2021-01-01'
        """
        # db에 연결
        conn = pymysql.connect(host='localhost', port=3306, db=self.db,
                               user=self.user, passwd=self.password, autocommit=True)
        cursor = conn.cursor()

        date = self.GSD.getRecentOpenDate(date)

        # 수정 주가 테이블을 가져옴
        try:
            # stock_code의 start_date와 end_date 데이터 불러오기
            sql = "SELECT * FROM STOCK_ADJUST_TABLE WHERE\
                               DATE(date) = '{}'".format(date)

            cursor.execute(sql)

            df = pd.DataFrame(cursor.fetchall())
            df.columns = [col[0] for col in cursor.description]
            df.sort_values(by='date', inplace=True, ascending=False)
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        except:
            print('데이터가 존재하지 않거나 잘못된 요청입니다.')
            return

        cursor.close()
        conn.close()

        # 종목과 종가(수정 주가)를 리턴
        df = df[['stock_code', 'close']]
        df = df.rename(columns={
            'stock_code': '종목',
            'close': '종가'
        })
        return df

    def getMKTCAP(self, date):
        """시가총액을 가져오는 함수

            Parameters
            ==========
            date : str, 날짜
                '2021-01-01'
        """
        # db에 연결
        conn = pymysql.connect(host='localhost', port=3306, db=self.db,
                               user=self.user, passwd=self.password, autocommit=True)
        cursor = conn.cursor()

        date = self.GSD.getRecentOpenDate(date)

        # 원시 주가 테이블을 가져옴
        try:
            # stock_code의 start_date와 end_date 데이터 불러오기
            sql = "SELECT * FROM STOCK_RAW_TABLE WHERE\
                               DATE(date) = '{}'".format(date)

            cursor.execute(sql)

            df = pd.DataFrame(cursor.fetchall())
            df.columns = [col[0] for col in cursor.description]
            df.sort_values(by='date', inplace=True, ascending=False)
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        except:
            print('데이터가 존재하지 않거나 잘못된 요청입니다.')
            return

        cursor.close()
        conn.close()

        # 종목과 시가총액을 리턴
        df = df[['stock_code', 'MKTCAP']]
        df = df.rename(columns={
            'stock_code': '종목',
            'MKTCAP': '시가총액'
        })
        return df

    def getPER(self, df):
        """PER을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['EPS'] = df['당기순이익'] / df['상장주식수']
        df['PER'] = df['종가'] / df['EPS']

        return df

    def getPBR(self, df):
        """PBR을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['주당순자산'] = df['자본'] / df['상장주식수']
        df['PBR'] = df['종가'] / df['주당순자산']

        return df

    def getPSR(self, df):
        """PSR을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['PSR'] = df['시가총액'] / df['매출액']

        return df

    def getPCR(self, df):
        """PCR을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['PCR'] = df['시가총액'] / df['영업현금흐름']

        return df

    def getEV_EBITDA(self, df):
        """EV/EBITDA를 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['EV'] = df['시가총액'] + df['장기차입금'] + df['단기차입금'] - df['현금및현금성자산']
        df['EV/EBITDA'] = df['EV'] / df['EBITDA']

        return df

    def getEV_Sales(self, df):
        """EV/Sales를 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['EV'] = df['시가총액'] + df['장기차입금'] + df['단기차입금'] - df['현금및현금성자산']
        df['EV/Sales'] = df['EV'] / df['매출액']

        return df

    def getSafetyMargin(self, df):
        """안전마진을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['NCAV'] = df['유동자산'] - df['부채']
        df['SafetyMargin'] = df['NCAV'] - df['시가총액'] * 1.5

        return df

    def getLiability_Equity(self, df):
        """부채비율을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['Liability/Equity'] = df['부채'] / df['자본']

        return df

    def getDebt_Equity(self, df):
        """차입금비율을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['Debt/Equity'] = (df['장기차입금'] + df['단기차입금']) / df['자본']

        return df

    def getGrossMargin(self, df):
        """매출총이익률을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['GrossMargin'] = df['매출총이익'] / df['매출액']

        return df

    def getOperatingMargin(self, df):
        """영업이익률을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['OperatingMargin'] = df['영업이익'] / df['매출액']

        return df

    def getProfitMargin(self, df):
        """순이익률을 계산하여 리턴하는 함수

        Parameters
        ==========
        df : DataFrame, 재무 데이터
        """
        df['ProfitMargin'] = df['당기순이익'] / df['매출액']

        return df