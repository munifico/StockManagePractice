import pandas as pd
import time, json, requests
import pymysql
import warnings
import FinanceDataReader as fdr
import exchange_calendars as ecals
from datetime import datetime, timedelta
from tqdm import tqdm

"""
    * getStockData
        - KRX로부터 원시 주가 데이터를 가져옴.
        - FDR로부터 수정 주가 데이터를 가져옴.
"""
class getStockData:
    server = '127.0.0.1:3306'  # local server
    user = 'root'  # user name
    password = 'js0815'  # 개인 password
    db = '16010980_my1st_db'  # DB 이름

    def getDateRange(self, st_date, end_date):
        """날짜의 범위를 리턴하는 함수

        Parameters
        ==========
        st_date  : datetime, 시작 날짜
        end_date : datetime, 종료 날짜
        """
        print('{} ~ {}'.format(st_date, end_date))
        for n in range(int((end_date - st_date).days)+1):
            #print(st_date + timedelta(days=n))
            yield st_date + timedelta(days=n)

    def read_krx_code(self):
        """KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=' \
              'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'stock_code', '회사명': 'stock_name'})
        krx.stock_code = krx.stock_code.map('{:06d}'.format)

        return krx

    def getRecentOpenDate(self, date):
        """date 또는 date 바로 직전의 주식 시장 개장일을 리턴하는 함수

        Parameters
        ==========
        date : datetime, 날짜
        """
        # Warning 방지
        warnings.filterwarnings('ignore')
        # 한국 기준 주식 시장 개장일인지 확인
        XKRX = ecals.get_calendar("XKRX")  # 한국 코드
        while not (XKRX.is_session(date)):
            date = date - timedelta(days=1)

        return date

    def getRecentOpenDateReverse(self, date):
        """date 또는 date 바로 직후의 주식 시장 개장일을 리턴하는 함수

        Parameters
        ==========
        date : datetime, 날짜
        """
        # Warning 방지
        warnings.filterwarnings('ignore')
        # 한국 기준 주식 시장 개장일인지 확인
        XKRX = ecals.get_calendar("XKRX")  # 한국 코드
        while not (XKRX.is_session(date)):
            date = date + timedelta(days=1)

        return date

    def getStockList(self, date):
        """해당 날짜의 주식 종목 리스트를 데이터베이스로부터 모두 가져오는 함수

        Parameters
        ==========
        date : datetime, 날짜
        """
        # 가장 최근 주식 시장 개장일을 가져옴
        date = self.getRecentOpenDate(date)
        date = date.strftime('%Y-%m-%d')

        conn = pymysql.connect(host='localhost', port=3306, db=self.db,
                               user=self.user, passwd=self.password, autocommit=True)
        cursor = conn.cursor()

        # 원시 주가 테이블로부터 해당 날짜에 해당하는 모든 데이터를 가져옴
        sql = "SELECT * FROM STOCK_RAW_TABLE WHERE\
                            DATE(date) = '{}'".format(date)

        cursor.execute(sql)

        df = pd.DataFrame(cursor.fetchall())
        df.columns = [col[0] for col in cursor.description]

        # Nan 데이터와 중첩을 제거
        df = df.dropna()
        df = df.drop_duplicates(['stock_code'])

        return df

    def getKRXPrice(self, mktId, st_dt, end_dt):
        """원시 주가를 KRX로부터 가져오는 함수

        Parameters
        ==========
        mktId  : str, 주식 시장 종류
            'ALL', 'KOSPI', 'KOSDAQ', 'KODEX'
        st_dt  : str, 시작 날짜
        end_dt : str, 종료 날짜
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020103"
        }

        # 수집 기간 설정
        sdate = datetime.strptime(st_dt,'%Y%m%d').date()
        edate = datetime.strptime(end_dt,'%Y%m%d').date()
        dt_idx = []
        for dt in self.getDateRange(sdate, edate):
            if dt.isoweekday() < 6:
                dt_idx.append(dt.strftime("%Y%m%d"))

        daily = []
        for dt in tqdm(dt_idx):
            # 날짜별 전종목 일봉 불러오기
            p_data = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
                'mktId': mktId,
                'trdDd': dt,
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false'
            }

            url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            res = requests.post(url, headers=headers, data=p_data)
            html_text = res.content
            html_json = json.loads(html_text)
            html_jsons = html_json['OutBlock_1']

            if len(html_jsons) > 0:
                for html_json in html_jsons:
                    if html_json['TDD_OPNPRC'] == '-': # 시장이 열리지 않아 값이 없는 경우
                        continue

                    ISU_SRT_CD = html_json['ISU_SRT_CD']
                    ISU_ABBRV = html_json['ISU_ABBRV']
                    MKT_NM = html_json['MKT_NM']
                    TRD_DD = datetime.strptime(dt,'%Y%m%d').strftime('%Y-%m-%d')

                    FLUC_RT = float(html_json['FLUC_RT'].replace(',', ''))/100
                    TDD_CLSPRC = int(html_json['TDD_CLSPRC'].replace(',', ''))
                    TDD_OPNPRC = int(html_json['TDD_OPNPRC'].replace(',', ''))
                    TDD_HGPRC = int(html_json['TDD_HGPRC'].replace(',', ''))
                    TDD_LWPRC = int(html_json['TDD_LWPRC'].replace(',', ''))

                    ACC_TRDVOL = int(html_json['ACC_TRDVOL'].replace(',', ''))
                    ACC_TRDVAL = int(html_json['ACC_TRDVAL'].replace(',', ''))
                    MKTCAP = int(html_json['MKTCAP'].replace(',', ''))
                    LIST_SHRS = int(html_json['LIST_SHRS'].replace(',', ''))

                    daily.append((ISU_SRT_CD, ISU_ABBRV, MKT_NM, TRD_DD, TDD_OPNPRC, TDD_HGPRC, TDD_LWPRC, TDD_CLSPRC, ACC_TRDVOL, FLUC_RT, ACC_TRDVAL, MKTCAP, LIST_SHRS))
            else:
                pass

            # 1초 sleep
            time.sleep(1)

        if len(daily) > 0:
            daily = pd.DataFrame(daily)
            daily.columns = ['stock_code', 'stock_name', 'market_name', 'date', 'open', 'high', 'low', 'close', 'volume', 'change', 'ACC_TRDVAL', 'MKTCAP', 'LIST_SHRS']
            daily = daily.sort_values(by='date').reset_index(drop=True)
            return daily

        else:
            return pd.DataFrame()

    def load_data(self, code, start_date, end_date):
        """FinanceDataReader로부터 한 종목에 대한 수정 주가를 가져오는 함수

        Parameters
        ==========
        code       : str, 종목 코드
        start_date : str, 시작 날짜
        end_date   : str, 종료 날짜
        """
        # FinanceDataReader로부터 데이터를 가져옴
        if end_date == datetime.today().strftime('%Y-%m-%d'):
            data = fdr.DataReader(code, start_date)
        else:
            data = fdr.DataReader(code, start_date, end_date)
        data['stock_code'] = code
        return data

    def getFDRPrice(self, start_date, end_date):
        """FinanceDataReader로부터 모든 종목에 대한 수정 주가를 가져오는 함수

        Parameters
        ==========
        start_date : str, 시작 날짜
        end_date   : str, 종료 날짜
        """
        # end_date 기준으로 주식 종목 리스트를 가져옴
        date = datetime.strptime(end_date, '%Y%m%d')
        krx_df = self.getStockList(date)

        code_list = krx_df['stock_code'].values.tolist()
        # code_list = ['005930', '066570']

        df = pd.DataFrame()

        # FinanceDataReader로부터 데이터를 가져옴
        for code in tqdm(code_list):
            new_df = self.load_data(code, start_date, end_date)
            df = pd.concat([df, new_df])
            time.sleep(1)

        # 데이터프레임의 열의 이름과 순서를 바꿈
        df = df.rename_axis('date').reset_index()
        df = df.rename(columns={'Open': 'open', 'High': 'high',
                                'Low': 'low', 'Close': 'close',
                                'Volume': 'volume', 'Change': 'change'})
        df = df.reindex(columns=['stock_code', 'date',
                                 'open', 'high',
                                 'low', 'close',
                                 'volume', 'change'])

        return df