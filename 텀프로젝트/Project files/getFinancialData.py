import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from tqdm import tqdm
from getStockData import getStockData

"""
    * getFinancialData
        - 재무제표 데이터를 Fnguide로부터 가져옴.
"""
class getFinancialData:
    server = '127.0.0.1:3306'  # local server
    user = 'root'  # user name
    password = 'js0815'  # 개인 password
    db = '16010980_my1st_db'  # DB 이름
    GSD = getStockData()

    # 달의 마지막 날로 바꾸서 리턴
    def last_day_of_month(self, any_date):
        """현재 달의 마지막 날을 리턴하는 함수

        Parameters
        ==========
        any_date : datetime, 날짜
        """
        next_month = any_date.replace(day=28) + timedelta(days=4)  # this will never fail
        return next_month - timedelta(days=next_month.day)

    # 한 종목에 대해서 손익계산서 데이터를 로드
    def getIncomeStat(self, stock_code, rpt_type, freq):
        """[FnGuide] 공시기업의 최근 3개 연간 및 4개 분기 손익계산서를 수집하는 함수

        Parameters
        ==========
        stock_code : str, 종목코드
        rpt_type   : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq       : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기), 'T'(트레일링)
        """
        items_en = ['rev', 'cgs', 'gross', 'sga', 'sga1', 'sga2', 'sga3', 'sga4', 'sga5', 'sga6', 'sga7', 'sga8', 'opr', 'opr_',  # 14
                    'fininc', 'fininc1', 'fininc2', 'fininc3', 'fininc4', 'fininc5',  # 6
                    'fininc6', 'fininc7', 'fininc8', 'fininc9', 'fininc10', 'fininc11',  # 6
                    'fincost', 'fincost1', 'fincost2', 'fincost3', 'fincost4', 'fincost5',  # 6
                    'fincost6', 'fincost7', 'fincost8', 'fincost9', 'fincost10',  # 5
                    'otherrev', 'otherrev1', 'otherrev2', 'otherrev3', 'otherrev4', 'otherrev5', 'otherrev6', 'otherrev7', 'otherrev8',  # 9
                    'otherrev9', 'otherrev10', 'otherrev11', 'otherrev12', 'otherrev13', 'otherrev14', 'otherrev15', 'otherrev16',  # 8
                    'othercost', 'othercost1', 'othercost2', 'othercost3', 'othercost4', 'othercost5',  # 6
                    'othercost6', 'othercost7', 'othercost8', 'othercost9', 'othercost10', 'othercost11', 'othercost12', # 7
                    'otherpl', 'otherpl1', 'otherpl2', 'otherpl3', 'otherpl4', 'ebit', 'tax', 'contop', 'discontop', 'netinc']  # 12

        if rpt_type.upper() == 'CONSOLIDATED':
            # 연결 연간 손익계산서(ReportGB=D)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?" + "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(
                stock_code)
            items_en = items_en + ['netinc1', 'netinc2']

        else:
            # 별도 연간 손익계산서(ReportGB=B)
            url = "https://comp.fnguide.com/SV02/asp/SVD_Finance.asp?" + "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(
                stock_code)

        # Header 설정
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':  # 연간 손익계산서 영역 추출
            is_a = soup.find(id='divSonikY')
            num_col = 4  # 최근 4개 연간 데이터
        else:  # 분기 손익계산서 영역 추출 freq.upper() == 'Q'
            is_a = soup.find(id='divSonikQ')
            num_col = 4  # 최근 4개 분기 데이터

        # 존재하지 않는 데이터(None) 처리
        if is_a is None:
            return
        else:
            is_a = is_a.find_all(['tr'])

        try:
            # 연간 손익계산서 항목 펼친 뒤 하위 항목 추출
            items_kr = [is_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                        for m in range(1, len(is_a))]

            # 최근 4개 연간 손익계산서 자료 수집
            period = [is_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

            # 항목별 최근 4개 연간데이터 불러오기
            for item, i in zip(items_en, range(1, len(is_a))):
                temps = []
                for j in range(0, num_col):
                    temp = [float(is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                                if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                                else (0 if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                          else 0)]
                    temps.append(temp[0])

                # item_en 내 각 항목을 global 변수로 지정하고 값 저장
                globals()[item] = temps

            # 지배/비지배 항목 청리
            if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간 손익계산서는 아무 것도 하지 않음
                pass

            else:  # 별도 연간 손익계산서 해당 항목을 Null값으로 채움
                globals()['netinc1'], globals()['netinc2'] = [np.NaN] * num_col, [np.NaN] * num_col
        except:
            print("\n예외가 발생하여 데이터를 불러오지 못했습니다.\n - stock_code: {}".format(stock_code))
            return

        # 손익계산서 각 컬럼에 위에서 저장한 global 변수값을 지정하여 dataframe으로 변환
        is_domestic = pd.DataFrame({'종목': stock_code, '기간': period,
                                    '매출액': rev, '매출원가': cgs, '매출총이익': gross,
                                    '판매비와관리비': sga, '유무형자산상각비': sga2, '영업이익': opr,
                                    '금융수익': fininc, '금융원가': fincost,
                                    '기타수익': otherrev, '기타비용': othercost,
                                    '관계기업관련손익': otherpl, 'EBIT': ebit,
                                    '법인세비용': tax, '계속영업이익': contop,
                                    '중단영업이익': discontop, '당기순이익': netinc,
                                    '지배주주순이익': globals()['netinc1'],
                                    '비지배주주순이익': globals()['netinc2']})

        # '재무제표종류'와 'EBITDA'를 데이터프레임에 추가
        is_domestic['재무제표종류'] = rpt_type + '_' + freq.upper()
        is_domestic['EBITDA'] = is_domestic['유무형자산상각비'] + is_domestic['영업이익']

        # freq가 'T'인 경우, 트레일링 데이터로 변환
        if freq.upper() == 'T':
            df_trailing = is_domestic[[
                '매출액', '매출원가', '매출총이익',
                '판매비와관리비', '유무형자산상각비', '영업이익',
                '금융수익', '금융원가',
                '기타수익', '기타비용',
                '관계기업관련손익', 'EBITDA', 'EBIT',
                '법인세비용', '계속영업이익',
                '중단영업이익', '당기순이익',
                '지배주주순이익',
                '비지배주주순이익']].rolling(4).sum()
            df_trailing = pd.concat([is_domestic[['종목', '기간', '재무제표종류']], df_trailing], axis=1)

            df_trailing = df_trailing.dropna()
        else:
            df_trailing = is_domestic

        # '기간'의 형태를 변환 ('%Y/%m' -> '%Y-%m-%d')
        df_trailing['기간'] = df_trailing['기간'].map(lambda str: self.last_day_of_month(datetime.strptime(str, '%Y/%m')))
        df_trailing['기간'] = df_trailing['기간'].map(lambda date: date.strftime('%Y-%m-%d'))

        return df_trailing

    # 모든 종목에 대해서 손익계산서 데이터를 로드
    def getAllIncomeStat(self, rpt_type, freq, date=datetime.now().strftime('%Y%m%d')):
        """모든 종목에 대한 손익계산서를 로드하는 함수

        Parameters
        ==========
        rpt_type : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq     : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기), 'T'(트레일링)
        date     : str, 날짜
        """
        # 해당 날짜에서의 종목 리스트를 데이터베이스로부터 로드
        date = datetime.strptime(date, '%Y%m%d')
        krx_df = self.GSD.getStockList(date)

        # 종목 코드만을 추려내어 리스트로 변환
        code_list = krx_df['stock_code'].values.tolist()
        # code_list = ['005930', '066570']

        df = pd.DataFrame()

        # 모든 종목에 대해서 손익계산서를 fnguide로부터 로드
        for code in tqdm(code_list):
            new_df = self.getIncomeStat(code, rpt_type, freq)
            df = pd.concat([df, new_df])

        return df

    def getBalanceSheet(self, stock_code, rpt_type, freq):
        """[FnGuide] 공사기업의 최근 3개 연간 및 4개 분기 재무상태표를 수집하는 함수

        Parameters
        ==========
        stock_code : str, 종목코드
        rpt_type   : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq       : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기)
        """
        items_en = ['assets', 'curassets', 'curassets1', 'curassets2', 'curassets3', 'curassets4', 'curassets5',  # 7
                    'curassets6', 'curassets7', 'curassets8', 'curassets9', 'curassets10', 'curassets11',  # 6
                    'ltassets', 'ltassets1', 'ltassets2', 'ltassets3', 'ltassets4', 'ltassets5', 'ltassets6',
                    'ltassets7'  # 8
                    'ltassets8', 'ltassets9', 'ltassets10', 'ltassets11', 'ltassets12', 'ltassets13', 'finassets',  # 7
                    'liab', 'curliab', 'curliab1', 'curliab2', 'curliab3', 'curliab4', 'curliab5',  # 7
                    'curliab6', 'curliab7', 'curliab8', 'curliab9', 'curliab10', 'curliab11', 'curliab12', 'curliab13',
                    # 8
                    'ltliab', 'ltliab1', 'ltliab2', 'ltliab3', 'ltliab4', 'ltliab5', 'ltliab6',  # 7
                    'ltliab7', 'ltliab8', 'ltliab9', 'ltliab10', 'ltliab11', 'ltliab12', 'finliab',  # 7
                    'equity', 'equity1', 'equity2', 'equity3', 'equity4', 'equity5', 'equity6', 'equity7',
                    'equity8']  # 9

        if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간 재무상태표(ReportGB=D)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?" + "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(stock_code)

        else:  # 별도 연간 재무상태표(ReportGB=B)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?" + "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(stock_code)


        itmes_en = [item for item in items_en if item not in ['equity1', 'equity8']]

        # Header 설정
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':  # 연간 재무상태표 영억 추출
            bs_a = soup.find(id='divDaechaY')
            num_col = 4  # 최근 4개 연간 데이터
        else:  # 분기 재무상태표 영역 추출 (freq.upper() == 'Q')
            bs_a = soup.find(id='divDaechaQ')
            num_col = 4  # 최근 4개 분기 데이터

        # 존재하지 않는 데이터(None) 처리
        if bs_a is None:
            return
        else:
            bs_a = bs_a.find_all(['tr'])

        try:
            # 연간 재무상태표 항목 펼친 뒤 하위 항목 추출
            items_kr = [bs_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                        for m in range(1, len(bs_a))]

            # 최근 4개 연간 재무상태표 자료 수집(첫 번째부터 세 번째 컬럼까지 수집)
            period = [bs_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

            for item, i in zip(items_en, range(1, len(bs_a))):
                # 항목별 최근 4개 연간데이터 불러와서 temps에 모으기
                temps = []
                for j in range(0, num_col):  # 첫 번째부터 세 번째 컬럼까지 수집
                    temp = [float(bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                                if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                                else (0 if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                          else 0)]
                    temps.append(temp[0])

                # item_en 내 각 항목을 global 변수로 지정하고 temps값 저장
                globals()[item] = temps

            if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간 재무상태표 아무 것도 하지 않음
                pass
            else:  # 별도 연간 재무상태표는 연간에만 존재하는 항목을 Null값으로 채움
                globals()['equity1'], globals()['equity8'] = [np.NaN] * num_col, [np.NaN] * num_col
        except:
            print("\n예외가 발생하여 데이터를 불러오지 못했습니다.\n - stock_code: {}".format(stock_code))
            return

        bs_domestic = pd.DataFrame({
            "종목": stock_code, "기간": period, # 2
            "자산": assets, "유동자산": curassets, # 2
            "재고자산": curassets1, "매출채권": curassets4, # 2
            "현금및현금성자산": curassets10, # 1
            "비유동자산": ltassets, "기타금융업자산": finassets, # 2
            "부채": liab, "유동부채": curliab, "단기차입금": curliab2, # 3
            "비유동부채": ltliab, "장기차입금": ltliab2, "기타금융업부채": finliab, # 3
            "자본": equity,
            "지배기업주주지분": globals()['equity1'],
            "비지배주주지분": globals()['equity8'] # 3
        })

        # '재무제표종류'를 데이터프레임에 추가
        bs_domestic['재무제표종류'] = rpt_type + '_' + freq.upper()

        # if freq.upper() == 'T':
        #     df_trailing = bs_domestic[[
        #         "자산", "유동자산", # 2
        #         "비유동자산", "기타금융업자산", # 2
        #         "부채", "유동부채", # 2
        #         "비유동부채", "기타금융업부채", # 2
        #         "자본",
        #         "지배기업주주지분",
        #         "비지배주주지분"]].rolling(4).sum()
        #     df_trailing = pd.concat([bs_domestic[['종목', '기간', '재무제표종류']], df_trailing], axis=1)
        #
        #     df_trailing = df_trailing.dropna()
        # else:
        #     df_trailing = bs_domestic
        #
        # df_trailing['기간'] = df_trailing['기간'].map(lambda str: time.strptime(str, '%Y/%m'))
        # df_trailing['기간'] = df_trailing['기간'].map(lambda date: time.strftime('%Y-%m-%d', date))
        #
        # return df_trailing

        # bs_domestic 데이터프레임을 재구성 (열 순서 바꾸기)
        part_bs = bs_domestic[[
                "자산", "유동자산",
                "재고자산", "매출채권",
                "현금및현금성자산",
                "비유동자산", "기타금융업자산",
                "부채", "유동부채", "단기차입금",
                "비유동부채", "장기차입금", "기타금융업부채",
                "자본",
                "지배기업주주지분",
                "비지배주주지분"]]
        bs_domestic = pd.concat([bs_domestic[['종목', '기간', '재무제표종류']], part_bs], axis=1)

        # '기간'의 형태를 변환 ('%Y/%m' -> '%Y-%m-%d')
        bs_domestic['기간'] = bs_domestic['기간'].map(lambda str: self.last_day_of_month(datetime.strptime(str, '%Y/%m')))
        bs_domestic['기간'] = bs_domestic['기간'].map(lambda date: date.strftime('%Y-%m-%d'))

        return bs_domestic

    def getAllBalanceSheet(self, rpt_type, freq, date=datetime.now().strftime('%Y%m%d')):
        """모든 종목에 대한 재무상태표를 로드하는 함수

        Parameters
        ==========
        rpt_type : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq     : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기)
        date     : str, 날짜
        """
        # 해당 날짜에서의 종목 리스트를 데이터베이스로부터 로드
        date = datetime.strptime(date, '%Y%m%d')
        krx_df = self.GSD.getStockList(date)

        # 종목 코드만을 추려내어 리스트로 변환
        code_list = krx_df['stock_code'].values.tolist()
        # code_list = ['005930', '066570']

        df = pd.DataFrame()

        # 모든 종목에 대해서 재무상태표를 fnguide로부터 로드
        for code in tqdm(code_list):
            new_df = self.getBalanceSheet(code, rpt_type, freq)
            df = pd.concat([df, new_df])

        return df

    def getCashflowStat(self, stock_code, rpt_type, freq):
        """[FnGuide] 공시기업의 최근 3개 연간 및 4개 분기 현금흐름표를 수집하는 함수

        Parameters
        ==========
        stock_code : str, 종목코드
        rpt_type   : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq       : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기), 'T'(트레일링)
        """
        # 연간 현금흐름표 항목(22개)
        items_en = ["cfo", "cfo1", "cfo2", "cfo3", "cfo4", "cfo5", "cfo6", "cfo7", # 8
                    "cfi", "cfi1", "cfi2", "cfi3", "cff", "cff1", "cff2", "cff3", # 8
                    "cff4", "cff5", "cff6", "cff7", "cff8", "cff9"] # 8

        if rpt_type.upper() == 'CONSOLIDATED': # 연결 연간 현금흐름표(ReportGB=D)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?" + "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(stock_code)
        else: # 별도 연간 현금흐름표(ReportGB=B)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?" + "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(stock_code)

        # Header 설정
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A': # 연간 현금흐름표 영역 추출
            cf_a = soup.find(id='divCashY')
            num_col = 4 # 최근 4개 연간 데이터
        else: # 분기 현금흐름표 영역 추출 (freq.upper() == 'Q')
            cf_a = soup.find(id='divCashQ')
            num_col = 4 # 최근 4개 분긴 데이터

        # 존재하지 않는 데이터(None) 처리
        if cf_a is None:
            return
        else:
            cf_a = cf_a.find_all(['tr'])

        try:
            # 연간 현금흐름표 항목 펼친 뒤 하위 항목 추출
            items_kr = [cf_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                        for m in range(1, len(cf_a))]

            # 최근 4개 연간 현금흐름표 자료 수집(첫 번째부터 세 번째 컬럼까지 수집)
            period = [cf_a[0].find_all('th')[n].get_text() for n in range(1, num_col+1)]

            idx = [1, 2, 3, 4, 39, 70, 75, 76, 84, 85, 99, 113, 121, 122, 134, 145, 153, 154, 155, 156, 157, 158]
            for item, i in zip(items_en, idx):
                temps = []
                for j in range(0, num_col): # 첫 번째부터 세 번째 컬럼까지 수집
                    temp = [float(cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                        if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                        else (0 if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                else 0)]
                    temps.append(temp[0])

                    # item_en 내 각 항목을 global 변수로 지정하고 값 저장
                    globals()[item] = temps
        except:
            print("\n예외가 발생하여 데이터를 불러오지 못했습니다.\n - stock_code: {}".format(stock_code))
            return

        cf_domestic = pd.DataFrame({"종목": stock_code, "기간": period,
                                    "영업현금흐름": cfo, "당기순손익": cfo1,
                                    "투자현금흐름": cfi, "재무현금흐름": cff, "현금자산증가": cff7})

        # '재무제표종류'를 데이터프레임에 추가
        cf_domestic['재무제표종류'] = rpt_type + '_' + freq.upper()

        # freq가 'T'인 경우, 트레일링 데이터로 변환
        if freq.upper() == 'T':
            df_trailing = cf_domestic[[
                "영업현금흐름", "당기순손익",
                "투자현금흐름", "재무현금흐름", "현금자산증가"]].rolling(4).sum()
            df_trailing = pd.concat([cf_domestic[['종목', '기간', '재무제표종류']], df_trailing], axis=1)

            df_trailing = df_trailing.dropna()
        else:
            df_trailing = cf_domestic

        # '기간'의 형태를 변환 ('%Y/%m' -> '%Y-%m-%d')
        df_trailing['기간'] = df_trailing['기간'].map(lambda str: self.last_day_of_month(datetime.strptime(str, '%Y/%m')))
        df_trailing['기간'] = df_trailing['기간'].map(lambda date: date.strftime('%Y-%m-%d'))

        return df_trailing

    def getAllCashflowStat(self, rpt_type, freq, date=datetime.now().strftime('%Y%m%d')):
        """모든 종목에 대한 현금흐름표를 로드하는 함수

        Parameters
        ==========
        rpt_type : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq     : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기), 'T'(트레일링)
        date     : str, 날짜
        """
        # 해당 날짜에서의 종목 리스트를 데이터베이스로부터 로드
        date = datetime.strptime(date, '%Y%m%d')
        krx_df = self.GSD.getStockList(date)

        # 종목 코드만을 추려내어 리스트로 변환
        code_list = krx_df['stock_code'].values.tolist()
        # code_list = ['005930', '066570']

        df = pd.DataFrame()

        # 모든 종목에 대해서 현금흐름표를 fnguide로부터 로드
        for code in tqdm(code_list):
            new_df = self.getCashflowStat(code, rpt_type, freq)
            df = pd.concat([df, new_df])

        return df