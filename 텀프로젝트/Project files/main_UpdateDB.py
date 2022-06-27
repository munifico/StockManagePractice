"""
    데이터베이스 업데이트를 시현.
"""

import sqlalchemy # sql 접근 및 관리를 도와주는 패키지
from sqlalchemy import create_engine

from getStockData import getStockData
from getFinancialData import getFinancialData

server = '127.0.0.1:3306' # local server
user = 'root' # user name
password = 'js0815' # 개인 password
db = '16010980_my1st_db' # DB 이름

# sqlalchemy의 create_engine을 이용해 DB 연결
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(user,password,server,db))

GSD = getStockData()
GFD = getFinancialData()


"""
    * 임시 종목 리스트
        - read_krx_code 를 통해 종목 리스트를 로드
"""
# # 종목 리스트 데이터 가져오기
# print('종목 리스트 데이터 가져오는 중...')
# li_df = GSD.read_krx_code()
# print('데이터를 가져오는 데 성공!')
#
# # 가져온 종목 리스트, 데이터베이스에 저장
# print('데이터베이스에 저장 중...')
# li_df.to_sql(name='stocklist_table', con=engine, if_exists='append', index=False,
#              dtype = { # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
#                  'stock_code' : sqlalchemy.types.VARCHAR(10),
#                  'stock_name' : sqlalchemy.types.TEXT()
#              }
#             )
# print('데이터베이스에 저장 완료!')


"""
    * 원시 주가 (종목 리스트)
        - 종목 리스트와 상장주식수를 위한 데이터
"""
# 원시 주가 데이터 가져오기
print('원시 주가 데이터 가져오는 중...')
pr_df = GSD.getKRXPrice('ALL', '20120501', '20220501')
print('데이터를 가져오는 데 성공!')

# 가져온 원시 주가, 데이터베이스에 저장
print('데이터베이스에 저장 중...')
pr_df.to_sql(name='stock_raw_table', con=engine, if_exists='append', index=False,
             dtype = { # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                 'stock_code' : sqlalchemy.types.VARCHAR(10),
                 'stock_name' : sqlalchemy.types.TEXT(),
                 'market_name' : sqlalchemy.types.TEXT(),
                 'date' : sqlalchemy.types.DATE(),
                 'open' : sqlalchemy.types.BIGINT(),
                 'high' : sqlalchemy.types.BIGINT(),
                 'low' : sqlalchemy.types.BIGINT(),
                 'close' : sqlalchemy.types.BIGINT(),
                 'volume' : sqlalchemy.types.BIGINT(),
                 'change' : sqlalchemy.types.FLOAT(),
                 'ACC_TRDVAL' : sqlalchemy.types.BIGINT(),
                 'MKTCAP' : sqlalchemy.types.BIGINT(),
                 'LIST_SHRS' : sqlalchemy.types.BIGINT()
             }
            )
print('데이터베이스에 저장 완료!')


"""
    * 수정 주가
        - 실질적인 계산을 위한 수정 주가
"""
# 수정 주가 데이터 가져오기
print('수정 주가 데이터 가져오는 중...')
ap_df = GSD.getFDRPrice('20120501', '20220501')
print('데이터를 가져오는 데 성공!')

# 가져온 수정 주가, 데이터베이스에 저장
print('데이터베이스에 저장 중...')
ap_df.to_sql(name='stock_adjust_table', con=engine, if_exists='append', index=False,
             dtype = { # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                 'stock_code' : sqlalchemy.types.VARCHAR(10),
                 'date' : sqlalchemy.types.DATE(),
                 'open' : sqlalchemy.types.BIGINT(),
                 'high' : sqlalchemy.types.BIGINT(),
                 'low' : sqlalchemy.types.BIGINT(),
                 'close' : sqlalchemy.types.BIGINT(),
                 'volume' : sqlalchemy.types.BIGINT(),
                 'change' : sqlalchemy.types.FLOAT(),
             }
            )
print('데이터베이스에 저장 완료!')


"""
    * 손익계산서
"""
# 손익계산서 데이터 가져오기
print('손익계산서 데이터 가져오는 중...')
is_df = GFD.getAllIncomeStat('Consolidated', 'T', '20220401')
print('데이터를 가져오는 데 성공!')

# 가져온 손익계산서, 데이터베이스에 저장
print('데이터베이스에 저장 중...')
is_df.to_sql(name='income_table', con=engine, if_exists='append', index=False,
             dtype = {
                '종목' : sqlalchemy.types.VARCHAR(10),
                '기간' : sqlalchemy.types.DATE(),
                '재무제표종류' : sqlalchemy.types.VARCHAR(20),
                '매출액' : sqlalchemy.types.BIGINT(),
                '매출원가' : sqlalchemy.types.BIGINT(),
                '매출총이익' : sqlalchemy.types.BIGINT(),
                '판매비와관리비' : sqlalchemy.types.BIGINT(),
                '유무형자산상각비' : sqlalchemy.types.BIGINT(),
                '영업이익(발표기준)' : sqlalchemy.types.BIGINT(),
                '금융수익' : sqlalchemy.types.BIGINT(),
                '금융원가' : sqlalchemy.types.BIGINT(),
                '기타수익' : sqlalchemy.types.BIGINT(),
                '기타비용' : sqlalchemy.types.BIGINT(),
                '관계기업관련손익' : sqlalchemy.types.BIGINT(),
                'EBITDA' : sqlalchemy.types.BIGINT(),
                'EBIT' : sqlalchemy.types.BIGINT(),
                '법인세비용' : sqlalchemy.types.BIGINT(),
                '계속영업이익' : sqlalchemy.types.BIGINT(),
                '중단영업이익' : sqlalchemy.types.BIGINT(),
                '당기순이익' : sqlalchemy.types.BIGINT(),
                '지배주주순이익' : sqlalchemy.types.BIGINT(),
                '비지배주주순이익' : sqlalchemy.types.BIGINT()
             }
            )
print('데이터베이스에 저장 완료!')


"""
    * 재무상태표
"""
# 재무상태표 데이터 가져오기
print('재무상태표 데이터 가져오는 중...')
bs_df = GFD.getAllBalanceSheet('Consolidated', 'Q', '20220401')
print('데이터를 가져오는 데 성공!')

# 가져온 재무상태표, 데이터베이스에 저장
print('데이터베이스에 저장 중...')
bs_df.to_sql(name='balance_table', con=engine, if_exists='append', index=False,
             dtype = {
                '종목' : sqlalchemy.types.VARCHAR(10),
                '기간' : sqlalchemy.types.DATE(),
                '재무제표종류' : sqlalchemy.types.VARCHAR(20),
                '자산' : sqlalchemy.types.BIGINT(),
                '유동자산' : sqlalchemy.types.BIGINT(),
                '재고자산' : sqlalchemy.types.BIGINT(),
                '매출채권' : sqlalchemy.types.BIGINT(),
                '현금및현금성자산' : sqlalchemy.types.BIGINT(),
                '비유동자산' : sqlalchemy.types.BIGINT(),
                '기타금융업자산' : sqlalchemy.types.BIGINT(),
                '부채' : sqlalchemy.types.BIGINT(),
                '유동부채' : sqlalchemy.types.BIGINT(),
                '단기차입금' : sqlalchemy.types.BIGINT(),
                '비유동부채' : sqlalchemy.types.BIGINT(),
                '장기차입금' : sqlalchemy.types.BIGINT(),
                '기타금융업부채' : sqlalchemy.types.BIGINT(),
                '자본' : sqlalchemy.types.BIGINT(),
                '지배기업주주지분' : sqlalchemy.types.BIGINT(),
                '비지배주주지분' : sqlalchemy.types.BIGINT()
             }
            )
print('데이터베이스에 저장 완료!')


"""
    * 현금흐름표
"""
# 현금흐름표 데이터 가져오기
print('현금흐름표 데이터 가져오는 중...')
cf_df = GFD.getAllCashflowStat('Consolidated', 'T', '20220401')
print('데이터를 가져오는 데 성공!')

# 가져온 현금흐름표, 데이터베이스에 저장
print('데이터베이스에 저장 중...')
cf_df.to_sql(name='cashflow_table', con=engine, if_exists='append', index=False,
             dtype = {
                '종목' : sqlalchemy.types.VARCHAR(10),
                '기간' : sqlalchemy.types.DATE(),
                '재무제표종류' : sqlalchemy.types.VARCHAR(20),
                '영업현금흐름' : sqlalchemy.types.BIGINT(),
                '당기순손익' : sqlalchemy.types.BIGINT(),
                '투자현금흐름' : sqlalchemy.types.BIGINT(),
                '재무현금흐름' : sqlalchemy.types.BIGINT(),
                '현금자산증가' : sqlalchemy.types.BIGINT()
             }
            )
print('데이터베이스에 저장 완료!')

# DB 접속 해제
engine.dispose()
#cur.close()
#conn.close()