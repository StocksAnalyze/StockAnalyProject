import MongoDriver
from pykrx import stock, bond
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import warnings

global_today = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
DEBUG = False

class StockKr:
    def __init__(self):
        self.mongo = MongoDriver.MongoDB()

        self.now = datetime.now()
        self.today = "{0:0>2}{1:0>2}{2:0>2}".format(self.now.year, self.now.month, self.now.day)
        self.cwd = os.getcwd()

        # 주식코드 : 주식명 --> local file로 저장
        self.tk_KOSPI_tkdict = dict()
        self.tk_KOSDAQ_tkdict = dict()
        self.tk_total_dict = dict()

        # 데이터를 읽어오는 딕셔너리.
        self.thema_tmp = ["lg에너지솔루션", "posco홀딩스", "포스코퓨처엠", "sk이노베이션", "lg화학",
                          "삼성전자", "sk하이닉스", "에코프로", "에코프로비엠", "미래나노텍"]
        self.thema_KOSPI_tkdict = dict()  # 테마주(관심주) 중 코스피 데이터 가져오기.
        self.thema_KOSDAQ_tkdict = dict()  # 테마주(관심주) 중 코스닥 데이터 가져오기.

        self.thema_total_dict = dict()  # 테마주(관심주) 중 코스피 + 코스닥 데이터 가져오기.
        self.force_total_dict = dict()  # 오늘의 세력주(거래량폭등주) 데이터 가져오기. <-- 기본 상위 10개

    ''' AUTO. 각 Module Wrapper Method _ Junhyeong(20190511) '''
    def module (self, code_update=False, dayinfo_update=True):
        ''' ALL-OUT Module __ DayInfo DataBase 갱신 통합모듈 *Junhyeong(20190511)
        :param code_update: Ticker (Company Code) Update 여부
        :param dayinfo_update: 
        :param dayinfo_sub_update: 사용 X
        :return: 
        '''
        if self.mongo.err:
            print("MongoDB에 연결할 수 없습니다")
            return False

        #Ticker 불러오기 (없으면 갱신 krx Interface 사용) _ Junhyeong(20190511)
        print(f"\n1. 회사별 주식코드를 가져옵니다... \n")
        self.module_readTr(update=code_update)

        if dayinfo_update:
            print("\n2. 일봉데이터를 조회합니다 \n")
            self.update_day_info()
        '''
        if dayinfo_sub_update == True:
            print("\n\n 공매도, 거래량 데이터를 조회합니다 \n\n")
            self.update_day_chart(sub=True)
        '''

        ''' 주식종목 코드 및 원하는 주식 종목 추출 메소드류'''
        
    def module_readTr(self, update=False):
        ''' 회사 별 주식 코드를 가져오는 통합 모듈 (Read | Get) _ Junhyeong(20190511)
        :param update: False(갱신안함), True(갱신함)
        :return:
        '''
        print("[주식코드 가져오는중...] ")
        self.readTicker()
        if update or len(self.tk_KOSDAQ_tkdict) == 0:
            self.writeTicker("KOSDAQ")
        if update or len(self.tk_KOSPI_tkdict) == 0:
            self.writeTicker("KOSPI")
        self.readTicker()
        # 통합 딕셔너리 설정
        self.tk_total_dict.update(self.tk_KOSDAQ_tkdict)
        self.tk_total_dict.update(self.tk_KOSPI_tkdict)

        if DEBUG:
            self.readTmpThema() #임시방편으로 몇 개만 해봄
        else:
            self.thema_total_dict = self.tk_total_dict


    ''' A. Ticker 조회 메소드 _ Junhyeong(20190511) '''
    def readTicker(self):
        ''' Ticker 정보를 MongoDB 로부터 GET (Junhyeong_(20190511))
        :return:
        '''
        dictDAQ = self.mongo.read("StockCode", "KOSDAQ")
        for item in dictDAQ:
            self.tk_KOSDAQ_tkdict[item["company"]] = item["code"]
        dictPI = self.mongo.read("StockCode", "KOSPI")
        for item in dictPI:
            self.tk_KOSPI_tkdict[item["company"]] = item["code"]

    def writeTicker(self, market="ALL"):
        ''' 현재 상장된 회사에 대한 회사코드를 가져오는 메소드 _ (Junhyeong(20190511))
        :param market: KOSDAQ, KOSPI, ALL, ..
        :return:
        '''
        tk = stock.get_market_ticker_list(self.today, market)
        print(f"상장 코드 갱신... : {market}")
        queryList = []
        for t in tk:
            name = stock.get_market_ticker_name(t)
            queryList.append({"company":name, "code":t})
        self.mongo.insert("StockCode", market, queryList, "code", True)

    def readTmpThema(self):
        ''' 임시방편으로 관심주르를 읽어오는 파일 _ (Junhyeong(20190511))
        :return:
        '''
        print("[관심주를 가져오는 중...] ")
        for l in self.thema_tmp:
            l = l.upper()
            if l in self.tk_KOSPI_tkdict:
                self.thema_KOSPI_tkdict[l] = self.tk_KOSPI_tkdict[l]
            elif l in self.tk_KOSDAQ_tkdict:
                self.thema_KOSDAQ_tkdict[l] = self.tk_KOSDAQ_tkdict[l]
            else:
                print("[name error] : \'" + l + "\' 사명에 해당하는 주식코드가 없습니다.")
                continue

        print("KOSPI : ", end="")
        print(self.thema_KOSPI_tkdict)
        print("KOSDAQ : ", end="")
        print(self.thema_KOSDAQ_tkdict)
        print("------------------------")

        # 전체 파일리스트 관리.
        #self.thema_total_dict.update(self.thema_KOSPI_tkdict)
        #self.thema_total_dict.update(self.thema_KOSDAQ_tkdict)


    ''' B. Day Information (일봉 시가, 고가, 매도 정보 크롤링...) _ Junhyeong(20190511) '''
    def update_day_info(self, sub=False, updateTime=global_today):
        ''' Get일봉데이터 --> DayInfo.회사코드 _ (Junhyeong(20190511))
            self.thema_total_dict 를 기준으로 제작됨 <- 수정가능
        :param sub:
        :param updateTime: date() 형태로 넣을 것.
        :return:
        '''

        for company, tr_code in self.thema_total_dict.items():
            #last_date = self.mongo.read_last_one("DayInfo", "Info","날짜", {"티커": tr_code})
            last_date_obj = self.mongo.read_list_obj("DayInfo", "Info", "날짜", {"티커": tr_code})
            # 없는 경우 새 갱신 - 기본 120일..
            start_dt = self.day_counter(offset=230, pos=-1)

            if last_date_obj:
                last_date = self.mongo.read_last_date("DayInfo", "Info", {"회사명": company})
                if last_date:
                    start_dt = last_date["날짜"] + timedelta(days=1)

            print(f"{company} 회사의 일봉 데이터를 가져오는중 ...")
            self.get_day_info(company, tr_code, start_dt)

    def get_day_info(self, company: str, tk: str, start: datetime):
        ''' 오늘을 기준으로 start 까지 받아오기.
        :param tk: 회사코드 (Ticker)
        :param start: datetime 프레임
        '''

        base = start
        base_end = self.day_counter(base, 80, 1)
        while base < global_today:
            try:
                df = self.get_day_info_krx(tr_code=tk, start_dt=base, end_dt=base_end)
                if df.empty:
                    return
            except Exception:
                return
            base = df.index[-1] + timedelta(days=1)
            base_end = self.day_counter(base, 80, 1)

            df = df.drop(columns=["시가"])
            # DataFrame --> Dictionary (열 이름 겹침 워닝은 무시하도록 설정)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore") # 워닝 무시
                df_to_dict = df.reset_index().to_dict(orient="records")

            print(df_to_dict)
            self.mongo.insert_list("DayInfo", "Info",[company, tk], df_to_dict,
                                   primaryKey=tk, primaryKeySet=True)


    def get_day_info_krx(self, tr_code: str, start_dt: datetime, end_dt: datetime, mode="d")\
            -> pd.DataFrame:
        ''' KRX, NaverAPI, 한국투자증권 등을 이용한 정보취합.
            >> Junhyeong (20190511)
        :param tr_code: 회사 코드
        :param start_dt: 조회 시작 날짜 : Dataframe
        :param end_dt: 조회 끝 날짜 : DataFrame
        :param mode: "d" (일봉), "r" (실시간?)
        :return: 일봉 종합 데이터프레임
        '''
        start = self.convert_date_string(start_dt) ; end = self.convert_date_string(end_dt)
        print("{종가,시가,고가,거래량을 가져오는 중 ...}")
        df1 = stock.get_market_ohlcv(start, end, tr_code, mode)
        '''
        print("{시가총액 정보를 가져오는 중 ...}")
        df2 = stock.get_market_cap(start, end, tr_code, mode)
        print("{매수 거래량을 가져오는 중 ...}")
        df3 = stock.get_market_trading_volume_by_date(start, end, tr_code)
        name_list = []
        for item in df3.columns:
            name_list.append(str(item) + "_매수")
        df3.columns = name_list
        print("{매도 거래량을 가져오는 중 ...}")
        df4 = stock.get_market_trading_volume_by_date(start, end, tr_code, on="매도")
        # 공매도 잔고는 3일전이라 NaN 데이터가 들어가므로 이는 따로 구하는게 좋아보임
        ## Author : Junhyeong(20190511)
        print("{공매도, 상장수, 시총, 공매도 비중 정보를 가져오는중 ...}")
        df5 = stock.get_shorting_balance_by_date(start, end, tr_code)
        
        name_list2 = []
        for item in df4.columns:
            name_list2.append(str(item) + "_매도")
        df4.columns = name_list2
        df = pd.concat([df1, df2, df3, df4], axis=1)
        df = df.loc[:, ~df.T.duplicated()]
        if len(df) != 0:
            print(df.tail(5))
            '''
        return df1 #df

    ''' C. Read SQL Mehtod (Day Info Crawling Information Method) _ Junhyeong(20190511) '''
    def readDaySQL(self, company: str) -> pd.DataFrame:
        ''' MongoDB (DayInfo.회사코드) 에서 DayInfo 정보 (일봉, 거래량..) 받아오기
        _ Junhyeong (20190511)
        :param company: reading할 회사
        :return: 일봉 Dataframe
        '''

        # 회사가 DB에 없을 경우 빈 DataFrame 리턴
        try:
            query = {
                "$or": [
                    {"회사명": company},
                    {"티커": company}
                ]
            }
            findingSQL = self.mongo.read_list_obj("DayInfo", "Info", query)
            if findingSQL:
                return pd.DataFrame(findingSQL["data"]).set_index("날짜")
        except Exception:
            print(f"{company} 는 invalid 데이터 입니다.")
            return pd.DataFrame()

    ''' D. 거래량 상위 회사 추출 메소드 _ Junhyeong (20190511) '''
    def find_small_module(self, rank=10):
        ''' 거래량 상위 회사 추출 (_Junhyeong(20190511))
        :param rank:
        :return:
        '''
        self.find_small_init(rank=rank)
        #self.find_small_getDayinfo()

    def find_small_init(self, rank=10):
        ''' 거래량 기준 rank회사만큼 추출하여 MongoDB (StockCode.HIGHVOLUMN) 에 저장.
        :param rank: 상위 N개의 회사 추출 (거래량 기준)
        :return:
        '''
        today = self.today
        df = pd.DataFrame()
        date_back = 0
        while len(df) == 0:
            self.today = "{0:0>2}{1:0>2}{2:0>2}".format(self.now.year, self.now.month, self.now.day - date_back)
            df = stock.get_market_cap(self.today)
            date_back += 1

        tableName = "HIGHVOLUMN"
        cost_dict = {}
        tmp_list = []
        low_cost = 200000000000
        high_cost = 500000000000

        for ticker in df.index:
            tr = df.loc[ticker, "시가총액"]
            if tr >= low_cost and tr <= high_cost:
                tmp_list.append([ticker, df.loc[ticker, "거래량"]])

        print(f"거래량 정렬중.. {rank} 개")
        sorted_list = sorted(tmp_list, key=lambda x: -x[1])
        ticker_list = []
        #상위 10개만 골라오기
        for ticker, tr_size in sorted_list[:rank]:
            name = stock.get_market_ticker_name(ticker)
            print(name, tr_size)
            ticker_list.append([name, ticker, tr_size])
            self.force_total_dict[name] = ticker

        print(ticker_list)

    ''' E. Utility _ Junhyeong(20190511)'''
    def day_counter(self, start=global_today, offset=60, pos=-1):
        ''' 날짜 계산기 : start 기준으로 몇 일 offset 만큼 갈 것인가? _ (Junhyeong (20190511))
        :param start: "20240309" 형태로 입력할 것.
        :param offset: 가고싶은 날짜,
        :param pos: 1 (증가하는방향), -1 (감소하는방향)
        :return: 계산된 datetime()
        :: ex) start=datetime(2024,03,09) , offset=60, pos=-1 -> datetime(2024,01,10)
        '''

        s_time = start
        off = (offset-1) * pos
        s_time += timedelta(days=off)
        return s_time

    def convert_date_string(self, date: datetime):
        ''' 날짜 변환기 : datetime(2024,03,09) -> "20240309"
        :return: "20240309"
        '''
        return "{0:0>2}{1:0>2}{2:0>2}".format(date.year, date.month, date.day)

if __name__ == "__main__":
    obj = StockKr()
    obj.module(code_update=False, dayinfo_update=True)
    print(obj.readDaySQL("005930"))