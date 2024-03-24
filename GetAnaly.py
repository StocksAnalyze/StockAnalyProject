from datetime import datetime
import pandas as pd
import talib as ta
import warnings
import GetCals

def dt(year=0, mon=0, day=0, strs=""):
    ''' (2023,08,11) or "20230811" 을 datatime 객체로 Translate 함수.
    :param year:
    :param mon:
    :param day:

    :param str:
    :return:
    '''
    if strs != "":
        return datetime(year=int(strs[:4]), month=int(strs[4:6]), day=int(strs[6:]))
    else:
        return datetime(year=year, month=mon, day=day)
def df_t(df : pd.DataFrame, index_num : int):
    ''' DataFrame 인덱스값 범위를 계산해서 구해주는 함수.
    '''
    if index_num < 0 or index_num >= len(df):
        return -1
    return df.loc[index_num]
def df_check_row(df: pd.DataFrame, row_name: str):
    ''' DataFrame에 해당 row_name이 존재하는지 여부
    '''
    df_row_list = df.columns
    return row_name in df_row_list
def df_unify (*dfs):
    ''' DataFrame을 합쳐주는 함수.
        ex) df1, df2, df3 데이터를 df로 합쳐줌.'''
    df = pd.concat(list(dfs), axis=1)
    return df.loc[:, ~df.T.duplicated()]


class StockAnaly:
    def __init__(self):
        self.calc_obj = GetCals.StockCals()
        self.info_obj = self.calc_obj.infoObj
        self.mongo = self.info_obj.mongo
        self.sma_window = self.calc_obj.sma_window
        self.high_crit = self.calc_obj.high_crit
        self.saved_df = pd.DataFrame()

        self.anal_namedict = {
            "SMA60_check": "60 days MA Trend",
            "전기_nearess_check(후행)": "Base and Conversion Narrow Status (for Lagging)",
            "전기_nearess_check": "Base and Conversion Narrow Status",
            "MACD_check": "MACD Status",
            "후행스팬_line_cross_check": "Lagging Span x Base and conversion",
            "후행스팬_bong_cross_check": "Lagging Span x Bong",
            "스팬꼬리_check": "Leading Span Tail Direction",
            "스팬위치_check": "Bong and Cloud Status",
            "전_cross_기": "Conversion x Base line",
            "봉_cross_전기": "Bong x Base and Conversion",
            "어제기준_가격비교": "Difference LastDate"
        }
        self.anal_namedict_r = dict()  # 역으로 구성된 딕셔너리.
        # 분석용 DataFrame
        self.read_df_dayinfo = pd.DataFrame()  # 일봉 데이터 가져오는 멤버변수
        self.read_df_criteria = pd.DataFrame()  # 지표 데이터 가져오는 멤버변수

        # 점수 측정용 DataFrame
        self.today_score = dict()  # {key=점수 : Value=점수}
        self.anal_score = {"O": 0,
                           "up_near": 1,
                           "up_cross": 2,
                           "up": 3,
                           "down_near": 4,
                           "down_cross": 5,
                           "down": 6,
                           "X": 7,
                           "mid": 8}
        self.anal_scoreboard = dict()


    def module(self, code_update=False, day_info=False,
               compute_criteria=True, analysis=True, percent =2):

        #Override self.calc_obj
        self.calc_obj.module(code_update=code_update, day_info=day_info, compute_criteria=compute_criteria)

        if analysis:
            self.analdict_update()
            self.module_analysis(percent=percent)

    def analdict_update(self):
        # X일 이동평균선 통과
        for item in self.sma_window:
            sma_name = "SMA" + str(item) + "_cross_check"
            dict_name = str(item) + "day cross MA Check"
            self.anal_namedict[sma_name] = dict_name

        # x이 최고가 통과여부
        for item in self.high_crit:
            sma_name = str(item) + "_highest_check"
            dict_name = str(item) + "day Highest Price Trend"
            self.anal_namedict[sma_name] = dict_name

        self.anal_namedict_r = {value: key for key, value in self.anal_namedict.items()}

    def module_analysis(self, percent):
        print("Start Analysis")

        for company, ticker in self.info_obj.thema_total_dict.items():  # 나중에 이 부분을 건들면 다른 딕셔너리에 대해서도 수행가능.
            analys_last_date = datetime(1999, 1, 1)
            print("[" + company + " 지표 분석 중 ...]")

            try:
                last_date_info = self.mongo.read_last_date("DayInfo", "Info", {"티커": ticker})
                last_date_analys = self.mongo.read_last_date("DayInfo", "Analys", {"티커": ticker}, client=self.mongo.client2) # 임시방편
                if last_date_info and last_date_analys:
                    difference = last_date_info["날짜"] - last_date_analys["날짜"]
                    if difference.days == 0:
                        continue
                    before_info = self.mongo.read_date_limits("DayInfo", "Info", {"티커": ticker}, limits=difference.days + 120)
                    before_cals = self.mongo.read_date_limits("DayInfo", "Cals", {"티커": ticker}, limits=difference.days + 120)
                    df_info = pd.DataFrame(before_info).set_index("날짜")
                    df_cals = pd.DataFrame(before_cals).set_index("날짜")
                    if not df_info.empty:
                        df_info = df_info.iloc[::-1]
                    else:
                        df_info = self.info_obj.readDaySQL(company)

                    if not df_cals.empty:
                        df_cals = df_cals.iloc[::-1]
                    else:
                        df_cals = self.calc_obj.read_days_cals(company)

                    analys_last_date = last_date_analys["날짜"]
                    self.read_df_dayinfo = df_info
                    self.read_df_criteria = df_cals
                else:
                    if not last_date_info:
                        continue
                    self.read_df_dayinfo = self.info_obj.readDaySQL(company)
                    self.read_df_criteria = self.calc_obj.read_days_cals(company)
            except Exception:
                self.read_df_dayinfo = self.info_obj.readDaySQL(company)
                self.read_df_criteria = self.calc_obj.read_days_cals(company)
            self.saved_df = pd.DataFrame(index=self.read_df_dayinfo.index)

            # 0. 60일 이동평균선의 추이
            self.sma60_direction(df_crit=self.read_df_criteria)

            # 0-2. 전환선_기준선_가까움
            self.near_line_check(df_crit=self.read_df_criteria, percent=percent)
            # 2. MACD 시그널 체킹 (하한~상한 제한걸어둠)
            self.check_macd(self.read_df_criteria)
            # 4. 스팬친구들
            self.cross_backspan_line(df_crit=self.read_df_criteria, percent=percent)
            self.cross_backspan(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)
            self.check_spantail(df_crit=self.read_df_criteria)
            self.check_span_position(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)
            # 5. 전환선 >= 기준선
            self.span_line_cross(df_crit=self.read_df_criteria, percent=percent)

            # 6. 봉 >= 기준/전환선
            self.bong_cross_line(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria, percent=percent)
            # 1. XX일 이동선을 통과했는가?
            self.cross_moving_line(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)
            # 3. *일 최고가 갱신여부?
            self.cross_highest_price(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria, percent=percent)

            # OLAP. 전날에 비해 가격상승이 있었는가? --> 실제 사용하기 위해선 shift(1) 해서 비교할 것.
            self.compare_today_yesterday_price(df_day=self.read_df_dayinfo)

            self.saved_df = self.saved_df.reset_index()
            '''
            except KeyError:
                continue
            '''
            processing_frame = self.saved_df[self.saved_df["날짜"] > analys_last_date]
            # DataFrame --> Dictionary (열 이름 겹침 워닝은 무시하도록 설정)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")  # 워닝 무시
                df_to_dict = processing_frame.to_dict(orient="records")
            print(df_to_dict)
            self.mongo.insert_list("DayInfo", "Analys", [company, ticker], df_to_dict,
                                   primaryKey=ticker, primaryKeySet=True, client=self.mongo.client2)
            #self.mongo.insert("DayInfo", "Analys", df_to_dict, primaryKeySet=False, client=self.mongo.client2)
            #print(processing_frame.tail(5))



    ''' B. 계산 메소드들 
        1. 60일 이동평균선 추이가 up(상행) 인가 bottom (하방) 인가?
            > 전체적인 주식지표가 상승지표인가? 판단
        2. 전환선, 기준선이 서로 근접해있는가?
            > 일목균형표가 지표들에 근접할 수록 주식의 분위기가 바뀌는 것으로 가정 
        3. 평균 이동선(SMA)을 통과했는가?
            > 일반적인 주식 분위기 흐름을 판단하는 지표 
                (모든 평균 이동선을 통과하면 Golden Cross)
        4. MACD (Moving Average Convergence and Divergence)
            > 화폐에서 주로사용하는 지표로, 주식장이 대기업같이 안정적일 때 주식의 상, 하방을 판단하기 좋음
            >> 주식에서는 주로 MACD Signal 이라는 지표를 추가로 사용함. 
            MACD Signal 은 MACD 의 9일 이동평균선으로 MACD의 흐름을 파악하는 라인임
            위의 평균이동선이 Golden Cross 되는 지점과 비슷하게 MACD Signal 을 MACD가 가로질러 상방하는 경우
            좋은 지표로 주로 판단할 수 있음.
            >>> MACD Histogram = MACD - MACD_Signal 을 의미
            
        5. 현재가가 X일 최고가를 갱신했는가?
            > 주식의 전반적인 흐름이 X일 이전보다 상승했음으로 해석될 수 있음
        
        6. 후행스팬이 기준선, 전환선을 가로질러갈 때
            > 후행스팬은 현재가에서 26일을 뒤로 보냈을 때 과거값과 비교하는 방식이다.
                후행스팬이 봉(기준선, 전환선) 을 넘어갔다면 장기적으로 상방성이 높음을 의미.
                 
        7. 26일전 주식 종가가 후행스팬을 뚫었는가?
            > 일목균형표의 서로의 지표들이 교차되는 지점으로 보조지표로 분석예정.
        
        8. 현 주가의 스팬꼬리 (대략 3일전부터의 값) 이 양수방향인가 여부
            선행스팬은 과거주가를 기반으로 미래주가를 예측하는 선이다.
            
            >> 선행스팬1은 당일전환선과 당일 기준선의 평균을 26일 앞으로 이동 
            >> 선행스팬2은 52일 간 최고/최저 중앙 값을 26일 앞으로 이동
            즉, 하여 선행스팬1이 선행스팬2보다 위에 있는 상승폭을 지녔을 경우 상승력이 강함을 의미.
            
        9. 현 주가의 선행스팬위치가 상대적으로 어떻게 되어있는가?
            >> 스팬은 26일을 뒤로 보내기 때문에 과거에비해 주가가 올라가는 힘이 강한가 판단여부로 활용
        
        10. 전환선이 기준선보다 위에있는가?
            > 전환선이 (9일간 최고가) 기준선 (26일간의 최고가) 더 자주 움직이는 점을 활용하여 주식 변동성이 상방인지 체크
         
        11. 현재위치의 종가가 기준,전환선을 뚫으려고 하는가 + (기준,전환선이 근접한가)
            > 해당위치에 심한 변동성이 존재하였으며 과거에 비해 해당 위치를 뚫으면 
            주가흐름이 바뀌어 올라갈 수 있음을 추측.
            
        >>> +++ +++ <<<
        파이썬 shift는 DataFrame을 당겨주는 메소드이다.
        a.shift(1) 을 한다면 a데이터프레임을 앞으로 한 칸 당긴다. (즉, 과거의 값이 앞으로 한 칸 온다.)
            그러므로, a > a.shift(1) 을 한다면 오늘값이 어제값보다 크냐로 해석할 수 있다. (헷갈리지 말것)
        >>> +++ +++ <<<
        
        _ Junhyeong (20190511) .. 03.10
        '''

    def compare_today_yesterday_price(self, df_day: pd.DataFrame):
        naming = self.anal_namedict["어제기준_가격비교"]
        self.saved_df.loc[(df_day["종가"] > df_day["종가"].shift(1)), naming] = "상승"
        self.saved_df.loc[(df_day["종가"] < df_day["종가"].shift(1)), naming] = "하락"
        self.saved_df.loc[(df_day["종가"] == df_day["종가"].shift(1)), naming] = "유지"

    def sma60_direction(self, df_crit: pd.DataFrame, compare=[1, 3, 5, 9, 26]):
        naming = self.anal_namedict["SMA60_check"]
        self.saved_df[naming] = None

        mask = True
        for c in compare:
            mask &= (df_crit["SMA60"] > df_crit["SMA60"].shift(c))
        self.saved_df.loc[mask, naming] = "up"

        mask = False
        for c in compare:
            mask &= (df_crit["SMA60"] < df_crit["SMA60"].shift(c))
        self.saved_df.loc[mask, naming] = "down"

    # 0-1. 전환/기준선이 붙어있는가..
    def near_line_check(self, df_crit: pd.DataFrame, percent=2):
        naming2 = self.anal_namedict["전기_nearess_check"]
        naming = self.anal_namedict["전기_nearess_check(후행)"]
        self.saved_df[naming] = None

        mask = (df_crit[["conversion line", "base line"]].max(axis=1) *
                (1 - 0.01 * percent) < df_crit[["conversion line", "base line"]].min(axis=1))
        self.saved_df.loc[mask, naming] = "O"

        mask = (df_crit[["conversion line", "base line"]].max(axis=1) *
                (1 - 0.01 * percent) > df_crit[["conversion line", "base line"]].min(axis=1))
        self.saved_df.loc[mask, naming] = "X"

        self.saved_df[naming2] = self.saved_df[naming].shift(25)

    # 1. 평균이동선을 통과했는가?
    def cross_moving_line(self, df_day: pd.DataFrame, df_crit: pd.DataFrame):
        for sma in self.sma_window:
            sma_name = "SMA" + str(sma)
            d_key = sma_name + "_cross_check"
            naming = self.anal_namedict[d_key]
            self.saved_df[naming] = None

            mask = (df_day["종가"] >= df_crit[sma_name])
            self.saved_df.loc[mask, naming] = "O"

            mask = (df_day["종가"] < df_crit[sma_name])
            self.saved_df.loc[mask, naming] = "X"


    # 2. MACD 추이 (Up,Down_Cross)
    def check_macd(self, df_crit: pd.DataFrame(), low=1000):
        naming = self.anal_namedict["MACD_check"]
        # MACD 교차점을 상승하는지점 (매수지점 추천)
        self.saved_df[naming] = "X"

        mask = (-low <= df_crit["MACD_Histogram"])  # MACD 교차점이 -1000 ~ 1000 사이인가?
        mask &= (df_crit["MACD_Histogram"] > df_crit["MACD_Histogram"].shift(1))  # MACD 교차점이 5xPecrent 이하인가?
        mask &= (df_crit["MACD"] > 0)  # MACD 값이 0보다 큰가?
        self.saved_df.loc[mask & (df_crit["MACD_Histogram"] > 0), naming] = "up"
        self.saved_df.loc[mask & (df_crit["MACD_Histogram"] < 0), naming] = "up_near"
        self.saved_df.loc[
            mask & ((df_crit["MACD_Histogram"].shift(1) < 0) & (df_crit["MACD_Histogram"] > 0)), naming] = "up_cross"

        mask2 = (low > df_crit["MACD_Histogram"])  # MACD 교차점이 5xPecrent 이하인가?
        mask2 &= (df_crit["MACD_Histogram"] < df_crit["MACD_Histogram"].shift(1))  # MACD 교차점의 하강중인가?
        self.saved_df.loc[mask2 & (df_crit["MACD_Histogram"] < 0), naming] = "down"  # MACD 값이 0보다 큰가?
        self.saved_df.loc[mask2 & (df_crit["MACD_Histogram"] > 0), naming] = "down_near"
        self.saved_df.loc[
            mask2 & ((df_crit["MACD_Histogram"].shift(1) > 0) & (df_crit["MACD_Histogram"] < 0)), naming] = "down_cross"

        # MACD 교차점을 하강하는지점 (매도지점 추천)

    # 3. &일 최고가를 갱신했는가?
    def cross_highest_price(self, df_day: pd.DataFrame, df_crit: pd.DataFrame, percent=2):
        ''' 최고가 기준을 Cross 했는가? (근접했으면 near, 같거나 띄어넘었으면 up, 크로스했으면 up_cross 아니면 X)
        :param df_day: 일봉 데이터프레임
        :param df_crit: 기준표 데이터프레임
        :param percent: 퍼센트 기준
        :return:
        '''
        for hi in self.high_crit:
            d_key = str(hi) + "_highest_check"
            df_key = str(hi) + "th Highest Price"
            naming = self.anal_namedict[d_key]
            self.saved_df[naming] = "X"
            mask = (df_day["종가"] >= df_crit[df_key] * (1 - 0.01 * percent))
            self.saved_df.loc[mask & (df_day["종가"] < df_crit[df_key]), naming] = "up_near"
            self.saved_df.loc[mask & (df_day["종가"] >= df_crit[df_key]), naming] = "up"
            self.saved_df.loc[mask & (df_day["종가"].shift(1) < df_crit[df_key].shift(1)) & \
                              (df_day["종가"] >= df_crit[df_key]), naming] = "up_cross"

    # 4-01. 후행스팬이 기준선,전환선을 Cross할 때
    def cross_backspan_line(self, df_crit: pd.DataFrame, percent=2):
        naming = self.anal_namedict["후행스팬_line_cross_check"]
        naming2 = self.anal_namedict["전기_nearess_check(후행)"]
        self.saved_df[naming] = "X"
        mask = (df_crit["Lagging Span"] >= df_crit[["conversion line", "base line"]].min(axis=1) * (1 - 0.01 * percent))
        mask &= (self.saved_df[naming2] == "O")
        self.saved_df.loc[
            mask & (df_crit["Lagging Span"] < df_crit[["conversion line", "base line"]].min(axis=1)),
            naming
        ] = "up_near"
        self.saved_df.loc[
            mask & (df_crit["Lagging Span"] >= df_crit[["conversion line", "base line"]].min(axis=1)),
            naming
        ] = "up"
        self.saved_df.loc[
            mask & (df_crit["Lagging Span"] >= df_crit[["conversion line", "base line"]].min(axis=1)) & (
                    df_crit["Lagging Span"].shift(1) < df_crit[["conversion line", "base line"]].min(axis=1).shift(1)),
            naming
        ] = "up_cross"

        mask2 = (df_crit["Lagging Span"] < df_crit[["conversion line", "base line"]].max(axis=1) * (1 - 0.01 * percent))
        self.saved_df.loc[
            mask2 & (df_crit["Lagging Span"] > df_crit[["conversion line", "base line"]].max(axis=1)),
            naming
        ] = "down_near"
        self.saved_df.loc[
            mask2 & (df_crit["Lagging Span"] < df_crit[["conversion line", "base line"]].max(axis=1)),
            naming
        ] = "down"
        self.saved_df.loc[
            mask2 & (df_crit["Lagging Span"] <= df_crit[["conversion line", "base line"]].max(axis=1)) & (
                    df_crit["Lagging Span"].shift(1) > df_crit[["conversion line", "base line"]].max(axis=1).shift(1)),
            naming
        ] = "down_cross"
        self.saved_df[naming] = self.saved_df[naming].shift(25)

    # 4-1. 26일전 주식 종가가 후행스팬을 뚫었나?
    def cross_backspan(self, df_day: pd.DataFrame, df_crit: pd.DataFrame, percent=2):

        check_naming = self.anal_namedict["후행스팬_bong_cross_check"]
        self.saved_df[check_naming] = "X"
        mask = (df_day["종가"] >= df_crit["Lagging Span"] * (1 - 0.01 * percent))
        self.saved_df.loc[
            mask & (df_day["종가"] < df_crit["Lagging Span"]),
            check_naming
        ] = "up_near"
        self.saved_df.loc[
            mask & (df_day["종가"] >= df_crit["Lagging Span"]),
            check_naming
        ] = "up"
        self.saved_df.loc[
            mask & (df_day["종가"] > df_crit["Lagging Span"]) & (df_day["종가"].shift(1) < df_crit["Lagging Span"].shift(1)),
            check_naming
        ] = "up_cross"

        mask2 = (df_day["종가"] <= df_crit["Lagging Span"] * (1 - 0.01 * percent))
        self.saved_df.loc[
            mask2 & (df_day["종가"] > df_crit["Lagging Span"]),
            check_naming
        ] = "down_near"
        self.saved_df.loc[
            mask2 & (df_day["종가"] <= df_crit["Lagging Span"]),
            check_naming
        ] = "down"
        self.saved_df.loc[
            mask2 & (df_day["종가"] < df_crit["Lagging Span"]) &
            (df_day["종가"].shift(1) > df_crit["Lagging Span"].shift(1)),
            check_naming
        ] = "down_cross"
        self.saved_df[check_naming] = self.saved_df[check_naming].shift(25)

    # 4-2. 현 주가의 스팬 꼬리(3일전부터 모두 상승?) 가 양의 방향인가? + 스팬이 양수인가?
    def check_spantail(self, df_crit: pd.DataFrame):
        naming = self.anal_namedict["스팬꼬리_check"]
        self.saved_df[naming] = None

        mask = (df_crit["Future Leading Span A"] > df_crit["Future Leading Span B"])
        mask &= (df_crit["Future Leading Span A"] > df_crit["Future Leading Span A"].shift(1))
        mask &= (df_crit["Future Leading Span A"].shift(1) > df_crit["Future Leading Span A"].shift(2))
        self.saved_df.loc[mask, naming] = "O"

        mask = (df_crit["Future Leading Span A"] < df_crit["Future Leading Span B"])
        mask |= (df_crit["Future Leading Span A"] < df_crit["Future Leading Span A"].shift(1))
        mask |= (df_crit["Future Leading Span A"].shift(1) < df_crit["Future Leading Span A"].shift(2))
        self.saved_df.loc[mask, naming] = "X"


    # 4-3. 현 주가의 스팬 위치 (up, mid, bot)
    def check_span_position(self, df_day: pd.DataFrame, df_crit: pd.DataFrame):
        naming = self.anal_namedict["스팬위치_check"]
        self.saved_df[naming] = "mid"

        self.saved_df.loc[(df_day["종가"] <= df_crit[["Leading Span A", "Leading Span B"]].min(axis=1)), naming] = "down"
        self.saved_df.loc[(df_day["종가"] >= df_crit[["Leading Span A", "Leading Span B"]].max(axis=1)), naming] = "up"

    # 5. 전환선 >= 기준선 (통과전, 통과, 통과 후)
    def span_line_cross(self, df_crit: pd.DataFrame, percent=2):
        naming = self.anal_namedict["전_cross_기"]
        self.saved_df[naming] = "X"
        mask = (df_crit["conversion line"] >= df_crit["base line"] * (1 - 0.01 * percent))
        self.saved_df.loc[mask & (df_crit["conversion line"] < df_crit["base line"]), naming] = "up_near"
        self.saved_df.loc[mask & (df_crit["conversion line"] >= df_crit["base line"]), naming] = "up"
        self.saved_df.loc[mask & (df_crit["conversion line"].shift(26) < df_crit["base line"].shift(26)) & \
                          (df_crit["conversion line"].shift(25) >= df_crit["base line"].shift(25)), naming] = "up_cross"

        mask2 = (df_crit["conversion line"] < df_crit["base line"] * (1 - 0.01 * percent))
        self.saved_df.loc[mask2 & (df_crit["conversion line"] > df_crit["base line"]), naming] = "down_near"
        self.saved_df.loc[mask2 & (df_crit["conversion line"] <= df_crit["base line"]), naming] = "down"
        self.saved_df.loc[mask2 & (df_crit["conversion line"].shift(26) > df_crit["base line"].shift(26)) & \
                          (df_crit["conversion line"].shift(25) <= df_crit["base line"].shift(25)), naming] = "down_cross"

    # 6. 봉이 기준선과 전환선을 뚫고 갈려고 하는가? (+ 기준선+전환선이 붙어있어야함)
    def bong_cross_line(self, df_day: pd.DataFrame, df_crit: pd.DataFrame, percent=2):
        naming = self.anal_namedict["봉_cross_전기"]
        naming2 = self.anal_namedict["전기_nearess_check"]
        self.saved_df[naming] = "X"
        mask = (df_day["종가"] >= df_crit[["conversion line", "base line"]].min(axis=1) * (1 - 0.01 * percent))
        mask &= (self.saved_df[naming2] == "O")
        self.saved_df.loc[
            mask & (df_day["종가"] < df_crit[["conversion line", "base line"]].min(axis=1)),
            naming
        ] = "up_near"
        self.saved_df.loc[
            mask & (df_day["종가"] >= df_crit[["conversion line", "base line"]].min(axis=1)),
            naming
        ] = "up"
        self.saved_df.loc[
            mask & (df_day["종가"] >= df_crit[["conversion line", "base line"]].min(axis=1)) & (
                    df_day["종가"].shift(1) < df_crit[["conversion line", "base line"]].min(axis=1).shift(1)),
            naming
        ] = "up_cross"

        mask2 = (df_day["종가"] < df_crit[["conversion line", "base line"]].max(axis=1) * (1 - 0.01 * percent))
        self.saved_df.loc[
            mask2 & (df_day["종가"] > df_crit[["conversion line", "base line"]].max(axis=1)),
            naming
        ] = "down_near"
        self.saved_df.loc[
            mask2 & (df_day["종가"] < df_crit[["conversion line", "base line"]].max(axis=1)),
            naming
        ] = "down"
        self.saved_df.loc[
            mask2 & (df_day["종가"] <= df_crit[["conversion line", "base line"]].max(axis=1)) & (
                    df_day["종가"].shift(1) > df_crit[["conversion line", "base line"]].max(axis=1).shift(1)),
            naming
        ] = "down_cross"

    ''' B. Reading Moudles '''
    def readAnalySQL(self, name: str):
        # 회사가 DB에 없을 경우 빈 DataFrame 리턴
        try:
            query = {
                "$or": [
                    {"회사명": name},
                    {"티커": name}
                ]
            }
            findingSQL = self.mongo.read_list_obj("DayInfo", "Analys", query=query, client=self.mongo.client2)
            if findingSQL:
                return pd.DataFrame(findingSQL["data"]).set_index("날짜")
        except Exception:
            print(f"{name} 는 invalid 데이터 입니다.")
            return pd.DataFrame()

if __name__ == "__main__":
    obj = StockAnaly()
    obj.module(day_info=False, compute_criteria=False)
    #print(obj.anal_namedict)