# 확률 구하는 파일
import MongoDriver
import GetAnaly
import GetInfo
import pandas as pd
import datetime
import warnings

class StockProbs:
    def __init__(self):
        self.mongo = MongoDriver.MongoDB()
        self.info_obj = GetInfo.StockKr()
        self.analy_obj = GetAnaly.StockAnaly()
        self.analy_obj.analdict_update()
        self.name_dict = self.analy_obj.anal_namedict
        self.name_dict_r = self.analy_obj.anal_namedict_r

    def scoring_module(self):
        self.info_obj.module_readTr(update=False)
        print("Start Calc Probabilty")
        for company, ticker in self.info_obj.thema_total_dict.items():
            probs_last_date = datetime.datetime(1999, 1, 1)
            print("[" + company + " 확률 분석 중 ...]")
            df = pd.DataFrame()
            try:
                last_date_info = self.mongo.read_last_date("DayInfo", "Info", {"티커": ticker})
                last_date_analys = self.mongo.read_last_date("DayInfo", "Probs", {"티커": ticker},
                                                             client=self.mongo.client2)  # 임시방편
                if last_date_info and last_date_analys:
                    difference = last_date_info["날짜"] - last_date_analys["날짜"]
                    if difference.days == 0:
                        continue
                    before = self.mongo.read_date_limits("DayInfo", "Probs", {"티커": ticker},
                                                              limits=difference.days + 120, client=self.mongo.client2)
                    df_analys = pd.DataFrame(before).set_index("날짜")
                    if not df_analys.empty:
                        df_analys = df_analys.iloc[::-1]
                    else:
                        df_analys = self.analy_obj.readAnalySQL(company)

                    probs_last_date = last_date_analys["날짜"]
                    df = df_analys
                else:
                    if not last_date_info:
                        continue
                    df = self.analy_obj.readAnalySQL(company)

            except Exception:
                df = self.analy_obj.readAnalySQL(company)

            df = self.analy_obj.readAnalySQL(company)
            return_df = self.scoring_each(company, df)

            return_df = return_df.reset_index()
            processing_frame = return_df[return_df["날짜"] > probs_last_date]
            # DataFrame --> Dictionary (열 이름 겹침 워닝은 무시하도록 설정)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")  # 워닝 무시
                df_to_dict = processing_frame.to_dict(orient="records")

            print(df_to_dict)
            self.mongo.insert_list("DayInfo", "Probs", [company, ticker], df_to_dict,
                                   primaryKey=ticker, primaryKeySet=True, client=self.mongo.client2)

    def scoring_each(self, company, df):
        saved_df = pd.DataFrame()
        try:
            self.sma60_direction(df, saved_df)
            self.near_line_check(df, saved_df)
            self.check_macd(df, saved_df)
            self.cross_backspan_line(df, saved_df)
            self.cross_backspan(df, saved_df)
            self.check_spantail(df, saved_df)
            self.check_span_position(df, saved_df)
            self.span_line_cross(df, saved_df)
            self.bong_cross_line(df, saved_df)
            self.cross_moving_line(df, saved_df)
            self.cross_highest_price(df, saved_df)
        except Exception:
            pass
        return saved_df


    def sma60_direction(self, df, saved_df):
        name = self.name_dict["SMA60_check"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] == "up") | (df[name] == "down")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락")))

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def near_line_check(self, df, saved_df):
        name_dict = [self.name_dict["전기_nearess_check"], self.name_dict["전기_nearess_check(후행)"]]
        name2 = self.name_dict["어제기준_가격비교"]

        for name in name_dict:
            tmp_df = pd.DataFrame()
            tmp_df["total_cnt"] = ((df[name] == "O") | (df[name] == "X")).cumsum()
            tmp_df["cnt"] = 0

            mask = (((df[name].shift(1) == "O") & (df[name2] == "상승")) |
                    ((df[name].shift(1) == "X") & (df[name2] == "하락")))

            tmp_df.loc[mask, "cnt"] = 1
            tmp_df["prob"] = 0
            tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
            saved_df[name] = tmp_df["prob"]

    def check_macd(self, df, saved_df):
        name = self.name_dict["MACD_check"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] != "X")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_near") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_cross") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down_cross") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down_near") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def cross_backspan_line(self, df, saved_df):
        name = self.name_dict["후행스팬_line_cross_check"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] != "X")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_near") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_cross") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down_cross") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down_near") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def cross_backspan(self, df, saved_df):
        name = self.name_dict["후행스팬_bong_cross_check"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] != "X")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_near") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_cross") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down_cross") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down_near") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def check_spantail(self, df, saved_df):
        name = self.name_dict["스팬꼬리_check"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] == "O") | (df[name] == "X")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "O") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "X") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def check_span_position(self, df, saved_df):
        name = self.name_dict["스팬위치_check"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] == "down") | (df[name] == "up")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def span_line_cross(self, df, saved_df):
        name = self.name_dict["전_cross_기"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] != "X")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_near") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_cross") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down_cross") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down_near") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def bong_cross_line(self, df, saved_df):
        name = self.name_dict["봉_cross_전기"]
        name2 = self.name_dict["어제기준_가격비교"]

        tmp_df = pd.DataFrame()
        tmp_df["total_cnt"] = ((df[name] != "X")).cumsum()
        tmp_df["cnt"] = 0

        mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_near") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "up_cross") & (df[name2] == "상승")) |
                ((df[name].shift(1) == "down_cross") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down_near") & (df[name2] == "하락")) |
                ((df[name].shift(1) == "down") & (df[name2] == "하락"))
                )

        tmp_df.loc[mask, "cnt"] = 1
        tmp_df["prob"] = 0
        tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
        saved_df[name] = tmp_df["prob"]

    def cross_moving_line(self, df, saved_df):
        for sma in self.analy_obj.sma_window:
            sma_name = "SMA" + str(sma)
            d_key = sma_name + "_cross_check"
            name = self.name_dict[d_key]
            name2 = self.name_dict["어제기준_가격비교"]

            tmp_df = pd.DataFrame()
            tmp_df["total_cnt"] = ((df[name] == "O") | (df[name] == "X")).cumsum()
            tmp_df["cnt"] = 0

            mask = (((df[name].shift(1) == "O") & (df[name2] == "상승")) |
                    ((df[name].shift(1) == "X") & (df[name2] == "하락"))
                    )

            tmp_df.loc[mask, "cnt"] = 1
            tmp_df["prob"] = 0
            tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
            saved_df[name] = tmp_df["prob"]
    def cross_highest_price(self, df, saved_df):
        for hi in self.analy_obj.high_crit:
            d_key = str(hi) + "_highest_check"
            name = self.name_dict[d_key]
            name2 = self.name_dict["어제기준_가격비교"]

            tmp_df = pd.DataFrame()
            tmp_df["total_cnt"] = ((df[name] != "X")).cumsum()
            tmp_df["cnt"] = 0

            mask = (((df[name].shift(1) == "up") & (df[name2] == "상승")) |
                    ((df[name].shift(1) == "up_near") & (df[name2] == "상승")) |
                    ((df[name].shift(1) == "up_cross") & (df[name2] == "상승"))
                    )

            tmp_df.loc[mask, "cnt"] = 1
            tmp_df["prob"] = 0
            tmp_df["prob"] = round(tmp_df["cnt"].cumsum() / (tmp_df["total_cnt"]), 4)
            saved_df[name] = tmp_df["prob"]

if __name__ == "__main__":
    obj = StockProbs()
    obj.scoring_module()

