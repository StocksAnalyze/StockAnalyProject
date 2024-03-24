import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from typing import (TypeVar, Optional)
import bson
import pymongo

ID_PASSWD_FILE = "password"
class MongoDB:
    def __init__(self):
        self.uri = ""
        self.uri2 = ""
        self.get_url()
        self.err = False
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            self.client2 = MongoClient(self.uri2, server_api=ServerApi("1"))
        except pymongo.errors.ConnectionFailure:
            self.err = True

        #MongDB Database Logical Set
        self.dbDict = {
            "StockCode": {"KOSDAQ", "KOSPI", "HIGHVOLUMN"},
            "DayInfo": "상장회사 티커",
            "DayCals": "상장회사 티커",
            "DayAnalys": "상장회사 티커"
        }

    def get_url(self):
        with open(ID_PASSWD_FILE, "r", encoding="UTF-8") as f:
            while True:
                line = f.readline().replace("\n","")
                if not line:
                    break
                id_passwd = line.split(":",1)
                self.uri = f"mongodb+srv://{id_passwd[0]}:{id_passwd[1]}@stocksql.bn49sg9.mongodb.net/?retryWrites=true&w=majority&appName=StockSQL"
                self.uri2 = f"mongodb+srv://{id_passwd[0]}:{id_passwd[1]}@cluster0.dt997xz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"



    def insert(self, dbName:"str", tableName:"str", queryList: list, primaryKey="", primaryKeySet=False, client = None):
        ''' MongoDB의 dbName.tableName 에 insert를 하는 메소드
         _ Junhyeong (20190511)
        :param dbName: Database Name
        :param tableName: table Name
        :param queryList: query List
        :param primaryKey: PK 설정 시 index 이름
        :param primaryKeySet: PK 설정여부
        :return: 성공 시 True 에러시 False
        :return:
        '''
        if client is None:
            client = self.client
        db = client[dbName]
        collections = db[tableName]

        if primaryKeySet:
            for queryDict in queryList:
                queryDict["_id"] = queryDict[primaryKey]

        try:
            collections.insert_many(queryList)
        except pymongo.errors.BulkWriteError:
            return self.insert_listone(dbName, tableName, queryList, primaryKey, primaryKeySet)
        except TypeError:
            return False
        return True

    def insert_listone(self, dbName:"str", tableName:"str", queryList: list, primaryKey="", primaryKeySet=False, client=None):
        ''' Primary Key 충돌로 인하여 한 Query 씩 Insert 할 시 돌아가는 System Method
         _ Junhyeong (20190511)
        :param dbName: Database Name
        :param tableName: table Name
        :param queryList: query List
        :param primaryKey: PK 설정 시 index 이름
        :param primaryKeySet: PK 설정여부
        :return: 성공 시 True 에러시 False
        '''
        if client is None:
            client = self.client
        db = client[dbName]
        collections = db[tableName]

        if primaryKeySet:
            for queryDict in queryList:
                queryDict["_id"] = queryDict[primaryKey]

        #print(queryList)
        for item in queryList:
            try:
                collections.insert_one(item)
            except pymongo.errors.DuplicateKeyError:
                print(f"key:{item} 이미 존재 하므로 pass ...")
                continue
            except Exception:
                print(f"Key:{item} 알 수 없는 에러..")
                return False

        return True

    def insert_list(self, dbName:str, tableName:str, company:list, queryList: list, primaryKey="", primaryKeySet=False, client=None):
        ''' list 단위로 db에 집어 넣는
         _ Junhyeong (20190511) - 03.24
        :param dbName: Database Name
        :param tableName: table Name
        :param company: [회사명, Ticker]
        :param queryList: query List
        :param primaryKey: PK 설정 시 index 이름
        :param primaryKeySet: PK 설정여부
        :return: 성공 시 True 에러시 False
        '''
        if client is None:
            client = self.client
        db = client[dbName]
        collections = db[tableName]

        query = {
            "$or": [
                {"회사명": company[0]},
                {"티커": company[0]}
            ]
        }

        document = collections.find_one(query)
        if document:
            for ql in queryList:
                collections.update_many(query, {"$push": {"data": ql}})
        else:
            com, ticker = company
            make_query = {"회사명": com, "티커": ticker, "data": queryList}
            if primaryKeySet:
                make_query["_id"] = primaryKey
            collections.insert_one(make_query)



    def read(self, dbname: str, tablename: str, query={}, client=None) -> list:
        ''' MongoDB 에서 dbName.tablename 에 해당하는 모든 Record 를 dictionary List 형태로 반환
        _ Junhyeong (20190511)
        :param dbname: database name 
        :param tablename: table name
        :return: 해당 table의 딕셔너리가 들어있는 리스트
        '''
        if client is None:
            client = self.client
        db = client[dbname]
        collections = db[tablename]
        rawDict = collections.find(query)
        retDict = [{k: v for k, v in d.items() if k != "_id"} for d in rawDict]
        return retDict

    def read_first_one(self, dbname: str, tablename: str, idx="", query={}, limits=1, client=None) -> dict:
        ''' MongoDB 에서 dbName.tableName 에 해당 하는 가장 첫 번째 record 반환
         _ Junhyeong (20190511)
        :param dbname: Database Name
        :param tablename: Table Name
        :param idx: 정렬시킬 인덱스
        :parm query: 찾을 쿼리
        :return: 해당 하는 dictionary 값
        '''
        if idx == "":
            idx = "_id"

        if client is None:
            client = self.client
        db = client[dbname]
        collections = db[tablename]

        if limits == 1:
            return collections.find_one(query, sort=[(idx, pymongo.ASCENDING)])
        else:
            return collections.find(query, sort=[(idx, pymongo.ASCENDING)]).limit(limits)

    def read_last_one(self, dbname: str, tablename: str, idx="", query={}, limits=1, client=None) -> dict:
        ''' MongoDB 에서 dbName.tableName 에 해당 하는 가장 마지막 record 반환
         _ Junhyeong (20190511)
        :param dbname: Database Name
        :param tablename: Table Name
        :return: 해당하는 dictionary 값
        '''
        if idx == "":
            idx = "_id"
        if client is None:
            client = self.client
        db = client[dbname]
        collections = db[tablename]

        if limits == 1:
            return collections.find_one(query, sort=[(idx, pymongo.DESCENDING)])
        else:
            return collections.find(query, sort=[(idx, pymongo.DESCENDING)]).limit(limits)

    def read_list_obj(self, dbname: str, tablename: str, idx="", query={}, client=None) -> Optional[bson.typings._DocumentType]:
        if idx == "":
            idx = "_id"
        if client is None:
            client = self.client

        db = client[dbname]
        collections = db[tablename]
        return collections.find_one(query)

    def read_last_date(self, dbname: str, tablename: str, query: dict, client=None):
        if client is None:
            client = self.client
        db = client[dbname]
        collections = db[tablename]

        pipeline = [
            {"$match": query},
            {"$project": {"날짜": {"$max": "$data"}}}
        ]

        result = list(collections.aggregate(pipeline))
        if result:
            return result[0]["날짜"]
        else:
            return None

    def read_date_limits(self, dbname: str, tablename: str, query: dict, limits=120, client=None):
        if client is None:
            client = self.client
        db = client[dbname]
        collections = db[tablename]

        pipeline = [
            {"$match": query},
            {"$unwind": "$data"},
            {"$sort": {"data.날짜": -1}},
            {"$limit": limits}
        ]

        result = list(collections.aggregate(pipeline))
        return result["data"]

if __name__ == "__main__":

    # DB에 들어가는지 확인..
    test = [{"company":"APS", "code":"054620"}, {"company":"AP시스템", "code":"265520"}, {"company":"AP위성", "code":"211270"}, {"company":"3S", "code":"060310"}]
    obj = MongoDB()

    datas = obj.read("DayInfo", "Analys", {
        "티커": "207940"
    }, client=obj.client2 )

    for item in datas:
        print(item)
    #bj.insert("StockCode", "KOSDAQ", test, "code", primaryKeySet=True)
    #print(obj.read("StockCode", "KOSPI",  {"company": "동화약품"}))
