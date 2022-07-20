import tushare as ts
import pandas as pd
import numpy as np
from sklearn.externals import joblib
from sklearn.metrics import accuracy_score,r2_score
from sklearn import neighbors,ensemble,svm
import threading
import matplotlib.pyplot as plt
import pymongo
import json
import datetime
import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("train")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)


"""
新版本，下载器。
当时当时以为第一版本不好使，但是目前来看是好使的，可以完整的完成工作。所以就不用新版本了，新版本就先这么放着把
"""

def download_10tick(id:str,table):
    """
    下载数据，主要放在线程里面跑，可以理解成每个线程单独跑一个
    :param id: 股票代码
    :param file: 存储路径
    :return:
    """

    df: pd.DataFrame

    df = ts.get_realtime_quotes(id)  # Single stock symbol

    data_dict = json.loads(df.to_json(orient='records'))
    table.insert(data_dict)


    # if (datetime.datetime.now().strftime("%H:%M")) == "01:40":
    #
    #     break


def train():
    pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    ts_codes = data["symbol"].values



    def work(ts_codes,table):
        print('================================线程{0}'.format(i))
        while 1:
            for id in ts_codes:
                try:
                    download_10tick(id,table)

                except Exception as e:
                    print(e)
                    logger.error(e)
            if (datetime.datetime.now().strftime("%H:%M")) == "12:34":
                break
    i = 0
    s = 0



    while i < len(ts_codes):
        conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
        db = conn['HDB3']
        table = db["hdata{0}".format(s)]

        if i % 190 == 0:
            s += 1
        if i + 19 > len(ts_codes):
            ts_code_list = ts_codes[i:len(ts_codes) - 1]
        else:
            ts_code_list = ts_codes[i:i + 19]

        t = threading.Thread(target=work, args=[ts_code_list,table])

        t.start()
        i += 19

#测试服务其线程极限是533

if __name__ ==  '__main__':



    train()



