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

"""

@:keyword
股票二端训练程序，新版，目前线上部署的也是新版，这个版本。

"""

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




def trainnow(id,table):


    print("ok")
    myquery = {"code": id}

    cursor = table.find(myquery, {'_id': 0})

    df = pd.DataFrame(list(cursor))

    df.sort_values("date", inplace=True)

    y = df["today_updown"].values

    df.drop(["name", "date", "time", "today_updown", "code"], axis=1, inplace=True)
    df.fillna(value=0, inplace=True)

    col = list(df.columns)

    # 把所有列的类型都转化为数值型，出错的地方填入NaN，再把NaN的地方补0
    df[col] = df[col].apply(pd.to_numeric, errors='coerce').fillna(0.0)

    df = pd.DataFrame(df, dtype='float')



    print(df.dtypes)


    x = np.asarray(df)



    num = len(x)
    splits = int(num * 0.8)
    x_train = x[:splits]
    x_test = x[splits:]
    y_train = y[:splits]
    y_test = y[splits:]

    model = neighbors.KNeighborsClassifier()

    print("我已经开始训练".format(threading.current_thread()))

    mr = model.fit(x_train, y_train)

    joblib.dump(mr, "./model/{}.pkl".format(id))

    score = model.score(x_test, y_test)

    result = model.predict(x_test)
    # print(model.feature_importances_, "我是因子重要性")
    acc = accuracy_score(y_test, result)
    # print(score)
    # logger.info("我是{0}acc得分{1}".format(id, acc))
    print(acc)




def train():
    pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    ts_codes = data["symbol"].values




    def work(ts_codes,table):
        print('================================线程')

        for id in ts_codes:
            try:
                trainnow(id,table)

            except Exception as e:
                print(e)
                logger.error(e)
    i = 0
    s = 0



    while i < len(ts_codes):
        conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
        db = conn['HDB3']
        table = db["hdata{0}".format(s)]
        s += 1
        if i + 190 > len(ts_codes):
            ts_code_list = ts_codes[i:len(ts_codes) - 1]
        else:
            ts_code_list = ts_codes[i:i + 190]

        t = threading.Thread(target=work, args=[ts_code_list,table])
        t.start()
        i += 190


if __name__ ==  '__main__':

    while 1:
        # 每隔6天更新一次模型
        train()
        time.sleep(518400)
        # train()






