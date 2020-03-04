import pymongo
from concurrent.futures import ThreadPoolExecutor
from sklearn.externals import joblib
import logging
import time,datetime
import pandas as pd
import numpy as np
from scipy import stats
import threading

"""
预测器，主要是用来预测，目前线上就是这个版本。
"""


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("predict.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def perdict(id:str):
    """
    预测数据，100个线程，100个股票。
    :param id: 股票代码
    :param file: 存储路径
    :return:
    """
    print("开始执行预测")
    model = joblib.load("./model/{}.pkl".format(id))


    conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
    db = conn['HDB3']
    date = datetime.datetime.now().strftime("%Y-%m-%d")


    myquery = {"code": id, "date": date}
    for name in db.list_collection_names():

        mydoc = db[name].find_one(myquery)
        if mydoc:
            cursor = db[name].find(myquery, {'_id': 0})

            df = pd.DataFrame(list(cursor))
            df.sort_values("date", inplace=True)




            df.drop(["name", "date", "time", "today_updown", "code"], axis=1, inplace=True)
            df.fillna(value=0, axis=1, inplace=True)

            x = np.asarray(df)

            result = model.predict(x)

            y = stats.mode(result)[0][0]

            dic = {"predict_mode": y}
            values = {"$set": dic}

            myquery = {"code": id, "date": date}

            db[name].update_many(myquery, values)



            print("已经找到，执行预测")
            break




def getYesterday():
    """
    获取昨天的日期
    :return:
    """
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    s = ""
    for i in str(yesterday).split("-"):
        s = s + i

    print(s)
    return s



def main():
    """
    :param

    :return:
    """

    #拿到100个股票代码，给到100个线程里面去预测。
    #查出1端的100个股票，然后做成列表。

    print('开始查询昨天的预测数据')

    conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
    db = conn['HDB1']
    table = db['Hdata']
    # pre_day = getYesterday()
    pre_day = "20200228"
    myquery = {"trade_date_x": pre_day}
    # 升序
    x = table.find(myquery, {'_id': 0, 'ts_code': 1, 'result_1': 1}).sort("result_1", pymongo.ASCENDING)
    df = pd.DataFrame(list(x))

    predict_y = []
    result_1 = df[-100:]['result_1'].values
    print(result_1)
    for i in result_1:
        if i >= 0.5:
            predict_y.append(1)
        else:
            predict_y.append(0)

    seed = df[-100:]['ts_code'].values

    print(seed)

    ids= []
    for i in seed:
        print(type(i))
        id = i.split(".")[0]
        ids.append(id)
    print(ids)
    #这里同时启动多个线程，等于同时跑多个函数，并把前面准备的参数，按照顺序传入进去。
    for id in ids:
        t = threading.Thread(target=perdict ,args=[id,])
        t.start()














if __name__ == "__main__":
    # while 1 :
    #     time.sleep(2)
        # if (datetime.datetime.now().strftime("%H:%M")) == "12:34":
        #     try:

    main()
            # except Exception as e:
            #     logging.error("我是错误{0}".format(e))





