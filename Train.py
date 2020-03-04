import tushare as ts
import pandas as pd
import numpy as np
from sklearn.externals import joblib
from sklearn.metrics import accuracy_score
from sklearn import neighbors
import pymongo
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
import logging
import time,datetime
import threading



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




def train(id:str, table):
    """
    下载数据，主要放在线程里面跑，可以理解成每个线程单独跑一个
    :param id: 股票代码
    :param table: mongo表格
    :return:
    """
    print("ok")
    myquery = {"code": id}

    cursor = table.find(myquery, {'_id': 0})

    df = pd.DataFrame(list(cursor))

    df.sort_values("date", inplace=True)

    y = df["today_updown"].values

    df.drop(["name", "date", "time", "today_updown", "code"], axis=1, inplace=True)
    df.fillna(value=0, axis=1, inplace=True)

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
    logger.info("我是{0}acc得分{1}".format(id, acc))
    print(acc)


def main(seed: list, tab):
    """

    :param seed: 一个个的股票代码划分列表，
    abc:根据股票代码，制作出文件名
    :return:
    """

    conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
    db = conn['HDB3']
    table = db["hdata{0}".format(tab)]
    abc = [table for i in seed]

    # print(abc)
    # 这里同时启动多个线程，等于同时跑多个函数，并把前面准备的参数，按照顺序传入进去。
    with ThreadPoolExecutor(190) as executor1:
        # print(222222)

        executor1.map(train, seed, abc)  # 提交任务给进程池。这里始终用相同的进程再跑，注意看循环的位置。



def split_stockid(stocks: list, p_num=None, t_num=190):
    """
    对股票按照进程和线程数进行等分，默认是10*30
    :param stocks:
    :param t_num: 这里是线程数据，根据你的线程数进行切片。 注意
    :param p_num:
    :return:
    """
    try:
        seed = []

        temp_list = []

        for i in range(len(stocks)):

            if i % t_num == 0:
                temp_list.append(i)

        for x in range(len(temp_list) - 1):
            print((temp_list[x]), (temp_list[x + 1]))

            temp = stocks[temp_list[x]:temp_list[x + 1]]
            seed.append(temp)
        seed.append(stocks[3610:])
        return seed


    except Exception as e:
        print(e)

def all_stocks():
    """
    #查询当前所有正常上市交易的股票列表
    :return:
    """

    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    return data["symbol"].values


# def run():
#     stocks = all_stocks()
#     # stocks = stocks[1200:1501]
#     seed = split_stockid(stocks)
#     print(seed)
#
#     table = [i for i in range(20)]
#     with ProcessPoolExecutor(20) as executor1:
#
#         executor1.map(main, seed, table)


if __name__ == "__main__":
    while 1 :
        time.sleep(2)
        if (datetime.datetime.now().strftime("%H:%M")) == "17:54":
            try:

                run()
            except Exception as e:
                logging.error("我是错误{0}".format(e))





