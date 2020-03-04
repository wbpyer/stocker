import tushare as ts
import datetime
import pymongo
import logging
import time
import threading

"""
并行版本数据更新，不需要无限循环，每天只需要跑一次,最新更改版本，目前线上版本 。
"""

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("y_append")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")

def download_y(id:str, table):
    """
    下载数据，主要放在线程里面跑，可以理解成每个线程单独跑一个
    :param id: 股票代码
    :param table: mongo里面的数据表
    :return:
    """


    date = datetime.datetime.now().strftime("%Y%m%d")
    # date = "20200228"

    ndate = datetime.datetime.now().strftime("%Y-%m-%d")
    # ndate = "2020-02-28"
    if id.startswith("0"):
        nid = id+".SZ"
    elif id.startswith("3"):
        nid = id +".SZ"
    else:
        nid =  id +".SH"

    df  = pro.daily(ts_code=nid, start_date=date, end_date=date)
    # df = ts.get_hist_data(id, start=date, end=date)



    y = df["pct_chg"].values

    if y >= 0.00:
        data = 1
    else:
        data = 0

    dic = {"today_updown": data}
    values = {"$set": dic}

    myquery = {"code": id, "date": ndate}

    table.update_many(myquery, values)



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



def train():
    pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    ts_codes = data["symbol"].values

    def work(ts_codes, table):
        print('================================线程')

        for id in ts_codes:
            try:
                download_y(id, table)

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
        time.sleep(60)
        t = threading.Thread(target=work, args=[ts_code_list, table])
        t.start()
        i += 190

if __name__ == '__main__':



        train()


    # todo 目前已经可以自动化保存所有分笔到mongdb,但是启动时间这里还没有调试成功，现在只能人为启动，下一步需要改写。





