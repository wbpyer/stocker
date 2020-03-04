from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
import time
import tushare as ts
import pandas as pd
import json
import pymongo
import logging
import datetime


pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("downloader")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)



"""

用来下载集合竞价的数据，并保存下来，建立数据中心。单独打一个docker部署。
老板本下载器的问题已经修复，可以继续使用，主要是要考虑两点，一个是线程数，另一个是注意循环的位置。
目前线上部署的就是这个版本的下载器

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



def main(seed:list,tab):
    """

    :param seed: 一个个的股票代码划分列表，
    abc:根据股票代码，制作出文件名
    :return:
    """

    conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
    db = conn['HDB3']
    table = db["hdata{0}".format(tab)]
    abc= [table for i in seed]

    # print(abc)
    #这里同时启动多个线程，等于同时跑多个函数，并把前面准备的参数，按照顺序传入进去。

    with ThreadPoolExecutor(190) as executor1:
            # print(222222)
        while 1:

            executor1.map(download_10tick,seed,abc) #提交任务给进程池。这里始终用相同的进程再跑，注意看循环的位置。



def split_stockid(stocks:list,t_num=190):
    """
    对股票按照进程和线程数进行等分，默认是10*30
    :param stocks:
    :param t_num: 这里是线程数据，根据你的线程数进行切片。 注意
    :param
    :return:
    """
    try:
        seed = []

        temp_list = []

        for i in range(len(stocks)):

            if i% t_num == 0:
                temp_list.append(i)

        for x in range(len(temp_list)-1):

            print((temp_list[x]),(temp_list[x+1]))

            temp = stocks[temp_list[x]:temp_list[x+1]]
            seed.append(temp)
        seed.append(stocks[3610:])
        return seed


    except Exception as e:
        print(e)






data:pd.DataFrame
def all_stocks():
    """
    #查询当前所有正常上市交易的股票列表
    :return:
    """

    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # d = data["symbol"].values
    # random.shuffle(d)
    return data["symbol"].values


def run():

    stocks = all_stocks()
    # stocks = stocks[1200:1501]
    seed = split_stockid(stocks)
    print(seed)

    table = [ i for i in range(20)]
    with ProcessPoolExecutor(20) as executor1:


        executor1.map(main,seed,table)






if __name__ == "__main__":
    # while 1 :
    #
    #     if (datetime.datetime.now().strftime("%H:%M")) == "09:15":
    #         try:

                run()
        #     except Exception as e:
        #         #         logging.info(e)
        #         # time.sleep(2)

    #todo 目前已经可以自动化保存所有分笔到mongdb,但是启动时间这里还没有调试成功，现在只能人为启动，下一步需要改写。



