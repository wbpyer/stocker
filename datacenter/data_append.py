import tushare as ts
import pandas as pd
import pymongo
import datetime


pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

"""每日天晚上5点执行，用来更新目标值,就是说早上存的是X,晚上6点，也就是现在，存储的是Y。"""
"""只是个是全库全表遍历，动态的寻找，是串行的，适合列表总是改变的，但是我还有
另一个版本，是并发的，是利用线程去写的。适合固定不变的版本，如果是固定不变的，就可以用线程的版本。"""
# todo 也需要加上定时

def val(ts_code,date):

    try:
        df = ts.get_hist_data(ts_code, start=date, end=date)
        print(df)

        y = df["p_change"].values

        if y >= 0.00:
            data = 1
        else:
            data = 0

        dic = {"today_updown" : data}
        values = {"$set": dic}
        return values

    except Exception as e:
        print(e)




if __name__ == "__main__":


    conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
    db = conn['HDB3']
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    ts_codes = data["symbol"].values





    for code in ts_codes:

        myquery = {"code": code, "date": date}
        for name in db.list_collection_names():

            mydoc = db[name].find_one(myquery)
            if mydoc:

                values = val(code,date)

                db[name].update_many(myquery, values)

                print(code,name,"已经找到，执行更新")
                break





