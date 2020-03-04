import pymongo

"""测试文本，主要是测试用，"""
import datetime

# conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
# db = conn['HDB2']
#
#
#
# table = [db['Hdata{0}'.format(i) ] for i in range(20)]
# print(len(table))
# for i in table:
#     print(i)

# import  time
# print(datetime.datetime.now().strftime("%H:%M"))
# status = True
#
# while status:
#     time.sleep(2)
#     if (datetime.datetime.now().strftime("%H:%M")) == "12:38":
#         print("start")
#         status = False

# import datetime
#
#
# def getYesterday():
#     today = datetime.date.today()
#     oneday = datetime.timedelta(days=1)
#     yesterday = today - oneday
#     s = ""
#     for i in str(yesterday).split("-"):
#         s = s + i
#
#     print(s)
#     return s
#
# getYesterday()


# import pymongo
# import json
# import datetime
# import time
# import logging
# print((datetime.datetime.now().strftime("%H:%M:%S")) )
#
# #600528
# id = "002262"
# conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
# db = conn['HDB3']
# myquery ={"code": id}
# for name in db.list_collection_names():
#
#     mydoc = db[name].find_one(myquery)
#     if mydoc:
#         cursor = db[name].find(myquery, {'_id': 0})
#         print(name)
#         print("ok")

import tushare as ts
pro = ts.pro_api("462fc78ba2417e9a79a5ac00d8b71b2959b2a8875a0457952921ade4")
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
ts_codes = data["symbol"].values

i=0
s=0
print(len(ts_codes))
while i < len(ts_codes):
    # conn = pymongo.MongoClient('mongodb://172.16.13.241:27017/')
    # db = conn['HDB3']
    table = "hdata{0}".format(s)

    if i % 190 == 0:
        s += 1
    if i + 5 > len(ts_codes):
        ts_code_list = ts_codes[i:len(ts_codes) - 1]
    else:
        ts_code_list = ts_codes[i:i + 5]

    # t = threading.Thread(target=work, args=[ts_code_list,table])

    # t.start()
    i += 5

    print(table)