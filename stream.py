import logging
from kiteconnect import KiteTicker
from dbconfig import getaccesstoken,db_connection
import time
from json import dumps
from kafka import KafkaProducer
import datetime
import json
import pandas as pd
import json

producer = KafkaProducer(bootstrap_servers='164.52.207.158:9092',
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


def convertdate(listD):
    newlistD = []
    for obj in listD:
        obj["last_trade_time"] = str(obj.get("last_trade_time",""))
        obj["timestamp"] = str(obj.get("timestamp"))
        newlistD.append(obj)
    return newlistD

def getList(dict):
    list = []
    for key in dict.keys():
        list.append(int(float(key)))
    return list
api_key,access_token = getaccesstoken("ES2479")
logging.basicConfig(level=logging.DEBUG)

# Initialise
kws = KiteTicker(api_key, access_token)

def on_ticks(ws, ticks):
    # Callback to receive ticks.
    logging.debug("Ticks: {}".format(ticks))
    ticks = convertdate(ticks)
    print (ticks,"ticks")
    for t in ticks:
        print (t,"kafka sending")
        producer.send("optionst",value=t)
        producer.flush()

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    jsonR = json.load(open("order.json","r"))
    tokenL = getList(jsonR)
    print (tokenL)
    # ws.subscribe([tokenL])
    ws.subscribe(tokenL)
    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, tokenL)
    # ws.set_mode(ws.MODE_FULL, [tokenL])

def on_close(ws, code, reason):
    # On connection close stop the event loop.
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

def get_account():
    db = db_connection("stock_log")
    timeN = datetime.datetime.now()
    timeT = datetime.datetime.strftime(timeN,"%H:%M")
    sql = "select * from token where bidding_time ='{}'".format(timeT)
    df = pd.read_sql(sql,db)
    print (timeT)
    db.close()
    if len(df) > 0:
        return df["username"][0]
    else:
        return ""

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

def run_main():
    # username = get_account()
    # username = "IK3908"
    if True:
        print ("went in")
        kws.connect()
run_main()
# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
# kws.connect()