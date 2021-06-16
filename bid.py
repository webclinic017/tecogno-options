from kafka import KafkaConsumer
from json import loads
import json
import datetime
from dbconfig import db_connection,getaccesstoken
import pandas as pd
import _thread
import os, signal 
import requests
import traceback

url = "http://0.0.0.0:5000/"

consumer = KafkaConsumer(
    'optionst',
     bootstrap_servers='164.52.207.158:9092',
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id=None,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

def myround(x, prec=2, base=.05):
  return round(base * round(float(x)/base),prec)

def process(name): 
      
    try: 
          
        # iterating through each instance of the proess 
        for line in os.popen("ps ax | grep " + name + " | grep -v grep"):  
            fields = line.split() 
              
            # extracting Process ID from the output 
            pid = fields[0]  
              
            # terminating process  
            os.kill(int(pid), signal.SIGKILL)  
        print("Process Successfully terminated") 
          
    except: 
        print("Error Encountered while running script") 
   
 



# def get_order_id(username,tradingsymbol=""):
#     print (tradingsymbol)
#     db = db_connection("stock_log")
#     if len(tradingsymbol) != 0:
#         sql = "select l2.order_id,l2.price,l2.username,l2.tradingsymbol,l2.order_status,l2.transaction_type from log l1 inner join log l2 on l1.order_id = l2.parent_order_id where l2.order_type_indiana='FOR_SQUARING' and l2.order_status != 'COMPLETE' and l2.order_status != 'CANCELLED' and l2.tradingsymbol='{}' and date(l1.created_at) = curdate() and l2.username in ('{}')".format(tradingsymbol,username)
#     else:
#         sql = "select l2.order_id,l2.price,l2.username,l2.tradingsymbol,l2.order_status,l2.transaction_type from log l1 inner join log l2 on l1.order_id = l2.parent_order_id where l2.order_type_indiana='FOR_SQUARING' and l2.order_status != 'COMPLETE' and l2.order_status != 'CANCELLED'  and date(l1.created_at) = curdate() and l2.username in ('{}')".format(username)

#     df = pd.read_sql(sql,db)
#     db.close()
#     return df

def get_status(order_id):
    order_id = str(int(order_id))
    db = db_connection("stock_log")
    sql = "select * from log where order_id = '{}'".format(order_id)
    df = pd.read_sql(sql,db)
    db.close()
    return df["order_status"][0]

def get_order_id(username,tradingsymbol=""):
    df = pd.read_csv("../adjustment/adjustment_{}.csv".format(username))
    df = df.fillna("")
    if len(tradingsymbol) == 0:
        dfT = df[(df["Order_Status"] != "SQUARED") & (df['ADJUSTMENT']>0)]
        dfT = dfT.reset_index(drop=True)
        return dfT
    else:
        dfT = df[df["tradingsymbol"] == tradingsymbol]
        dfT = dfT.reset_index(drop=True)
        print (len(str(dfT['order_id'][0])),"order id")
        if len(str(dfT['order_id'][0])) == 0:
            df.loc[df["tradingsymbol"] == tradingsymbol,"Order_Status"] = "SQUARED"
            df.to_csv("../adjustment/adjustment_{}.csv".format(username),index=False)
            return pd.DataFrame()
        status = get_status(dfT["order_id"][0])
        print (status,"status")
        if status == "PLACED":
            return dfT
        else:
            df.loc[df["tradingsymbol"] == tradingsymbol,"Order_Status"] = "SQUARED"
            df.to_csv("../adjustment/adjustment_{}.csv".format(username),index=False)
            return pd.DataFrame()

def place_modify(order_id,username,price):
    order_id = str(int(order_id))
    urlD = url+"/api/order/modify"
    jsonD = {"order_id":order_id,"username":username,"trigger_price":price}
    requests.post(urlD,data=json.dumps(jsonD))
    print (order_id,username,price)
    return "done"

def place_modify_ordertype(order_id,username):
    order_id = str(int(order_id))
    urlD = url+"/api/order/modify"
    jsonD = {"order_id":order_id,"username":username,"order_type":"MARKET"}
    requests.post(urlD,data=json.dumps(jsonD))
    return "done"

def get_status_stoploss(order_id):
    order_id = str(int(float(order_id)))
    db = db_connection("stock_log")
    sql = "select * from log where parent_order_id = '{}'".format(order_id)
    df = pd.read_sql(sql,db)
    db.close()
    return df

def modify_stoploss(jsonD):
    urlD = url+"/api/order/modify"
    jsonR = requests.post(urlD,data=json.dumps(jsonD)).json()
    return jsonR

def getout(instrument_token):
    df = pd.read_csv("order.csv")
    flag = 0
    dfT = df[(df["instrument_token"] == float(instrument_token)) & (df['squared'] == False)].reset_index(drop=True)
    if len(dfT) == 0:
        return "Fail",1
    for i in range(0,len(dfT)):
        order_id = dfT["msg"][i].split("|")[0]
        dfS = get_status_stoploss(order_id)
        print (dfS,"getout")
        if dfS['order_status'][i] == 'PLACED':
            resp = modify_stoploss({"order_type":"MARKET","order_id":str(int(float(dfS['order_id'][0]))),"username":dfS['username'][0]})
            if resp.get("status") == "Succss":
                df.loc[df["instrument_token"] == float(instrument_token),"squared"]="true"
            else:
                flag = 1
    df.to_csv("order.csv",index=False)
    return "Succss",flag

def bring_sl_price(instrument_token,price):
    df = pd.read_csv("order.csv")
    print (df)
    flag = 0
    dfT = df[(df["instrument_token"] == float(instrument_token)) & (df['squared'] == False)].reset_index(drop=True)
    if len(dfT) == 0:
        return "Fail",1
    for i in range(0,len(dfT)):
        order_id = dfT["msg"][i].split("|")[0]
        dfS = get_status_stoploss(order_id)
        resp = modify_stoploss({"order_id":str(int(float(dfS['order_id'][0]))),"username":dfS['username'][0],"trigger_price":price})
        print (resp,"resp")
        if resp.get("status") == "Succss":
            # df.loc[df["instrument_token"] == float(instrument_token),"squared"]="true"
            print (resp)
        else:
            flag = 1
    # df.to_csv("order.csv",index=False)
    return "Succss",flag
def main():
    dateN = datetime.datetime.now()
    for message in consumer:
        stime = datetime.datetime(dateN.year,dateN.month,dateN.day,9,20,0)
        etime = datetime.datetime(dateN.year,dateN.month,dateN.day,15,0,0)
        nowtime = datetime.datetime.now()
        if (nowtime >= stime) and (nowtime < etime):
        # if True:
            message = message.value
            jsonR = json.load(open("order.json","r"))
            try:
            # if True:
                timeT = message.get("timestamp")
                print (timeT)
                dateT = datetime.datetime.strptime(timeT,"%Y-%m-%d %H:%M:%S")
                if dateT >= dateN:
                    instrument_token = message.get("instrument_token","")
                    last_price = message.get("last_price")
                    key1 = str(float(instrument_token))
                    priceJ = jsonR.get(key1)
       
                    marketprice = priceJ.get("marketprice")
                    pnl1 = marketprice - last_price
                    squared1 = jsonR[key1].get("squared","")
                    trail1 = jsonR[key1].get("trail")
                    print (jsonR[key1],trail1)
                    pairorderid = priceJ.get("pairorderid")
                    key2 = str(pairorderid)
                    pnl2 = jsonR.get(key2).get("pnl",0)
                    priceM = jsonR.get(key2).get("marketprice",0)
                    squared2 = jsonR[key2].get("squared","")

                    # pnl base out
                    total = pnl1 + pnl2
                    # if total > 0 :
                    if True:
                        plperc = float(total/(marketprice+priceM))*100
                        if (plperc >= 40) or (total <= -80):
                            if len(squared1) == 0:
                                status,flag = getout(instrument_token)
                                if (flag == 0):
                                    jsonR[key1]["squared"] = "true"
                            if len(squared2) == 0:
                                status,flag = getout(pairorderid)
                                if (flag == 0):
                                    jsonR[key2]["squared"] = "true"

                    #checkforstoloss hit
                    Stoploss_trigger_price = jsonR.get(key1).get("Stoploss_trigger_price")
                    sltoprice = jsonR[key1].get("sltoprice","")
                    if (last_price > Stoploss_trigger_price) and (len(sltoprice) == 0):
                        status,flag = bring_sl_price(pairorderid,jsonR[key2]['marketprice'])
                        if flag == 0:
                            jsonR[key2]['Stoploss_trigger_price'] = jsonR[key2]['marketprice']
                            jsonR[key1]['sltoprice'] = "true"

                    individualpnc = float(pnl1/marketprice)* 100
                    percT = (trail1 + 1) * 10
                    if individualpnc > percT:
                    # if True:
                        slprice = jsonR[key1]['Stoploss_trigger_price']
                        slprice = slprice - ((percT/200) * marketprice)
                        slprice = myround(slprice)
                        if slprice > marketprice:
                        # if True:
                            status,flag = bring_sl_price(instrument_token,slprice)
                            if flag == 0:
                                jsonR[key2]['Stoploss_trigger_price'] = slprice
                                jsonR[key1]['trail'] = jsonR[key1]['trail'] + 1

                    jsonR[key1]["pnl"] = pnl1
                    with open("order.json","w") as wb:
                        json.dump(jsonR,wb) 

            except Exception as e:
            # else:
                # process("stream.py")
                # consumer.close()
                print(traceback.format_exc(),"First try")
                print (e)
                pass
        else:
            process("stream.py")
            consumer.close()
            process("bid.py")


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

def get_zerodha_status(order_id,username):
    api_key,access_token = getaccesstoken(username)
    headers = {"X-Kite-Version":"3","Authorization":"token {}:{}".format(api_key,access_token)}
    url = "https://api.kite.trade/orders/{}".format(str(int(order_id)))
    d = requests.get(url,headers=headers).json().get("data",[])
    jsonD = d[len(d)-1]
    status = jsonD.get("status","")
    average_price = jsonD.get("average_price","")
    return status,average_price

def run_main():
    try:
    # if True:
        # username = get_account()
        # username = "IK3908"
        if True:
            main()
    except Exception as  e:
    # else:
        print (datetime.datetime.now(),e)
        consumer.close()
        process("stream.py")

# print (get_order_id("IK3908"))
run_main()