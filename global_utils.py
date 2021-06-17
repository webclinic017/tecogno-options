import requests
from kiteconnect import KiteConnect
from dbconfig import getaccesstoken,db_connection
import pandas as pd
# from crud import retrieveall
import datetime
from mailer import mailer,mailer_with_attachment
import os
import json
import traceback
from place_order import place_order
import time
# mailerList = ["prateekrastogi911@gmail.com","gauravsingh111994@gmail.com","kalpesh.vaghani@gmail.com"]
mailerList = ["contact@tecogno.com"]

def round_nearest_large(x,num=50000):
    return ((x+num//2)//num)*num

def myround(x, prec=2, base=.05):
  return round(base * round(float(x)/base),prec)

def get_inst():
	dateT = str(datetime.datetime.now().date())
	filename = "{}.csv".format(dateT)
	listD = os.listdir("ins")
	if filename in listD:
		df = pd.read_csv("ins/{}".format(filename))
	else:
		api_key,access_token = getaccesstoken("ES2479")
		headers = {"X-Kite-Version":"3","Authorization":"{}:{}".format(api_key,access_token)}
		data = requests.get("https://api.kite.trade/instruments",headers=headers).text
		dataL = data.split("\n")
		mat = [n.split(',') for n in dataL]
		df = pd.DataFrame(mat)
		df.columns = df.iloc[0]
		df = df.reindex(df.index.drop(0))
		df.to_csv("ins/{}".format(filename))
	#token = df[(df["tradingsymbol"] == symbol) & (df['exchange'] == exchange)]["instrument_token"].unique()[0]
	return df

def get_option_df(dfToken,cepename,instrument_type,strike):
	dateT = datetime.datetime.now().date()
	dfToken['expiry']=pd.to_datetime(dfToken['expiry'])
	dfToken['expiry'] = dfToken['expiry'].map(lambda x : x.date())
	dfToken = dfToken.sort_values(by=['expiry'])
	dfTemp = dfToken[(dfToken["name"]=='"'+cepename+'"') & (dfToken["expiry"]>=dateT) & (dfToken['instrument_type']==instrument_type)&(dfToken['strike']==strike)]
	dfTemp = dfTemp.reset_index(drop=True)
	return dfTemp

def get_date_circuit(api_key,access_token,listD):
	kite = KiteConnect(api_key=api_key)
	kite.set_access_token(access_token)
	d = kite.quote(listD)
	return d

def main():
	try:
		baseQ = 10
		usernameL = ["ES2479"]
		df = pd.DataFrame()
		count = 0
		api_key,access_token = getaccesstoken("ES2479")
		data = get_date_circuit(api_key,access_token,"{}:{}".format("NSE","NIFTY BANK"))
		print (data)
		subD = data.get('NSE:NIFTY BANK',{})
		ohlc = subD.get("ohlc")
		higP = subD.get("last_price")
		Stoploss_trigger_price = higP * 1.50

		highR = int(round_nearest_large(higP,100))
		higP = myround(higP)
		Stoploss_trigger_price = myround(Stoploss_trigger_price)
		insdf = get_inst()
		dfce = get_option_df(insdf,"BANKNIFTY","CE",highR)
		time.sleep(2)
		datace = get_date_circuit(api_key,access_token,"{}:{}".format("NFO",dfce["tradingsymbol"][0]))
		quantityce = dfce['lot_size'][0] * baseQ
		highce = datace.get("NFO:{}".format(dfce['tradingsymbol'][0])).get("ohlc").get("high")
		marketce = datace.get("NFO:{}".format(dfce['tradingsymbol'][0])).get("last_price")
		sLce = myround(highce * 1.50)
		dfpe = get_option_df(insdf,"BANKNIFTY","PE",highR)
		time.sleep(2)
		datape = get_date_circuit(api_key,access_token,"{}:{}".format("NFO",dfpe["tradingsymbol"][0]))
		quantitype = dfpe['lot_size'][0] * baseQ
		highPe = datape.get("NFO:{}".format(dfpe['tradingsymbol'][0])).get("ohlc").get("high")
		marketpe = datape.get("NFO:{}".format(dfpe['tradingsymbol'][0])).get("last_price")
		sLpe = myround(highPe * 1.50)
		jsonR = {}
		jsonR[dfce["instrument_token"][0]] = {"Stoploss_trigger_price":sLce,"pairorderid":dfpe["instrument_token"][0],"marketprice":marketce,"trail":0}
		jsonR[dfpe["instrument_token"][0]] = {"Stoploss_trigger_price":sLpe,"pairorderid":dfce["instrument_token"][0],"marketprice":marketpe,"trail":0}
		print (jsonR)
		with open("order.json","w") as wb:
			json.dump(jsonR,wb)
		for user in usernameL:
			df.loc[count,"tradingsymbol"] = dfce["tradingsymbol"][0]
			df.loc[count,"leg"] = "CE"
			df.loc[count,"transaction_type"] = "SELL"
			df.loc[count,"Stoploss_trigger_price"] = sLce
			df.loc[count,"order_type"] = "MARKET"
			df.loc[count,"exchange"] = "NFO"
			df.loc[count,"quantity"] = quantityce
			df.loc[count,"marketprice"] = marketce
			df.loc[count,"instrument_token"] = dfce["instrument_token"][0]
			df.loc[count,"squared"] = "false"
			df.loc[count,"username"] = user
			count +=1

			df.loc[count,"tradingsymbol"] = dfpe["tradingsymbol"][0]
			df.loc[count,"leg"] = "PE"
			df.loc[count,"transaction_type"] = "SELL"
			df.loc[count,"Stoploss_trigger_price"] = sLpe
			df.loc[count,"order_type"] = "MARKET"
			df.loc[count,"exchange"] = "NFO"
			df.loc[count,"quantity"] = quantitype
			df.loc[count,"marketprice"] = marketpe
			df.loc[count,"instrument_token"] = dfpe["instrument_token"][0]
			df.loc[count,"squared"] = "false"
			df.loc[count,"username"] = user
			count +=1
		df = place_order(df)
		df.to_csv("order.csv")
		dateT = str(datetime.datetime.now().date())
		Subject = "{}-Options - Success".format(dateT)
		for m in mailerList:
			mailer_with_attachment("PFA: Csv",m,Subject,"order.csv")
		return 1
	except Exception as e:
		print(traceback.format_exc(),"First try")
		print (e)
	# else:
		dateT = str(datetime.datetime.now().date())
		print (e)
		for m in mailerList:
			Subject = "{}-Options Code - Some error occured".format(dateT)
			mailer("Some error occured Options - {}".format(e),m,Subject)
		return 0





# api_key,access_token = getaccesstoken("KN8589")


# print (get_date_circuit(api_key,access_token,"{}:{}".format("NSE","NIFTY BANK")))
flag = main()
if flag == 0:
	main()
# get_inst()