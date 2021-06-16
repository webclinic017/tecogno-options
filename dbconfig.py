import pymysql
import pandas as pd
# HOST = "34.70.47.170"
#lab	HOST = "35.232.148.19"
HOST = "164.52.207.114"
USER = "root"
PASS = "saymyname"
DATABASE = "stock_production"

def db_connection(DATABASE="stock_production"):
	if DATABASE == "stock_log":
		HOST = "104.154.93.88"
		USER = "gaurav"
		PASS = "gaurav"
	else:
		HOST = "164.52.207.114"
		USER = "root"
		PASS = "saymyname"
	db = pymysql.connect(host=HOST,user=USER,passwd=PASS,db=DATABASE)
	return db


def getaccesstoken(username):
	db = db_connection("stock_log")
	df = pd.read_sql("select apikey,token from token where username='{}'".format(username),db)
	apikey,token = df["apikey"][0],df["token"][0]
	db.close()
	return apikey,token

