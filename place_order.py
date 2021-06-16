import requests
import pandas as pd
import json
# url = "https://oms.theindianafund.com/"
url = "http://0.0.0.0:5000"
def place_order(df):
	for i in range(0,len(df)):
		try:
			tradingsymbol = df["tradingsymbol"][i]
			transaction_type = df["transaction_type"][i]
			username = df["username"][i]
			order_type = df["order_type"][i]
			quantity = df["quantity"][i]
			exchange = df["exchange"][i]
			stop_loss_value = df['Stoploss_trigger_price'][i]
			jsonD = {"username":username,"tradingsymbol":tradingsymbol,"exchange":exchange,"transaction_type":transaction_type,"order_type":order_type,"quantity":quantity,"product":"MIS","validity":"DAY","on_fail":"MARKET","stop_loss_value":stop_loss_value}
			urlD = url+"/api/order/preplace"
			resp = requests.post(urlD,data = json.dumps(jsonD)).json()
			status = ""
			order_id = ""
			if "listofstatus" in resp:
				resp = resp.get("listofstatus",[])
				print (resp)
				for r in resp:
					if "dataFromZ_on_fail" in r:
						order_id += r.get("dataFromZ_on_fail").get("data",{}).get("order_id","") + "|"
						status = r.get("status","") + " ON FAIL |"
					else:
						order_id += r.get("data",{}).get("order_id","") + "|"
						status = r.get("status","") + "|"
				df.loc[i,"Order_Status"] = status
				df.loc[i,"msg"] = order_id
			else:
				df.loc[i,"Order_Status"] = resp.get("status","")
				df.loc[i,"msg"] = resp.get("msg","")

		except Exception as e:
			df.loc[i,"Order_Status"] = "Failed"
			df.loc[i,"msg"] = "Some error in the main code - {}".format(e)
	return df


# def place_order(df):
# 	status = "Success"
# 	for i in range(0,len(df)):
# 		df.loc[i,"Order_Status"] = status
# 		df.loc[i,"msg"] = str(i)
# 	return df

def place_single_order(jsonD):
	jsonD["on_fail"] = "MARKET"
	urlD = url+"/api/order/preplace"
	resp = requests.post(urlD,data = json.dumps(jsonD)).json()
	status = ""
	order_id = ""
	if "listofstatus" in resp:
		resp = resp.get("listofstatus",[])
		print (resp)
		for r in resp:
			if "dataFromZ_on_fail" in r:
				order_id += r.get("dataFromZ_on_fail").get("data",{}).get("order_id","") + "|"
				status = r.get("status","") + " ON FAIL |"
			else:
				order_id += r.get("data",{}).get("order_id","") + "|"
				status = r.get("status","") + "|"
	else:
		status = resp.get("status","")
		order_id = resp.get("msg","")
	return status,order_id,resp


def modify_order(jsonD):
	urlD = url+"/api/order/modify"
	resp = requests.post(urlD,data = json.dumps(jsonD)).json()
	print (resp)
	if resp.get("status","") == "Succss":
		return "Success",resp
	else:
		return "Fail",resp


def cancel_order(jsonD):
	urlD = url+"/api/order/cancel"
	resp = requests.post(urlD,data=json.dumps(jsonD)).json()
	print (resp)
	if resp.get("status","") == "Success":
		return "Success",resp
	else:
		return "Fail",resp