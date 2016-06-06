# -*- coding: utf-8 -*-

import time
import logging
import db_analytics
from db_analytics import DB_Analytics

# Return Values(For callback processes)
OK = 0
FAILURE = -1

JSON_PATH="/home/ec2-user/PinutJsonFiles"
PROCESSED_JSON_PATH="/home/ec2-user/ProcessedJsonFiles"
CODE_PATH="/home/ec2-user/Virtual_Env/DjangoCloudCode/DjangoCloudCode/pinutcloud"

LOG_CONF_PATH = CODE_PATH+"/"+"conf/logger.conf"
DB_INFO_CFG = CODE_PATH+"/"+"conf/db_info.cfg"

#==============================================================
#PINUT USER FILE
#Dump these record into json files PinutUser_d1f5c60cb9ff_18_01_2016.json
#{"cl_mac":"c1:f5:c6:0c:b9:ef","phone":8000678800,"email_id":"c1@gmail.com","data":"bajrangi bhaijan.mp4","category":"movie","device_timestamp":14508461}
PINUT_USER_FILE_PATH=JSON_PATH+"/"+"PinutUserFiles"
PINUT_USER_FILE_NAME="PinutUser"

#PINUT USER INTRO FILE
#Dump these record into json files PinutUserIntro_d1f5c60cb9ff_18_01_2016.json
#{"email_id": "pranetazvision@yahoo.co.in", "phone": "7022895195", "name": "preety", "cl_mac": "5c:51:88:07:8f:15"}
PINUT_USER_INTRO_FILE_PATH=JSON_PATH+"/"+"PinutUserIntroFiles"
PINUT_USER_INTRO_FILE_NAME="PinutUserIntro"

#PINUT FEEDBACK FILE
#Dump these record into json files PinutFeedback_d1f5c60cb9ff_18_01_2016.json
#{"comment": "Good ", "pinut_experience": 5, "name": "preety", "cl_mac": "5c:51:88:07:8f:15", "email_id": "pranetazvision@yahoo.co.in", "phone": "7022895195", "ride_experience": 5}
PINUT_FEEDBACK_FILE_PATH=JSON_PATH+"/"+"PinutFeedbackFiles"
PINUT_FEEDBACK_FILE_NAME="PinutFeedback"

#PINUT CONNECTION FILE
#{c1:f5:c6:0c:b9:ef:{"connection":1}}
PINUT_CONNECTION_FILE_PATH=JSON_PATH+"/"+"PinutConnectionFiles"
PINUT_CONNECTION_FILE_NAME="PinutConnection"
#=========================================================

#PROCESSED USER FILE (After the files will be processed, they'll be moved from PinutJsonFiles folder to ProcessedJsonFiles folder)
PROCESSED_USER_FILE_PATH=PROCESSED_JSON_PATH+"/"+"PinutUserFiles"
#PROCESSED USER INTRO FILE
PROCESSED_USER_INTRO_FILE_PATH=PROCESSED_JSON_PATH+"/"+"PinutUserIntroFiles"
#PROCESSED FEEDBACK FILE
PROCESSED_FEEDBACK_FILE_PATH=PROCESSED_JSON_PATH+"/"+"PinutFeedbackFiles"
#PROCESSED CONNECTION FILE
PROCESSED_CONNECTION_FILE_PATH=PROCESSED_JSON_PATH+"/"+"PinutConnectionFiles"
#===========================================================

#Queries:
GET_CUSTID_LOCID_CONTENTVERSION_FROM_PINUT_MAC="SELECT cid,lid,content_version from pinut_devices where pinut_mac=%s"
GET_CUST_NAME_FROM_CID="SELECT cust_name from customer where cid=%s"


def mkgmtime(x):
	ltime_sec = time.mktime(x)
	ltime = time.localtime(ltime_sec)
	if ltime.tm_isdst and time.daylight:
		ret_time = ltime_sec - time.altzone
	else:
		ret_time = ltime_sec - time.timezone
	return ret_time


def convert_date_str_to_timestamp(date_str):
	return (mkgmtime(time.strptime(date_str, '%d-%m-%Y')))


def convert_timestamp_to_date_str(date_time):
	return (time.strftime(' %d-%m-%Y', time.gmtime(date_time)))


def get_pinut_device_timestamp():
	#Check if local Or UTC needs to be sent ??
	return time.time()


def get_pinut_device_date():
	#Check if local Or UTC needs to be sent ??(cron job -local , filenames , in mongo , in postgres)
	return time.strftime("%d_%m_%Y")


def remove_colons_from_mac_address(mac_addr):
	mac_list=mac_addr.split(':')
	MAC = ''.join([str(mac) for mac in mac_list])
	return MAC


def get_params_from_pinutuser_file(filename):
	try:
		split_name=filename.split("_")
		pinut_mac_var=split_name[1]
		pinut_mac = ':'.join(pinut_mac_var[i:i+2] for i in range(0, len(pinut_mac_var), 2))
		day=split_name[2]
		month=split_name[3]
		year=split_name[4].split(".")[0]
		date_in_filename=str(day)+"-"+str(month)+"-"+str(year)
		return pinut_mac,date_in_filename
	except Exception, e:
		logging.exception("Exception [%s] in getting params for filename[%s] ", e, filename)
		raise
	

def get_params_from_pinutconnection_file(filename):
	try:
		split_name=filename.split("_")
		pinut_mac_var=split_name[1]
		pinut_mac = ':'.join(pinut_mac_var[i:i+2] for i in range(0, len(pinut_mac_var), 2))
		clmac_var=split_name[2]
		clmac = ':'.join(clmac_var[i:i+2] for i in range(0, len(clmac_var), 2))
		day=split_name[3]
		month=split_name[4]
		year=split_name[5].split(".")[0]
		date_in_filename=str(day)+"-"+str(month)+"-"+str(year)
		return pinut_mac,clmac,date_in_filename
	except Exception, e:
		logging.exception("Exception [%s] in getting params for filename[%s] ", e, filename)
		raise


def get_params_from_pinut_mac(pinut_mac):
	db_obj=None
	try:
		db_obj = DB_Analytics()
		pinut_rows = db_obj.select_query(GET_CUSTID_LOCID_CONTENTVERSION_FROM_PINUT_MAC, str(pinut_mac))
		for pinut_row in pinut_rows:
			cid = int(pinut_row['cid'])
			lid = int(pinut_row['lid'])
			content_version = str(pinut_row['content_version'])
		return cid,lid,content_version
	except db_analytics.Query_Error, e:
		logging.exception("Error in query:%s"% e)
		raise
	except db_analytics.No_Data_Found, e:
		logging.exception("No data found for query:%s"% e)
		raise
	except Exception, e:
		logging.exception("Exception [%s] in getting params for pinut mac[%s] ", e, pinut_mac)
		raise
	finally:
		if db_obj:
			db_obj.close_connection()


def get_cust_name_from_cid(cid):
	db_obj=None
	try:
		db_obj = DB_Analytics()
		pinut_rows = db_obj.select_query(GET_CUST_NAME_FROM_CID, int(cid))
		for pinut_row in pinut_rows:
			cust_name = str(pinut_row['cust_name'])
		return cust_name
	except db_analytics.Query_Error, e:
		logging.exception("Error in query:%s"% e)
		raise
	except db_analytics.No_Data_Found, e:
		logging.exception("No data found for query:%s"% e)
		raise
	except Exception, e:
		logging.exception("Exception [%s] in getting cust_name from cid[%s] ", e, cid)
		raise
	finally:
		if db_obj:
			db_obj.close_connection()

if __name__ == '__main__':
	filename="PinutUser_d1f5c60cb9ff_18_01_2016.json"
	pinut_mac,date_in_filename=get_params_from_pinutuser_file(filename)
	print pinut_mac,date_in_filename
	print "*************************"
	filename="PinutConnection_d1f5c60cb9ff_c1f5c60cb9ef_18_01_2016.csv"
	pinut_mac,clmac,date_in_filename=get_params_from_pinutconnection_file(filename)
	print pinut_mac,clmac,date_in_filename
