import json
import os
import time
import datetime
import common
import db_analytics
from db_analytics import DB_Analytics
#TODO: Need pymongo for mongo...
import mongoDbAnalytics
from mongoDbAnalytics import MongoDBAnalytics
#Modules for logging
import logging
import logging.config
import logging.handlers

def process_json_files():
	try:

		#DS (Data Structure)
                user_intro_cache = {}
		for filename in os.listdir(common.PINUT_USER_INTRO_FILE_PATH):
			logging.debug("Processing file for PinutUser:%s", filename)
			with open(common.PINUT_USER_INTRO_FILE_PATH+"/"+filename, 'r+') as f_in:
				pinut_mac,date_in_filename=common.get_params_from_pinutuser_file(filename)
				cid,lid,content_version=common.get_params_from_pinut_mac(pinut_mac)
				cust_name=common.get_cust_name_from_cid(cid)
				for line in iter(f_in):
					try:
						row = json.loads(line, 'utf-8')
                                                key_tuple = (date_in_filename, pinut_mac, lid, cust_name)
						#row={"cl_mac":"c1:f5:c6:0c:b9:ef","phone":8000678800,"email_id":"c1@gmail.com","name":"ali"}
                                                user_intro_cache(tuple(key_tuple)) = row
					except IndexError, e:
						logging.exception("Error [%s] in index operation for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
						continue
					except ValueError, e:
						logging.exception("Error [%s] in value operation for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
						continue
					except Exception, e:
						logging.exception("Unknown Error [%s] in accessing json line for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
						continue

		return user_intro_cache

	except db_analytics.Query_Error, e:
		logging.exception("Exception [%s] in query", e)
		raise Exception("")
	except db_analytics.No_Data_Found, e:
		logging.exception("No data found exception [%s] in query", e)
		raise Exception("")
	except Exception, e:
		logging.exception("Unknown Exception [%s]", e)
		raise Exception("")


def dump_in_mongo_collection(user_dict):

	try:
		#INSERTING FINAL OUTPUT IN MONGO
		mongo_db_obj = MongoDBAnalytics()

		for key,data in user_dict.iteritems():
                    date_in_filename = str(key[0])
                    pinut_mac = str(key[1])
                    lid = int(key[2])
                    cust_name = str(key[3])
                    date_obj = datetime.datetime.strptime(date_in_filename, "%d-%m-%Y")
		    if mongo_db_obj.mongo_select_one(cust_name, "pinut_user_intro_data") == None:
		        #Creating index for the first time for customer
			mongo_db_obj.mongo_ensure_index(cust_name, "pinut_user_intro_data", ["cust_name","lid","date","pinut_mac"])
                    for key,value in data.iteritems():
					cl_mac = str(value['cl_mac'])
					phone = str(value['phone'])
					email_id = str(value['email_id'])
					name = str(value['name'])
					#Inserting data for customer
					mongo_db_obj.mongo_insert(cust_name, "pinut_user_intro_data", {"lid" : int(lid), "date" : date_obj, "pinut_mac" : pinut_mac, "cl_mac" : cl_mac, "phone" : phone, "email_id" : email_id, "name" : name })
	except Exception, e:
		logging.exception("Exception[%s], could not insert in mongo collection pinut_user_intro_data", e)
		raise



if __name__ == '__main__':

	try:
		# Configure logger
		logging.config.fileConfig(common.LOG_CONF_PATH)
		logging.Formatter.converter = time.gmtime
                if os.listdir(common.PINUT_USER_INTRO_FILE_PATH) == []:
                    print "No files to process"
                    sys.exit(0)
                else:
		    user_dict=process_json_files()
		    logging.debug("user_dict: %s",user_dict)
	except Exception, e:
		logging.exception("Exception[%s]", e)
