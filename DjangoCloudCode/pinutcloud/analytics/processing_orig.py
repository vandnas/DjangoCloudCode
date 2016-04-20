import json
import gzip
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
import shutil

def get_connection_from_connection_file(pinut_mac,date_in_filename,cl_mac):
	pinut_mac=common.remove_colons_from_mac_address(pinut_mac)
	date_in_filename=date_in_filename.replace("-","_")
	PINUT_CONNECTION_JSON_FILE=common.PINUT_CONNECTION_FILE_PATH+"/"+common.PINUT_CONNECTION_FILE_NAME+"_"+str(pinut_mac)+"_"+str(date_in_filename)+".json"
	if os.path.exists(PINUT_CONNECTION_JSON_FILE):
		with open(PINUT_CONNECTION_JSON_FILE, 'r') as data_file:
			#Load entire file to dictionary
			pinut_connection_dict = json.load(data_file)
			#Check if clmac exist in that dictionary.
			if str(cl_mac) in pinut_connection_dict.keys():
				counter=pinut_connection_dict[str(cl_mac)]
				count=counter['connection']
			else:
				logging.debug("Clmac doesnt exist")
				count=0
	else:
		count=0
	
	return count

def process_assoc_analytics_data():
	try:
                if not os.path.exists(common.PROCESSED_USER_FILE_PATH):
                    os.makedirs(common.PROCESSED_USER_FILE_PATH)
                
		#DS (Data Structure)
		user_dict={}
		for filename in os.listdir(common.PINUT_USER_FILE_PATH):
			#DS
			user_cache = {}
			# input comes from json files
			logging.debug("Processing file for PinutUser:%s", filename)
			#TODO:Files are not zipped currently...zip them later nd use this code
			with open(common.PINUT_USER_FILE_PATH+"/"+filename, 'r+') as f_in:
				#TODO:Calculate largest date of pinut mac and send it to mongo n postgres function
                                #timestamp=convert_date_str_to_timestamp(date_in_filename)[common.py .. to compare dates]
				pinut_mac,date_in_filename=common.get_params_from_pinutuser_file(filename)
                                #calculate_latest_date(timestamp)
				cid,lid,content_version=common.get_params_from_pinut_mac(pinut_mac)
				cust_name=common.get_cust_name_from_cid(cid)

				for line in iter(f_in):
					try:
                                                movie_viewership_percentage = 0
						row = json.loads(line, 'utf-8')
						#row={"cl_mac":"c1:f5:c6:0c:b9:ef","phone":8000678800,"email_id":"c1@gmail.com","data":"bajrangi bhaijan.mp4","category":movie,"device_timestamp":1450846193,"data_length":180,""view_length":120}
						cl_mac = str(row['cl_mac'])
						phone = int(row['phone'])
						email_id = row['email_id']
						data = row['data']
						category = row['category']
						device_timestamp = int(row['device_timestamp'])
						data_length = row['data_length']
						view_length = row['view_length']
                                                movie_viewership_percentage = (view_length*100)/data_length
					except IndexError, e:
						logging.exception("Error [%s] in index operation for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
						continue
					except ValueError, e:
						logging.exception("Error [%s] in value operation for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
						continue
					except Exception, e:
						logging.exception("Unknown Error [%s] in accessing json line for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
						continue

					#DS
					user_cache_key=(lid,date_in_filename,pinut_mac,cl_mac)
					if user_cache.has_key(tuple(user_cache_key)):
						user_cache_value=user_cache[tuple(user_cache_key)]
						viewership_dict=user_cache_value["viewership"]
                                                if data not in viewership_dict.iteritems():
                                                    viewership_dict[str(data)]=movie_viewership_percentage
                                                else:
                                                    existing_movie_viewership_percentage=viewership_dict[str(data)]
                                                    #TODO:Average ?? ...divide by 2?? because both these numbers are %
                                                    viewership_dict[str(data)]=movie_viewership_percentage + existing_movie_viewership_percentage
						user_cache_value["viewership"]=viewership_dict

						if category == "movie":
							movie_list=user_cache_value["movie_list"]
							if data not in movie_list:
								movie_list.append(data)
								user_cache_value["movie_list"]=movie_list
						if category == "music":
							music_list=user_cache_value["music_list"]
							if data not in music_list:
								music_list.append(data)
								user_cache_value["music_list"]=music_list
						if category == "others":
							others_list=user_cache_value["others_list"]
							if data not in others_list:
								others_list.append(data)
								user_cache_value["others_list"]=others_list
			
					else:
						movie_list=[]
						music_list=[]
						others_list=[]
						user_cache_value={}
                                                viewership_dict={}
						user_cache_value["phone"]=phone
						user_cache_value["email_id"]=email_id
						user_cache_value["content_version"]=content_version
                                                viewership_dict[str(data)]=movie_viewership_percentage
						user_cache_value["viewership"]=viewership_dict

						if category == "movie":
							movie_list.append(data)
						if category == "music":
							music_list.append(data)
						if category == "others":
							others_list.append(data)
						user_cache_value["movie_list"]=movie_list
						user_cache_value["music_list"]=music_list
						user_cache_value["others_list"]=others_list
						user_cache_value["device_timestamp"]=device_timestamp
			

					connection_count=get_connection_from_connection_file(pinut_mac,date_in_filename,cl_mac)
					user_cache_value["connection_count"]=connection_count
					user_cache[tuple(user_cache_key)]=user_cache_value
        

				if user_dict.has_key(str(cust_name)):
					user_dict[str(cust_name)].append(user_cache)
				else:
					user_dict[str(cust_name)]=[user_cache]
				logging.debug("****************************************************************")
                logging.debug("Processing of file successful ... Moving it to Processedjson folder")
                shutil.move(common.PINUT_USER_FILE_PATH+"/"+filename, common.PROCESSED_USER_FILE_PATH+"/"+filename)
		dump_in_mongo_collection(user_dict)
		return user_dict

	except db_analytics.Query_Error, e:
		logging.exception("Exception [%s] in query", e)
		raise
	except db_analytics.No_Data_Found, e:
		logging.exception("No data found exception [%s] in query", e)
		raise
	except Exception, e:
		logging.exception("Unknown Exception [%s]", e)
		#TODO: return failure OR raise and in its parent file return failure to engine.
		raise


def dump_in_mongo_collection(user_dict):

	try:
		#INSERTING FINAL OUTPUT IN MONGO
		mongo_db_obj = MongoDBAnalytics()

		for cust_name,data in user_dict.iteritems():
        		if mongo_db_obj.mongo_select_one(cust_name, "pinut_summarized_data") == None:
	        		#Creating index for the first time for customer
        			mongo_db_obj.mongo_ensure_index(cust_name, "pinut_summarized_data", ["cust_name","lid","date","pinut_mac","cl_mac"])
			for item in data:
				for key,value in item.iteritems():
                                        viewership_list=[]
					lid=int(key[0])
					date=str(key[1])
					pinut_mac=str(key[2])
					cl_mac=str(key[3])
					phone=value['phone']
					email_id=value['email_id']
					device_timestamp=value['device_timestamp']
					movie_list=value['movie_list']
					music_list=value['music_list']
					others_list=value['others_list']
					connection_count=value['connection_count']
					viewership=value['viewership']
                                        for movie ,view_percentage in viewership.iteritems():
                                            watched_movie_tuple = (movie ,view_percentage)
                                            viewership_list.append(tuple(watched_movie_tuple))

					#TODO:To calculate largest date processed
					#dump_in_postgres_table(cust_name,lid,date,pinut_mac)
	        
					date_obj = datetime.datetime.strptime(date, "%d-%m-%Y")
					#Inserting data for customer
			    		mongo_db_obj.mongo_insert(cust_name, "pinut_summarized_data", {"lid" : int(lid) ,"date" : date_obj, "pinut_mac" : pinut_mac ,"cl_mac" : cl_mac ,"phone" : phone ,"email_id" : email_id ,"device_timestamp" : device_timestamp ,"movie_list" : movie_list ,"music_list" : music_list ,"others_list" : others_list ,"connection_count" : connection_count, "viewership_list" : viewership_list })
	except Exception, e:
                print e
		logging.exception("Exception[%s], could not insert in mongo collection pinut_summarized_data", e)
		raise


#def dump_in_postgres_table(cust_name,lid,date,pinut_mac):


if __name__ == '__main__':
    
    try:
	# Configure logger
	logging.config.fileConfig(common.LOG_CONF_PATH)
	logging.Formatter.converter = time.gmtime

	user_dict=process_assoc_analytics_data()
	logging.debug("user_dict: %s",user_dict)
    except Exception, e:
        print e
        logging.exception("Exception[%s]", e)
