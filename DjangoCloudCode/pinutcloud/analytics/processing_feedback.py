import json
import os
import time
import datetime
import common
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
        user_feedback_cache = {}
        for filename in os.listdir(common.PINUT_FEEDBACK_FILE_PATH):
            logging.debug("Processing file for PinutUser:%s", filename)
            with open(common.PINUT_FEEDBACK_FILE_PATH+"/"+filename, 'r+') as f_in:
                pinut_mac,date_in_filename=common.get_params_from_pinutuser_file(filename)
                cid,lid,content_version=common.get_params_from_pinut_mac(pinut_mac)
                cust_name=common.get_cust_name_from_cid(cid)
                for line in iter(f_in):
                    try:
                        row = json.loads(line, 'utf-8')
                        cl_mac = str(row['cl_mac'])
                        key_tuple = (date_in_filename, pinut_mac, lid, cust_name, cl_mac)
                        #row={"comment": "Good ", "pinut_experience": 5, "name": "preety", "cl_mac": "5c:51:88:07:8f:15", "email_id": "pranetazvision@yahoo.co.in", "phone": "7022895195", "ride_experience": 5}
                        user_feedback_cache[tuple(key_tuple)] = row
                    except IndexError, e:
                        logging.exception("Error [%s] in index operation for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
                        continue
                    except ValueError, e:
                        logging.exception("Error [%s] in value operation for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
                        continue
                    except Exception, e:
                        logging.exception("Unknown Error [%s] in accessing json line for line[%s], file[%s], customer_name[%s]", e, line, filename, cust_name)
                        continue

        return user_feedback_cache

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
            if mongo_db_obj.mongo_select_one(cust_name, "pinut_feedback_data") == None:
                #Creating index for the first time for customer
                mongo_db_obj.mongo_ensure_index(cust_name, "pinut_user_intro_data", ["cust_name","lid","date","pinut_mac","cl_mac"])
            cl_mac = str(data['cl_mac'])
            phone = str(data['phone'])
            email_id = str(data['email_id'])
            name = str(data['name'])
            comment = str(data['comment'])
            if str(cust_name) == "woobus":
                #row={"cl_mac":"c7:f5:c6:0c:b9:ef","phone":"8000678800","email_id":"c1@gmail.com","name":"priya","staff":3,"snacks":5,"cleanliness":3,"in_app_entertainment":5,"bus_tracking":3,"punctuality":5,"overall_experience":3,"comment":"Love the new entertainment media"}
                staff = int(data['staff'])
                snacks = int(data['snacks'])
                cleanliness = int(data['cleanliness'])
                in_app_entertainment = int(data['in_app_entertainment'])
                bus_tracking = int(data['bus_tracking'])
                punctuality = int(data['punctuality'])
                overall_experience = int(data['overall_experience'])
                mongo_db_obj.mongo_insert(cust_name, "pinut_feedback_data", {"lid" : int(lid), "date" : date_obj, "pinut_mac" : pinut_mac, "cl_mac" : cl_mac, "phone" : phone, "email_id" : email_id, "name" : name, "comment" : comment, "staff" : staff, "snacks" : snacks, "cleanliness" : cleanliness, "in_app_entertainment" : in_app_entertainment, "bus_tracking" : bus_tracking, "punctuality" : punctuality, "overall_experience" : overall_experience })
            else:
                #row={"comment": "Good ", "pinut_experience": 5, "name": "preety", "cl_mac": "5c:51:88:07:8f:15", "email_id": "pranetazvision@yahoo.co.in", "phone": "7022895195", "ride_experience": 5}
                pinut_experience = int(data['pinut_experience'])
                ride_experience = int(data['ride_experience'])
                #Inserting data for customer
                mongo_db_obj.mongo_insert(cust_name, "pinut_feedback_data", {"lid" : int(lid), "date" : date_obj, "pinut_mac" : pinut_mac, "cl_mac" : cl_mac, "phone" : phone, "email_id" : email_id, "name" : name, "comment" : comment, "pinut_experience" : pinut_experience, "ride_experience" : ride_experience })
    except Exception, e:
            logging.exception("Exception[%s], could not insert in mongo collection pinut_feedback_data", e)
            raise



if __name__ == '__main__':

	try:
		# Configure logger
		logging.config.fileConfig(common.LOG_CONF_PATH)
		logging.Formatter.converter = time.gmtime
                if os.listdir(common.PINUT_FEEDBACK_FILE_PATH) == []:
                    print "No files to process"
                    sys.exit(0)
                else:
		    user_dict=process_json_files()
		    logging.debug("user_dict: %s",user_dict)
                    dump_in_mongo_collection(user_dict)
	except Exception, e:
		logging.exception("Exception[%s]", e)
