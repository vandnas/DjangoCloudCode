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
import sys

def get_number_of_downloads(cust_name, start_date, end_date):
    try:
        mongo_db_obj = MongoDBAnalytics()
        if mongo_db_obj.mongo_select_one(cust_name, "pinut_user_intro_data") == None:
            logging.debug("MONGO COLLECTION doesnt exist")
            return 0
        number_of_downloads = mongo_db_obj.mongo_select(cust_name, "pinut_user_intro_data", {"date" : {"$gte" : start_date , "$lte" : end_date}}).count()
        if number_of_downloads == 0:
            logging.debug("No download done between your selected dates")
            return 0
        else:
            return number_of_downloads

    except Exception, e:
        logging.exception("Exception, in getting number of downloads from user intro data %s" % e)
        raise

def get_user_intro_content(cust_name, start_date, end_date):
    try:
        mongo_db_obj = MongoDBAnalytics()

        if mongo_db_obj.mongo_select_one(cust_name, "pinut_user_intro_data") == None:
            logging.debug("MONGO COLLECTION doesnt exist")
            return 0

        content_list=[]
        content = mongo_db_obj.mongo_select(cust_name, "pinut_user_intro_data", {"date" : {"$gte" : start_date , "$lte" : end_date}})
        entries_to_remove = ('pinut_mac', 'cl_mac', 'lid', '_id', 'date')
        for row in content:
            for k in entries_to_remove:
                row.pop(k, None)
            content_list.append(row)
        return content_list
    except Exception, e:
        logging.exception("Exception, in processing user intro data %s" % e)
        raise


if __name__ == '__main__':

    try: 
        #Configure logger
        logging.config.fileConfig(common.LOG_CONF_PATH)
        logging.Formatter.converter = time.gmtime
        cust_name="kk"
        start_date="24-02-2013"
        #To compare this date with mongo date we have converted to date_obj [string to time]
        start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end_date="03-03-2017"
        end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
        number_of_downloads = get_number_of_downloads(cust_name ,start_date_obj ,end_date_obj)
        content_list = get_user_intro_content(cust_name, start_date_obj, end_date_obj)
        print "number_of_downloads" ,number_of_downloads
        print "content_list" ,content_list
    except Exception, e:
        logging.exception("Exception, in processing user intro data %s" % e)
        raise
