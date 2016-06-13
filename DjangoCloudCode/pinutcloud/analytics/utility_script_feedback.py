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

def get_total_feedback(cust_name, start_date, end_date):
    try:
        mongo_db_obj = MongoDBAnalytics()
        if mongo_db_obj.mongo_select_one(cust_name, "pinut_feedback_data") == None:
            logging.debug("MONGO COLLECTION doesnt exist")
            return 0

        number_of_feedbacks = mongo_db_obj.mongo_select(cust_name, "pinut_feedback_data", {"date" : {"$gte" : start_date , "$lte" : end_date}}).count()
        if number_of_feedbacks == 0:
            logging.debug("No feedback given between your selected dates")
            return 0
        else:
            return number_of_feedbacks
    except Exception, e:
        logging.exception("Exception, No feedback data %s" % e)
        raise

def get_feedback_content(cust_name, start_date, end_date):
    try:

        number_of_feedbacks = get_total_feedback(cust_name, start_date, end_date)
        if number_of_feedbacks == 0:
            logging.debug("No feedback given between your selected dates")
            return 0

        content_list=[]
        mongo_db_obj = MongoDBAnalytics()
        content = mongo_db_obj.mongo_select(cust_name, "pinut_feedback_data", {"date" : {"$gte" : start_date , "$lte" : end_date}})
        entries_to_remove = ('pinut_mac', 'cl_mac', 'lid', '_id', 'date')
        if str(cust_name) == "woobus":
            for row in content:
                for k in entries_to_remove:
                    row.pop(k, None)
                content_list.append(row)
        else:
            for row in content:
                for k in entries_to_remove:
                    row.pop(k, None)
                content_list.append(row)
        print "content_list", content_list
        return content_list
    except Exception, e:
        logging.exception("Exception, in processing user intro data %s" % e)
        raise

def get_average_feedback(cust_name, start_date, end_date):
    try:

        number_of_feedbacks = get_total_feedback(cust_name, start_date, end_date)
        if number_of_feedbacks == 0:
            logging.debug("No feedback given between your selected dates")
            return 0

        #number of stars out of which rating is given to every service
        star_rating=5
        feedback_dict={}
        staff = snacks = cleanliness = in_app_entertainment = bus_tracking = punctuality = overall_experience = pinut_experience = ride_experience = 0
        mongo_db_obj = MongoDBAnalytics()
        content = mongo_db_obj.mongo_select(cust_name, "pinut_feedback_data", {"date" : {"$gte" : start_date , "$lte" : end_date}})

        if str(cust_name) == "woobus":
            for row in content:
                #row={"cl_mac":"c7:f5:c6:0c:b9:ef","phone":"8000678800","email_id":"c1@gmail.com","name":"priya","staff":3,"snacks":5,"cleanliness":3,"in_app_entertainment":5,"bus_tracking":3,"punctuality":5,"overall_experience":3,"comment":"Love the new entertainment media"}
                staff = staff + int(row['staff'])
                snacks = snacks + int(row['snacks'])
                cleanliness = cleanliness + int(row['cleanliness'])
                in_app_entertainment = in_app_entertainment + int(row['in_app_entertainment'])
                bus_tracking = bus_tracking + int(row['bus_tracking'])
                punctuality = punctuality + int(row['punctuality'])
                overall_experience = overall_experience + int(row['overall_experience'])
        else:
            for row in content:
                #row={"comment": "Good ", "pinut_experience": 5, "name": "preety", "cl_mac": "5c:51:88:07:8f:15", "email_id": "pranetazvision@yahoo.co.in", "phone": "7022895195", "ride_experience": 5}
                pinut_experience = pinut_experience + int(row['pinut_experience'])
                ride_experience = ride_experience + int(row['ride_experience'])


        if str(cust_name) == "woobus":
            staff_perc = (staff * 100)/(number_of_feedbacks * star_rating)
            snacks_perc = (snacks * 100)/(number_of_feedbacks * star_rating)
            cleanliness_perc = (cleanliness * 100)/(number_of_feedbacks * star_rating)
            in_app_entertainment_perc = (in_app_entertainment * 100)/(number_of_feedbacks * star_rating)
            bus_tracking_perc = (bus_tracking * 100)/(number_of_feedbacks * star_rating)
            punctuality_perc = (punctuality * 100)/(number_of_feedbacks * star_rating)
            overall_experience_perc = (overall_experience * 100)/(number_of_feedbacks * star_rating)
           
            feedback_dict["overall_experience_perc"]=overall_experience_perc
            feedback_dict["snacks_perc"]=snacks_perc
            feedback_dict["cleanliness_perc"]=cleanliness_perc
            feedback_dict["in_app_entertainment_perc"]=in_app_entertainment_perc
            feedback_dict["bus_tracking_perc"]=bus_tracking_perc
            feedback_dict["punctuality_perc"]=punctuality_perc
             
            print "snacks_perc",snacks_perc
            print "cleanliness_perc",cleanliness_perc
            print "in_app_entertainment_perc",in_app_entertainment_perc
            print "bus_tracking_perc",bus_tracking_perc
            print "punctuality_perc",punctuality_perc
            print "overall_experience_perc",overall_experience_perc
            print "number_of_feedbacks", number_of_feedbacks
            return feedback_dict
        else:
            pinut_experience_perc = (pinut_experience *100)/(number_of_feedbacks * star_rating)
            ride_experience_perc = (ride_experience * 100)/(number_of_feedbacks * star_rating)
        
            feedback_dict["pinut_experience_perc"]=pinut_experience_perc
            feedback_dict["ride_experience_perc"]=ride_experience_perc
            print "pinut_experience_perc", pinut_experience_perc
            print "ride_experience_perc", ride_experience_perc
            print "number_of_feedbacks", number_of_feedbacks
            return feedback_dict
            

    except Exception, e:
        logging.exception("Exception, in processing user intro data %s" % e)
        raise


if __name__ == '__main__':

    try:
        
        #Configure logger
        logging.config.fileConfig(common.LOG_CONF_PATH)
        logging.Formatter.converter = time.gmtime
        cust_name="kk"
        start_date="24-02-2015"
        #To compare this date with mongo date we have converted to date_obj [string to time]
        start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end_date="03-03-2017"
        end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")

        ret_val = get_total_feedback(cust_name, start_date_obj, end_date_obj)
        print "total feedbacks", ret_val
        if ret_val == 0:
            logging.debug("FEEDBACK data not available")
        print "*********************************"
        if str(cust_name) == "woobus":
                staff_perc, snacks_perc, cleanliness_perc, in_app_entertainment_perc, bus_tracking_perc, punctuality_perc, overall_experience_perc = get_average_feedback(cust_name ,start_date_obj ,end_date_obj)
        else:
                pinut_experience_perc, ride_experience_perc = get_average_feedback(cust_name ,start_date_obj ,end_date_obj)
        print "*********************************"

        content = get_feedback_content(cust_name, start_date_obj, end_date_obj)
        print "*********************************"

        sys.exit(0)
    except Exception, e:
        logging.exception("Exception, in processing user intro data %s" % e)
        sys.exit(1)
