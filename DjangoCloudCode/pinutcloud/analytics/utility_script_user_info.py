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
import json
import os

def cal_start_date(n):
        current_date = datetime.datetime.utcnow()
        n_days = datetime.timedelta(days=n)
        start_date = current_date - n_days
        return start_date

def get_data_per_device(cust_name, start_date, end_date):
    try:
        mongo_db_obj = MongoDBAnalytics()

        if mongo_db_obj.mongo_select_one(cust_name, "pinut_summarized_data") == None:
            logging.debug("MONGO COLLECTION doesnt exist")

        content = mongo_db_obj.mongo_select(cust_name, "pinut_summarized_data", {"date" : {"$gte" : start_date , "$lte" : end_date}})
        print "content/device",content
        row_count = mongo_db_obj.mongo_select(cust_name, "pinut_summarized_data", {"date" : {"$gte" : start_date , "$lte" : end_date}}).count()
        print "row_count",row_count
        lid_devicemac_dict={}
        for row in content:
            users_data={}
            popular_movies={}
            pinut_mac = row['pinut_mac']
            lid = row['lid']
            date_in_mongo = row['date']
            movie_list = row['movie_list']
            device_timesatmp = row['device_timestamp']
            date_time_string = (datetime.datetime.utcfromtimestamp(int(device_timesatmp)).strftime('%m/%d/%Y %H:%M:%S'))
            date_time_split = date_time_string.split(" ")
            time_string = date_time_split[1]
            hr = int(time_string.split(":")[0])
            time_slot=None
            if hr >= 8 and hr <= 10:
                time_slot = "8am-11am"
            elif hr >=11 and hr <= 12:
                time_slot = "11am-1pm"
            elif hr >=13 and hr <= 15:
                time_slot = "1pm-4pm"
            elif hr >=16 and hr <= 19:
                time_slot = "4pm-8pm"
            elif hr >=20 and hr <= 23:
                time_slot = "8pm-12am"
            else:
                time_slot = "12am-8am"
            
            key_tuple = (lid, pinut_mac)
            if lid_devicemac_dict.has_key(tuple(key_tuple)):
                users_data=lid_devicemac_dict[tuple(key_tuple)]
                users_connected=users_data['users_connected']
                popular_movies=users_data['popular_movies']
                time_slot_dict=users_data['user_dist_time_slot']
                users_data['users_connected'] = users_connected + 1
                for movie in movie_list:
                    if movie in popular_movies:
                        count=popular_movies[str(movie)]
                        popular_movies[str(movie)]=count+1
                    else:
                        popular_movies[str(movie)]=1
                users_data['popular_movies']=popular_movies
                if time_slot in time_slot_dict:
                    count=time_slot_dict[time_slot]
                    time_slot_dict[time_slot]=count+1
                else:
                    time_slot_dict[time_slot]=1
            else:
                time_slot_dict={}
                users_data['users_connected'] = 1
                for movie in movie_list:
                    popular_movies[str(movie)] = 1
                
                users_data['popular_movies']=popular_movies
                time_slot_dict[time_slot] = 1
            users_data['user_dist_time_slot'] = time_slot_dict
            lid_devicemac_dict[tuple(key_tuple)]=users_data

        return lid_devicemac_dict
                

#        lids = mongo_db_obj.mongo_find_distinct(cust_name, "pinut_summarized_data", "lid")
#        pinut_macs = mongo_db_obj.mongo_find_distinct(cust_name, "pinut_summarized_data", "pinut_mac")
#        lid_devicemac_dict={}
#        for lid in lids:
#            for mac in macs:
#                print mac
#                start_date = cal_start_date(7)
#                rows = mongo_db_obj.mongo_select(cust_name, "pinut_summarized_data", {"lid":int(lid), "pinut_mac":str(mac), "date" : {"$gt" : start_date}})
#                row_count = mongo_db_obj.mongo_select(cust_name, "pinut_summarized_data", {"lid":int(lid), "pinut_mac":str(mac), "date" : {"$gt" : start_date}}).count()
#                if row_count > 0:
#                    lid_devicemac_dict['users_connected_in_last_7_days'] = int(row_count)
#                    lid_devicemac_dict['users_connected_in_last_14_days'] = int(row_count)
#                    lid_devicemac_dict['users_connected_in_last_30_days'] = int(row_count)
#                    lid_devicemac_dict['lid'] = int(lid)
#                    lid_devicemac_dict['pinut_mac'] = int(mac)
                    


 
    except Exception, e:
        logging.exception("Exception, in processing data across pinut_device %s" % e)
        raise

def get_data_per_location(cust_name ,start_date_obj ,end_date_obj):
    try:
        print "start_date_obj",start_date_obj
        print "end_date_obj",end_date_obj
#        lids = mongo_db_obj.mongo_find_distinct(cust_name, "pinut_summarized_data", "lid")
#        pinut_macs = mongo_db_obj.mongo_find_distinct(cust_name, "pinut_summarized_data", "pinut_mac")
#        lid_devicemac_dict {(1, u'd2:f5:c6:0c:b9:ff'): {'user_dist_time_slot': {'12am-8am': 3}, 'popular_movies': {'c11.mp4': 1, 'c22.mp4': 1, 'c33.mp4': 1}, 'users_connected': 3}, (1, u'd3:f5:c6:0c:b9:ff'): {'user_dist_time_slot': {'4pm-8pm': 1, '12am-8am': 1, '1pm-4pm': 1}, 'popular_movies': {'c11.mp4': 1, 'c22.mp4': 1, 'c33.mp4': 1}, 'users_connected': 3}, (2, u'd1:f5:c6:0c:b9:ff'): {'user_dist_time_slot': {'12am-8am': 11, '1pm-4pm': 1}, 'popular_movies': {'piku.mp4': 4, 'dilli.mp4': 6, 'dilli6.mp4': 3, 'bhaijan.mp4': 4, 'bajrangi.mp4': 4, 'dil.mp4': 3}, 'users_connected': 12}}


        lid_devicemac_dict = get_data_per_device(cust_name ,start_date_obj ,end_date_obj)
        print "lid_devicemac_dict",lid_devicemac_dict
        lid_dict={}
        for lid_device_mac_key , data in lid_devicemac_dict.iteritems():
            lid=lid_device_mac_key[0]
            pinut_mac=lid_device_mac_key[0]
            popular_movies=data['popular_movies']
            users_connected=data['users_connected']
            time_slot_dict=data['user_dist_time_slot']
            if lid_dict.has_key(lid):
                data_cache=lid_dict[lid]
                existing_popular_movies=data_cache['popular_movie_list']
                for movie in popular_movies:
                    if movie in existing_popular_movies:
                        count=existing_popular_movies[movie]
                        existing_popular_movies[movie]=count+1
                    else:
                        existing_popular_movies[movie]=1
                data_cache['popular_movie_list']=existing_popular_movies 
                existing_users_connected_count=data_cache['total_users_connected']
                data_cache['total_users_connected']=existing_users_connected_count+users_connected
                existing_pinut_device_count=data_cache['total_pinut_devices']
                data_cache['total_pinut_devices']=existing_pinut_device_count+1
                existing_time_slot_dict=data_cache['user_dist_time_slot']
                for time_slot in time_slot_dict:
                    current_count=time_slot_dict[time_slot]
                    if time_slot in existing_time_slot_dict:
                        existing_count=existing_time_slot_dict[time_slot]
                        existing_time_slot_dict[time_slot]=existing_count+current_count
                    else:
                        existing_time_slot_dict[time_slot]=current_count
                data_cache['user_dist_time_slot']=existing_time_slot_dict
            else:
                data_cache={}
                data_cache['popular_movie_list']=popular_movies
                data_cache['total_users_connected']=users_connected
                data_cache['total_pinut_devices']=1
                data_cache['user_dist_time_slot']=time_slot_dict
            
            lid_dict[lid]=data_cache

        with open('/home/ec2-user/Virtual_Env/DjangoCloudCode/DjangoCloudCode/pinutcloud/processed_user_info.json', 'w') as f:
            json.dump(lid_dict, f)

        print "lid_dict", lid_dict

            
    except Exception, e:
        logging.exception("Exception, in processing data across location : %s" % e)
        raise

if __name__ == '__main__':

    try: 
        #Configure logger
        #TODO: Start date , end date and customer name will come from API
        logging.config.fileConfig(common.LOG_CONF_PATH)
        logging.Formatter.converter = time.gmtime
        cust_name="kk"
        start_date="24-02-2015"
        #To compare this date with mongo date we have converted to date_obj [string to time]
        start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end_date="03-03-2017"
        end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
        lid_devicemac_dict = get_data_per_device(cust_name ,start_date_obj ,end_date_obj)
        #print "lid_devicemac_dict" ,lid_devicemac_dict
        get_data_per_location(cust_name ,start_date_obj ,end_date_obj)
        sys.exit(0)
    except Exception, e:
        logging.exception("Exception, in processing data across location : %s" % e)
        sys.exit(1)
