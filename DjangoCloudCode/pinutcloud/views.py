from django.shortcuts import get_object_or_404, render
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.views import generic
from django.http import HttpResponse
import time
import json
import os
import datetime
import zipfile
from pinutcloud.analytics import common, utility_script_user_info, utility_script_user_intro, utility_script_feedback
import shutil
import sys
import logging

logging = logging.getLogger(__name__)


def byteify(input):
   if isinstance(input, dict):
	   return {byteify(key):byteify(value) for key,value in input.iteritems()}
   elif isinstance(input, list):
	   return [byteify(element) for element in input]
   elif isinstance(input, unicode):
	   return input.encode('utf-8')
   else:
	   return input

#==========================================================================
#FEEDBACK API


def getTotalFeedback(request):
	if request.method == "GET":
                cust_name=request.GET['cust_name']
                start_date=request.GET['startdate']
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
                end_date=request.GET['enddate']
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		number_of_feedbacks = utility_script_feedback.get_total_feedback(cust_name ,start_date_obj ,end_date_obj)
                print "number_of_feedbacks",number_of_feedbacks
                return HttpResponse(number_of_feedbacks)
	else:
		return HttpResponse("You're in POST request.")

def getFeedbackRatings(request):
	if request.method == "GET":
                cust_name=request.GET['cust_name']
                start_date=request.GET['startdate']
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
                end_date=request.GET['enddate']
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		feedback_ratings = utility_script_feedback.get_average_feedback(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(json.dumps(feedback_ratings).encode('utf-8'))
	else:
		return HttpResponse("You're in POST request.")

def getFeedbackContent(request):
	if request.method == "GET":
                cust_name=request.GET['cust_name']
                start_date=request.GET['startdate']
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
                end_date=request.GET['enddate']
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		feedback_content = utility_script_feedback.get_feedback_content(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(json.dumps(feedback_content).encode('utf-8'))
	else:
		return HttpResponse("You're in POST request.")

#==========================================================================
#USER INTRO API

#def getTotalDownloads(request, startdate,enddate):
def getTotalDownloads(request):
	if request.method == "GET":
                cust_name=request.GET['cust_name']
                start_date=request.GET['startdate']
                end_date=request.GET['enddate']
		#start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		#end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		number_of_downloads = utility_script_user_intro.get_number_of_downloads(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(number_of_downloads)
	else:
		return HttpResponse("You're in POST request.")


def getUserIntroContent(request):
	if request.method == "GET":
		logging.debug( "Inside GET request")
                cust_name=request.GET['cust_name']
                start_date=request.GET['startdate']
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
                end_date=request.GET['enddate']
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		user_intro_content = utility_script_user_intro.get_user_intro_content(cust_name, start_date_obj, end_date_obj)
                return HttpResponse(json.dumps(user_intro_content).encode('utf-8'))
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")


#==========================================================================
#CAPTIVE PORTAL API
#Raspberry Pi will be sending some information (like how many downloads, how many people opened captive portal) to AWS (Cloud). Cloud will dump
#all this information in a file


def captivedata(request):
    if request.method == "POST":
        pinut_captive_data_dict={}
        #Get body from post request
        msg=request.body
        #Decode it
        string_msg=msg.decode("utf-8")
        #Load into Json format
        json_data=json.loads(string_msg);
        captive_data_filename = json_data['filename']
        captive_data = json_data['data']
        #Byteify it
        pinut_captive_data_dict = byteify(captive_data)
        with open(common.PINUT_CAPTIVE_DATA_FILE_PATH + "/" + captive_data_filename, 'w') as fp:
            json.dump(captive_data, fp)
        return HttpResponse("You're in POST request.")
    else:
        return HttpResponse("You're in GET request.")


#==========================================================================

#Process utility_script_user_info and give its entire data on this API call.
#The index.html page will parse this json data into 4 charts
#Popular Movies Pie Chart , User Dist Time Slot Pie Chart , Pinut Travellers Widget, Pinut Devices Widget
#USER INFO API
def processUserInfoMongoData(request):
	logging.debug( "Inside processmongodata")
	if request.method == "GET":
		logging.debug( "Inside GET request")
                cust_name=request.GET['cust_name']
                start_date=request.GET['startdate']
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
                end_date=request.GET['enddate']
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		user_info_data = utility_script_user_info.get_data_per_location(cust_name ,start_date_obj ,end_date_obj)
                print "user_info_data",user_info_data
		#User Info data written to json file successfully
                return HttpResponse(json.dumps(user_info_data).encode('utf-8'))
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")


#==========================================================================

def write_into_file(write_file_path, file_content):
	try:
		with open(write_file_path, 'w') as fp:
			fp.write(file_content)
	except Exception, e:
		os.remove(write_file_path)
                logging.error("Error : [%s] in writing zipped file to %s", e, write_file_path)
		raise

def extract_zipped_files(write_file_path, extract_file_path):
	try:
		zip_ref = zipfile.ZipFile(write_file_path, 'r')
		zip_ref.extractall(extract_file_path)
		zip_ref.close()
		os.remove(write_file_path)
	except Exception, e:
                logging.error("Error in extracting zipped file : %s to %s . Error: %s"%(write_file_path, extract_file_path, e))
		raise

def check_if_file_already_exist_in_folder(filename, file_path, folder_name, extract_file_path):
	try:
		folder_path=extract_file_path+"/"+folder_name
		if filename not in os.listdir(folder_path):
			shutil.move(file_path, folder_path)
		else:
			os.remove(file_path)
	except Exception, e:
                logging.error("File: %s already exists in folder: %s , Error: %s"%(file_path, folder_name, e))
		raise

def move_files_to_json_folders(extract_file_path):
	try:
		for filename in os.listdir(extract_file_path):
			file_path=os.path.join(extract_file_path, filename)
			if os.path.isfile(file_path):
				file_prefix=filename.split("_")
				if file_prefix[0] == "PinutUserIntro":
					check_if_file_already_exist_in_folder(filename, file_path, "PinutUserIntroFiles", extract_file_path)
				elif file_prefix[0] == "PinutUser":
					check_if_file_already_exist_in_folder(filename, file_path, "PinutUserFiles", extract_file_path)
				elif file_prefix[0] == "PinutConnection":
					check_if_file_already_exist_in_folder(filename, file_path, "PinutConnectionFiles", extract_file_path)
				elif file_prefix[0] == "PinutFeedback":
					check_if_file_already_exist_in_folder(filename, file_path, "PinutFeedbackFiles", extract_file_path)
				else:
					logging.debug("Invalid file prefix: %s" % file_prefix)
	except Exception, e:
                logging.error("Error in moving files to json folders: %s "% e)
		raise

def uploadjsonfiles(request):	
	try:
                
		write_file_path = "/home/ec2-user/a.zip"
		extract_file_path = "/home/ec2-user/PinutJsonFiles"
		logging.debug("Inside uploadjsonfiles")
		if request.method == "POST":
			file_content=request.body;
			write_into_file(write_file_path, file_content)
			extract_zipped_files(write_file_path, extract_file_path)
			move_files_to_json_folders(extract_file_path)
			return HttpResponse(status=200)
                else:
                    return HttpResponse("You're in GET request.")
	except Exception, e:
                logging.error("Exception : %s " % e)
		return HttpResponse(status=500)

#==========================================================================
def validatelogin(request):
    try:
        # Validate login form data
        if request.method == 'GET':
            # create a form instance and populate it with data from the request:
            email=request.GET['email']
            password = request.GET['password']
            try:
                customer_info={}
                cust_name, loc_name = common.login_validation(email,password)
                customer_info['cust_name'] = cust_name
                customer_info['loc_name'] = loc_name
                return HttpResponse(json.dumps(customer_info).encode('utf-8'))
            except Exception, e:
                return HttpResponse("0")

    except Exception, e:
        logging.error("Exception : %s " % e)
        return HttpResponse(status=500)
#=================================================================

