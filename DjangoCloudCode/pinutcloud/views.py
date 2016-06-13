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
from pinutcloud.analytics import utility_script_user_info, utility_script_user_intro, utility_script_feedback
import shutil
import sys
import logging
from pinutcloud.dashboard import forms
#from forms import NameForm

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

def processFeedbackMongoData(request):
	if request.method == "GET":
            #return render(request, 'pinutcloud/highcharts/feedback_content.html')
            return render(request, 'pinutcloud/highcharts/feedback_ratings.html')

def getTotalFeedback(request):
	if request.method == "GET":
		logging.debug( "Inside GET request")
		cust_name = "kk"
		start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		number_of_feedbacks = utility_script_feedback.get_total_feedback(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(number_of_feedbacks)
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")

def getAverageFeedback(request):
	if request.method == "GET":
		logging.debug( "Inside GET request")
		cust_name = "kk"
		start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		feedback_ratings = utility_script_feedback.get_average_feedback(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(json.dumps(feedback_ratings).encode('utf-8'))
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")

def getFeedbackContent(request):
	if request.method == "GET":
		logging.debug( "Inside GET request")
		cust_name = "kk"
		start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		feedback_content = utility_script_feedback.get_feedback_content(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(json.dumps(feedback_content).encode('utf-8'))
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")

#==========================================================================
#USER INTRO API
def processUserIntroMongoData(request):
	if request.method == "GET":
            user_intro_content = getUserIntroContent(request)
            print user_intro_content
            return render(request, 'pinutcloud/highcharts/registration_content.html')

def getTotalDownloads(request):
	if request.method == "GET":
		logging.debug( "Inside GET request")
		cust_name = "kk"
		start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		number_of_downloads = utility_script_user_intro.get_number_of_downloads(cust_name ,start_date_obj ,end_date_obj)
                return HttpResponse(number_of_downloads)
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")


def getUserIntroContent(request):
	if request.method == "GET":
		logging.debug( "Inside GET request")
		cust_name = "kk"
		start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		user_intro_content = utility_script_user_intro.get_user_intro_content(cust_name, start_date_obj, end_date_obj)
                return HttpResponse(json.dumps(user_intro_content).encode('utf-8'))
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")


#==========================================================================

#Process utility_script_user_info and Dump its data to process_user_info Json File.
#This json file will be read by various API's to retrieve data from it.
#USER INFO API
def processUserInfoMongoData(request):
	logging.debug( "Inside processmongodata")
	if request.method == "GET":
		logging.debug( "Inside GET request")
		cust_name = "kk"
		start_date="24-02-2013"
		#To compare this date with mongo date we have converted to date_obj [string to time]
		start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
		end_date="03-03-2017"
		end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")
		utility_script_user_info.get_data_per_location(cust_name ,start_date_obj ,end_date_obj)
                #return render(request, 'pinutcloud/highcharts/popular_movies.html')
                return render(request, 'pinutcloud/highcharts/user_dist_time_slot.html')
		#return HttpResponse("User Info data written to json file successfully")
	else:
		logging.debug( "Its a POST request")
		return HttpResponse("You're in POST request.")

#PARSE USER_INFO FILE
def popularMovieList(request):
    try:
	if request.method == "GET":
            with open('/home/ec2-user/Virtual_Env/DjangoCloudCode/DjangoCloudCode/pinutcloud/processed_user_info.json', 'r') as f:
                content=json.load(f)
                for lid, data in content.iteritems():
                    popular_movie_list=data['popular_movie_list']
                    logging.debug( "popular_movie_list : %s" % popular_movie_list)
                    return HttpResponse(json.dumps(popular_movie_list).encode('utf-8'))
    except Exception, e:
        logging.debug("Exception in retrieving popular movie list from user info %s" % e)

#PARSE USER_INFO FILE
def userDistTimeSlot(request):
    try:
	if request.method == "GET":
            with open('/home/ec2-user/Virtual_Env/DjangoCloudCode/DjangoCloudCode/pinutcloud/processed_user_info.json', 'r') as f:
                content=json.load(f)
                for lid, data in content.iteritems():
                    user_dist_time_slot=data['user_dist_time_slot']
                    logging.debug( "user_dist_time_slot : %s" % user_dist_time_slot)
                    return HttpResponse(json.dumps(user_dist_time_slot).encode('utf-8'))
    except Exception, e:
        logging.debug("Exception in retrieving user dist time slot from user info %s" % e)

#PARSE USER_INFO FILE
def totalUsersConnected(request):
    try:
	if request.method == "GET":
            with open('/home/ec2-user/Virtual_Env/DjangoCloudCode/DjangoCloudCode/pinutcloud/processed_user_info.json', 'r') as f:
                content=json.load(f)
                for lid, data in content.iteritems():
                    total_users_connected=data['total_users_connected']
                    logging.debug( "total_users_connected : %s" % total_users_connected)
                    return HttpResponse(json.dumps(total_users_connected).encode('utf-8'))
    except Exception, e:
        logging.debug("Exception in retrieving total_users_connected from user info %s" % e)

#PARSE USER_INFO FILE
def totalPinutDevices(request):
    try:
	if request.method == "GET":
            with open('/home/ec2-user/Virtual_Env/DjangoCloudCode/DjangoCloudCode/pinutcloud/processed_user_info.json', 'r') as f:
                content=json.load(f)
                for lid, data in content.iteritems():
                    total_pinut_devices=data['total_pinut_devices']
                    logging.debug( "total_pinut_devices : %s" % total_pinut_devices)
                    return HttpResponse(json.dumps(total_pinut_devices).encode('utf-8'))
    except Exception, e:
        logging.debug("Exception in retrieving total pinut devices from user info %s" % e)
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
        # if this is a POST request we need to process the form data
        if request.method == 'POST':
            print "INSIDE POST REQUEST"
            # create a form instance and populate it with data from the request:
            form = forms.NameForm(request.POST)
            print "form",form
            # check whether it's valid:
            if form.is_valid():
                print "form is valid"
                print form.cleaned_data
                # process the data in form.cleaned_data as required
                email = form.cleaned_data['email']
                print "email",email
                password = form.cleaned_data['password']
                print "password",password
                # redirect to a new URL:
                #return HttpResponseRedirect('/thanks/')
                return render(request, "pinutcloud/pages/index.html")
            else:
                print "form is invalid"

        # if a GET (or any other method) we'll create a blank form
        else:
            print "INSIDE GET REQUEST"
            form = forms.NameForm()
            print "form",form

        return render(request, 'pinutcloud/pages/login.html', {'form': form})
    except Exception, e:
        logging.error("Exception : %s " % e)
        return HttpResponse(status=500)
#=================================================================
def highcharts(request):
    try:
        # if this is a POST request we need to process the form data
        if request.method == 'POST':
            print "INSIDE POST REQUEST"
            # create a form instance and populate it with data from the request:
            form = forms.NameForm(request.POST)
            print "form",form
            # check whether it's valid:
            if form.is_valid():
                print "form is valid"
                print form.cleaned_data
                # process the data in form.cleaned_data as required
                email = form.cleaned_data['email']
                print "email",email
                password = form.cleaned_data['password']
                print "password",password
                # redirect to a new URL:
                #return HttpResponseRedirect('/thanks/')
                return render(request, "pinutcloud/pages/index.html")
            else:
                print "form is invalid"

        # if a GET (or any other method) we'll create a blank form
        else:
            print "INSIDE GET REQUEST"
            form = forms.NameForm()
            print "form",form

        return render(request, 'pinutcloud/pages/login.html', {'form': form})
    except Exception, e:
        logging.error("Exception : %s " % e)
        return HttpResponse(status=500)
#=========================================================================
def renderloginpage(request):
    return render(request, "pinutcloud/pages/login.html")

def renderindexpage(request):
    return render(request, "pinutcloud/pages/index.html")

def rendercalendarpage(request):
    return render(request, "pinutcloud/pages/calendar.html")

def renderpiechart(request):
    return render(request, "pinutcloud/highcharts/popular_movies.html")
#===============================================

if __name__ =='__main__':
    parse_data()
