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
from pinutcloud.analytics import utility_script

print "CWD:",os.getcwd()
def byteify(input):
   if isinstance(input, dict):
       return {byteify(key):byteify(value) for key,value in input.iteritems()}
   elif isinstance(input, list):
       return [byteify(element) for element in input]
   elif isinstance(input, unicode):
       return input.encode('utf-8')
   else:
       return input



#Dump into User Json Files
#POST REQUEST
def processmongodata(request):
	print "Inside processmongodata"
	if request.method == "GET":
		print "Inside GET request"
                cust_name = "ola"
                start_date="24-02-2016"
                #To compare this date with mongo date we have converted to date_obj [string to time]
                start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y")
                end_date="03-03-2016"
                end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y")

                #lid_dict = GLOBAL_PATH_ANALYTICS.get_data_per_location(cust_name ,start_date_obj ,end_date_obj)
                lid_dict = utility_script.get_data_per_location(cust_name ,start_date_obj ,end_date_obj)
                print "******************************************************"
                print "lid_dict", lid_dict
                popular_movie_list=popularmovies(lid_dict)
                print "popular_movie_list1", popular_movie_list
		return HttpResponse("You're in GET request.")
	else:
		print "Its a POST request"
		return HttpResponse("You're in POST request.")

#Dump into User Json Files
#POST REQUEST
def popularmovies(lid_dict):
	print "Inside popularmovies"
        for lid, data in lid_dict.iteritems():
            popular_movie_list=data['popular_movie_list']
        print "popular_movie_list", popular_movie_list
        return (json.dumps(popular_movie_list).encode('utf-8'))

def write_into_file(write_file_path, file_content):
    try:
        with open(write_file_path, 'w') as fp:
            fp.write(file_content)
    except Exception, e:
        raise Exception("Error : [%s] in writing zipped file to %s", e, write_file_path)

def extract_zipped_files(write_file_path, extract_file_path):
    try:
        zip_ref = zipfile.ZipFile(write_file_path, 'r')
        zip_ref.extractall(extract_file_path)
        zip_ref.close()
    except Exception, e:
        raise Exception("Error : [%s] in extracting zipped file : %s to %s", e, write_file_path, extract_file_path)
	
def uploadjsonfiles(request):	
    try:
        write_file_path = "/home/ec2-user/a.zip"
        extract_file_path = "/home/ec2-user/PinutJsonFiles"
	print "Inside uploadjsonfiles"
	if request.method == "POST":
            file_content=request.body;
            write_into_file(write_file_path, file_content)
            extract_zipped_files(write_file_path, extract_file_path)
        return HttpResponse(status=200)
    except Exception, e:
        print "Exception : %s", e
        return HttpResponse(status=500)
        
