from django.shortcuts import get_object_or_404, render
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.views import generic
from django.http import HttpResponse
import time
import json
import os
from pinutcloud import GLOBAL_PATH_ANALYTICS

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

                lid_dict = GLOBAL_PATH_ANALYTICS.get_data_per_location(cust_name ,start_date_obj ,end_date_obj)
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
        popular_movie_list=lid_dict['popular_movie_list']
        print "popular_movie_list", popular_movie_list
        print(json.dumps(popular_movie_list).encode('utf-8'))
		
