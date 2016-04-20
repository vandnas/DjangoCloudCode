#!/usr/bin/python

import os
import time
import sys
import Queue
import subprocess

PATH="/home/pi/Digital_Signage"
MOVIES_FOLDER = "movies"
ADS_FOLDER = "ads"
MOVIES_PATH = PATH + "/" + MOVIES_FOLDER
ADS_PATH = PATH + "/" + ADS_FOLDER
PIPE_FILE = "test"
PIPE_FILE_PATH = PATH + "/" + PIPE_FILE

#Add queue
q_ads = Queue.Queue(maxsize=0)

# Movie queue
q_movies = Queue.Queue(maxsize=0)


def put_ads_in_queue(ADS_PATH):
    if q_ads.empty():
        ads_list = os.listdir(ADS_PATH)
        for ad in ads_list:
	    print "ad in queue",ad
            q_ads.put(ADS_PATH + "/" + ad)
    return q_ads


def put_movies_in_queue(MOVIES_PATH):
    if q_movies.empty():
        movies_list = os.listdir(MOVIES_PATH)
        for movie in movies_list:
            q_movies.put(MOVIES_PATH + "/" + movie)
    return q_movies


def play_content_in_player(content_type, content_name, PIPE_FILE_PATH):
	if content_type == "ad":
		print "in ad"
        	pipe = subprocess.Popen(['omxplayer', '-p', '-o', 'hdmi', content_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	else:
		print "in movie"
        	pipe = subprocess.Popen(['omxplayer', '-p', '-o', 'hdmi', content_name, '<', PIPE_FILE_PATH, '&'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = pipe.communicate()
	print "out",out
	print "err",err
        if err is not "":
            print "Error :%s in playing content : %s " % (err, content_name)
            raise Exception()

def make_player_ready_to_accept_commands(PIPE_FILE_PATH):
        pipe = subprocess.Popen(['echo', '.', '>', PIPE_FILE_PATH], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = pipe.communicate()
	print "out",out
	print "err",err
        if err is not "":
            print "Error :%s in making player ready to accept commands " % err
            raise Exception()


def control_player(PIPE_FILE_PATH):
        pipe = subprocess.Popen(['echo', '-n', 'p', '>', PIPE_FILE_PATH], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = pipe.communicate()
	print "out",out
	print "err",err
        if err is not "":
            print "Error :%s Unable to pause/unpause player " % err
            raise Exception()


def create_pipe_file():
	try:
		# Path to be created
		os.mkfifo(PIPE_FILE_PATH, 0644 )
		return PIPE_FILE_PATH
	except OSError, e:
                if e.errno == 17:
                    print "PIPE FILE ALREADY EXIST...Deleting and Re-Creating it again"
		    os.remove(PIPE_FILE_PATH)
		    os.mkfifo(PIPE_FILE_PATH, 0644 )
		    return PIPE_FILE_PATH
                else:
                    print "Error in creating pipe file : %s" % e
                    raise
	except Exception, e:
		print "Exception : %s in creating pipe file", e


# Function to enable sharding for customer and shard it's collections
def play_one_in_list(movie):
    try:
	PIPE_FILE_PATH = create_pipe_file()
        q_ads=put_ads_in_queue(ADS_PATH)
        print "***  MOVIE ****" ,movie
	play_content_in_player("movie", movie, PIPE_FILE_PATH)
	make_player_ready_to_accept_commands(PIPE_FILE_PATH)
        time.sleep(10)
	control_player(PIPE_FILE_PATH)
        ad=q_ads.get()
        print "ad from queue", ad
	play_content_in_player("ad", ad, PIPE_FILE_PATH)
	control_player(PIPE_FILE_PATH)

    except Exception, e:
        print "Exception: [%s], Unable to play movie : [%s]" % (e,movie) 
        raise

# Reads postgreSQL table and enables sharding for all customer ids in postgreSQL table
def play_all_in_list():
    try:
        q_movies = put_movies_in_queue(MOVIES_PATH)
        while q_movies.qsize() > 0 :
            movie=q_movies.get()
            play_one_in_list(movie)
    except Exception, e:
        print "Exception: [%s], Unable to play all the movies in the list" % e 
        raise

def play_all_repeatedly_in_list():
    try:
        while True:
            play_all_in_list()
    except Exception,e:
        print "Exception: [%s], Unable to repeat play list" % e
        raise

def check_if_movie_is_valid(arg):
        movies_list = os.listdir(MOVIES_PATH)
        if arg not in movies_list:
		raise Exception ("Invalid Movie .. Exiting")
		

def play_list(arg):
    try:
        if arg == "repeat":
            play_all_repeatedly_in_list()
        elif arg == "all":
            play_all_in_list()
        else: 
            # Movie name
            check_if_movie_is_valid(arg)
            play_one_in_list(arg)
    except Exception, e:
        print "Exception : [%s], Unable to play list with parameter:[%s]" % (e, arg)
        raise


if __name__ == "__main__":
    try:


        print "Command line argument:" , sys.argv[1]
        arg = str(sys.argv[1])
        print arg
        play_list(arg)
    except Exception, e:
        print "Exception[%s]" % e
        raise
