from django.conf import settings
import os

PATH="/home/ec2-user"
PINUT_JSON_FILE_DIR = PATH + "/" + "PinutJsonFiles"
PROCESSED_JSON_FILE_DIR = PATH + "/" + "ProcessedJsonFiles"
PINUT_USER_INTRO_FILE_PATH = PINUT_JSON_FILE_DIR + "/" + "PinutUserIntroFiles"
PINUT_USER_FILE_PATH = PINUT_JSON_FILE_DIR + "/" + "PinutUserFiles"
PINUT_CONNECTION_FILE_PATH = PINUT_JSON_FILE_DIR + "/" + "PinutConnectionFiles"
PINUT_FEEDBACK_FILE_PATH = PINUT_JSON_FILE_DIR + "/" + "PinutFeedbackFiles"


def create_directory(path):
        try:
                if not os.path.exists(path):
                        print "Creating directory : ", path
                        os.makedirs(path)
        except Exception,e:
                print "Exception : %s Not able to create directory : %s" % (e,path)


create_directory(PINUT_JSON_FILE_DIR)
create_directory(PROCESSED_JSON_FILE_DIR)
create_directory(PINUT_USER_INTRO_FILE_PATH)
create_directory(PINUT_USER_FILE_PATH)
create_directory(PINUT_CONNECTION_FILE_PATH)
create_directory(PINUT_FEEDBACK_FILE_PATH)

