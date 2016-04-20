#!/user/bin/python

import sys
import ConfigParser
import logging


class DB_Config(object):
    def __init__(self):
        self.db_config_file = "/home/ec2-user/DjangoCloudCode/pinutcloud/conf/db_info.cfg"

    def add_db_info(self, section, db_name, db_username, db_password):
        configParser = ConfigParser.RawConfigParser()
        configParser.add_section(section)
        configParser.set(section, "db_name", db_name)
        configParser.set(section, "db_username", db_username)
        configParser.set(section, "db_password", db_password)

        file_ptr = open(self.db_config_file, "a+")
        configParser.write(file_ptr)
        file_ptr.close()

    def get_db_info(self, section, prefix = ""):
        configParser = ConfigParser.RawConfigParser()
        configParser.read(self.db_config_file)
        db_info = {}
        db_info['db_host'] = configParser.get(section, "db_host")
        db_info['db_name'] = prefix + configParser.get(section, "db_name")
        db_info['db_username'] = configParser.get(section, "db_username")
        db_info['db_password'] = configParser.get(section, "db_password")
        try:
            db_info['db_port'] = configParser.get(section, "db_port")
        except Exception, e:
            print "Exception:",e
            logging.exception ('Exception %s', e)
            db_info['db_port'] = 5432
        return db_info

    def get_data_db_info(self, section):
        configParser = ConfigParser.RawConfigParser()
        configParser.read(self.db_config_file)
        db_info = {}
        db_info['db_name'] = configParser.get(section, "db_name")
        db_info['hostname'] = configParser.get(section, "hostname")
        db_info['port'] = configParser.get(section, "port")

        return db_info

if __name__ == '__main__':
    if len(sys.argv) == 5:
        section = sys.argv[1]
        db_name = sys.argv[2]
        db_username = sys.argv[3]
        db_password = sys.argv[4]

        db_config = DB_Config()
        db_config.add_db_info(section, db_name, db_username, db_password)
