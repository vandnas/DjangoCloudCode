#!/usr/bin/python

# This file provides an interface between Mongo DB and python program

import logging
import db_config
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo import MongoClient, ASCENDING
from bson import Code
from bson.son import SON
import subprocess
import shutil
import os


class MongoError(Exception):

    def __init__(self, err_text):
        self.value = err_text

    def __str__(self):
        return self.value


class MongoDBAnalytics:
    client = None
    db_name_prefix = None

    def __init__(self):
        self.get_connection()

    def __del__(self):
        self.close_connection()

    def get_connection(self):
        if self.client:
            pass
        else:
            try:
                db_config_obj = db_config.DB_Config()
                db_info = db_config_obj.get_data_db_info("data_db_info")
                self.client = MongoClient(db_info['hostname'], int(db_info['port']))
                self.db_name_prefix = db_info['db_name']
            except ConnectionFailure, e:
                logging.exception("Error : %s", e)
                raise MongoError(e)

    def close_connection(self):
        if self.client:
            self.client.close()
        self.client = None
        self.db_name_prefix = None

    # This function is used to ensure index on a collection
    # key_list : List of keys to be used for indexing
    # Keys will be indexed in ascending order
    # Keys are unique unless specified otherwise
    def mongo_ensure_index(self, cname, collection, key_list, unique=True, indexorder=ASCENDING):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            key_tuple_list=[]
            for key in key_list:
                key_tuple_list.append((key, indexorder))

            result = coll.ensure_index(key_tuple_list, unique=unique)
            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Returns a document in a form of a dictionary if present, else returns None
    def mongo_select_one(self, cname, collection, query=None):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            result = coll.find_one(query)
            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Returns all collections present for a database that contain as specific pattern in name
    def mongo_collection_names(self, cname, pattern=None):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]
            collections = db.collection_names()
            result = []

            if pattern:
                for collection in collections:
                    if pattern in collection:
                        result.append(collection)
            else:
                result = collections

            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Counts the number of records in a collection and returns it.
    def mongo_count(self, cname, collection):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            result = coll.count()
            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Returns documents in a form of a list of dictionary if present, else returns None
    def mongo_select(self, cname, collection, query):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            result = coll.find(query)
            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Returns documents in a form of a sorted list of dictionary if present, else returns None
    def mongo_select_sorted(self, cname, collection, query, sort_query):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            result = coll.find(query).sort(sort_query)
            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    def mongo_find_distinct(self, cname, collection, param):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            result = coll.distinct(param)
            return result

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)
    
    # Updates a document
    # Upsert : if query is satisfied, it updates a document else inserts new document (upsert is by default true)
    # Multi : Updates multiple documents which match the query (multi is by defaut false)
    def mongo_update(self, cname, collection, query, update, do_upsert=True, do_multi=False):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            coll.update(query, update, upsert=do_upsert, multi=do_multi)

        except DuplicateKeyError:
            logging.debug("duplicate key error, ignoring for customer[%s], collection:%s, query:%s, update:%s", cname, collection, query, update)
        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Insert new document in a collection
    def mongo_insert(self, cname, collection, query):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            coll.insert(query)

        except DuplicateKeyError:
            pass
        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Bulk insert documents in a collection, ignore errors (such as duplicate key errors)
    def mongo_bulk_insert(self, cname, collection, query):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            coll.insert(query, continue_on_error=True)

        except DuplicateKeyError:
            logging.debug("duplicate key error, ignoring for customer[%s], collection:%s, query:%s", cname, collection, query)
        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Delete document(s) which match the query
    def mongo_delete(self, cname, collection, query):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            coll.remove(query)

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Performs map reduce for a particular collection
    # Mapper function, reducer function, scope function are arguments
    def mongo_map_reduce(self, cname, collection, mapper_function, reduce_function, output_collection, scope_dict, query=None):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            map_func = Code(mapper_function)
            reduce_func = Code(reduce_function)

            coll.map_reduce(map_func, reduce_func, out=SON([("replace", output_collection)]), scope=scope_dict, query=query)

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Performs map reduce finalize for a particular collection
    # Mapper function, reducer function, finalize function, scope function are arguments
    def mongo_map_reduce_finalize(self, cname, collection, mapper_function, reduce_function, finalize_function ,output_collection, scope_dict, query=None):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            map_func = Code(mapper_function)
            reduce_func = Code(reduce_function)
            finalize_func = Code(finalize_function)

            coll.map_reduce(map_func, reduce_func, finalize=finalize_func, out=SON([("replace", output_collection)]), scope=scope_dict, query=query)

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Drop collection
    def mongo_drop_collection(self, cname, collection):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]

            coll = db[collection]

            coll.drop()

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Drop database
    def mongo_drop_database(self, cname):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            client.drop_database(db_name)

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Copy database
    def mongo_copy_database(self, source, dest):
        client = self.client
        try:
            db_name_source = self.db_name_prefix + "_" + str(source)
            db_name_dest = self.db_name_prefix + "_" + str(dest)
            client.copy_database(db_name_source, db_name_dest)

        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)

    # Merge two mongo tables using mongodump and mongorestore
    def mongo_merge_tables(self, cname, source_coll, dest_coll):
        temp_file_path1 = ""
        try:
            # Mongodump followed by mongorestore to copy data
            temp_file_path1 = "/tmp/" + cname + "_" + source_coll + "_dump"
            ret_code = subprocess.call(['mongodump', '--db', "analytics_data_" + cname, '--collection', source_coll, '--out', temp_file_path1])
            if ret_code != 0:
                logging.error("Mongo dump of temporary collection[%s] failed for customer[%s]", source_coll, cname)
                raise Exception("Mongo dump of temporary collection[%s] failed for customer[%s]" % (source_coll, cname))

            temp_file_path2 = temp_file_path1 + "/" + "analytics_data_" + cname + "/" + source_coll + ".bson"
            ret_code = subprocess.call(['mongorestore', '--db', "analytics_data_" + cname, '--collection', dest_coll, temp_file_path2])
            if ret_code != 0:
                logging.error("Mongo restore of temporary collection[%s] failed for customer[%s]", source_coll, cname)
                raise Exception("Mongo restore of temporary collection[%s] failed for customer[%s]" % (source_coll, cname))

        except Exception, e:
            logging.exception("Error:%s", e)
            raise MongoError(e)

        finally:
            if os.path.exists(temp_file_path1):
                shutil.rmtree(temp_file_path1)

    # mongoexport utility - export data to a csv file
    # Fields is a comma separated string (passed as is in the mongoexport command)
    def mongo_export(self, cname, coll, filename, fields, type_of_output="csv"):
        try:
            ret_code = subprocess.call(['mongoexport', '--db', "analytics_data_" + cname, '--collection', coll, "--out", filename, "--type", type_of_output, "--fields", fields])
            if ret_code != 0:
                logging.error("Mongoexport of temporary collection[%s] failed for customer[%s], filename[%s]", coll, cname, filename)
                raise Exception("Mongoexport of temporary collection[%s] failed for customer[%s], filename[%s]" % (coll, cname, filename))
        except Exception, e:
            logging.exception("Error:%s", e)
            raise MongoError(e)

    #selects specified range of documents
    #similar to limit-offset query in sql
    def mongo_select_limited_documents(self, cname, count, limit1, collection):
        client = self.client
        try:
            db_name = self.db_name_prefix + "_" + str(cname)
            db = client[db_name]
            coll = db[collection]
            list_of_records=coll.find().skip(count).limit(limit1)
            return list_of_records
        except Exception, e:
            logging.exception("Error : %s", e)
            raise MongoError(e)
