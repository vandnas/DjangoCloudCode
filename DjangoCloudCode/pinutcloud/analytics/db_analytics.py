#!/usr/bin/python

# This file provides interface between PostgreSQL and python application

import psycopg2
import logging

import db_config
# load the psycopg extras module
import psycopg2.extras



class IntegrityError(Exception):
    def __init__(self, query, args):
        self.value = "Query:"
        self.value += query
        self.value += ", Args:"
        for arg in args:
            self.value += str(arg) + ","
    def __str__(self):
        return self.value

class Query_Error(Exception):
    def __init__(self, query, args):
        self.value = "Query:"
        self.value += query
        self.value += ", Args:"
        for arg in args:
            self.value += str(arg) + ","
    def __str__(self):
        return self.value

class No_Data_Found(Exception):
    def __init__(self, query, args):
        self.value = "Query:"
        self.value += query
        self.value += ", Args:"
        for arg in args:
            self.value += str(arg) + ","
    def __str__(self):
        return self.value

class Connection_Error(Exception):
    def __init__(self, arg):
        self.value = arg
    def __str__(self):
        return self.value

class DB_Analytics:
    con = None
    cur = None

    def __init__(self):
        self.get_connection()

    def __del__(self):
        self.close_connection()

    # Get PostgreSQL Connection
    def get_connection(self):
        if self.con:
            pass
        else:
        # Try to get connection
            try:
                db_config_obj = db_config.DB_Config()
                db_info = db_config_obj.get_db_info("config_db_info")
                self.con = psycopg2.connect(host=db_info['db_host'], database=db_info['db_name'], user=db_info['db_username'], password=db_info['db_password'])
            except psycopg2.DatabaseError, e:
		print "DB_ANALYTICS file:psycopg2.DatabaseError ",e
                logging.exception ('Error %s', e)
                raise Connection_Error("Unable to connect to postgreSQL database")

    # Close PostgreSQL connection
    def close_connection(self):
        try:
            if self.con:
                self.con.close()
        except Exception, e:
            logging.exception (' Exception in closing postgreSQL connection %s', e)
        finally:
            self.con = None

    def start_transaction(self):
        self.cur = self.con.cursor()

    def commit(self):
        self.con.commit()
        self.cur.close()

    def rollback(self):
        self.con.rollback()
        self.cur.close()

    def execute_transaction_query(self, query, *args):
        try:
            self.cur.execute(query, args)

        except psycopg2.IntegrityError,e:
            logging.exception ('Error %s', e)
            raise IntegrityError(query,args)
        except psycopg2.DatabaseError, e:
            logging.exception ('Error %s', e)
            raise Query_Error(query,args)

    def execute_query(self, query, *args):
        con = self.con
        try:
            cur = con.cursor()
            cur.execute(query, args)
            con.commit()
            cur.close()

        except psycopg2.DatabaseError, e:
            logging.exception ('Error %s', e)
            raise Query_Error(query,args)

    def execute_procedure(self, query, *args):
        con = self.con
        try:
            cur = con.cursor()
            cur.callproc(query, args)
            rows = cur.fetchall()
            con.commit()
            cur.close()
            return rows

        except psycopg2.DatabaseError, e:
            logging.exception ('Error %s', e)
            raise Query_Error(query,args)



    # Arguments : connection, query and variable number of arguments(which will eventually be a tuple)
    # Returns : list of fetched rows - Success

    def select_query(self, query, *args):
        con = self.con
        try:
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cur.execute(query, args)
            rows = cur.fetchall()
            if rows == []:
                cur.close()
                raise No_Data_Found(query,args)

            cur.close()
            return rows

        except psycopg2.DatabaseError, e:
            logging.exception ('Error %s', e)

            raise Query_Error(query,args)
