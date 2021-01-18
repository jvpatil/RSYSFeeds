# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 18:31:31 2018
@author: jaganpat
"""
from __future__ import print_function
import logging
import configparser
import os
import sys
from configparser import SafeConfigParser, DuplicateOptionError, DuplicateSectionError
# from ConfigParser import SafeConfigParser

from datetime import datetime
import cx_Oracle
# propertyFile = "D:\\PyScripts\\python.properties"

class Setup():

    def __init__(self, test_class_name):
        self.curs = None
        self.update_os()
        # self.delete_files()
        # logging.basicConfig(filename='Feeds.log', filemode='w', format='%(process)d-%(levelname)s-%(message)s',level=logging.DEBUG)
        # logging.basicConfig(format='%(funcName)s.%(levelname)s : %(message)s',level=logging.INFO)
        # logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')

    ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print("Root Directory :", ROOT_DIR)
    propertyFile = ROOT_DIR + "/python.properties"
    print("Property File", propertyFile)

    resultFilePath = ROOT_DIR + "/Result"
    testfilespath = ROOT_DIR + "/InputFiles"

    def delete_files(self):
        path = self.resultFilePath
        for file in os.scandir(path):
            if file.name.endswith(".csv"):
                os.unlink(file.path)
                print("Deleted old file :", file.name,"  from ",path )

    def update_os(self):
        # os.environ["PATH"] = "C:\ORACLE_CLIENT\instantclient-basic-windows.x64-11.2.0.4.0\;" + os.environ["PATH"]
        # os.environ["PATH"] = "/opt/oracle/instantclient_19_3" + os.environ["PATH"]
        cx_Oracle.init_oracle_client(lib_dir="/opt/oracle/instantclient_19_3")
        # os.environ["PATH"] = "/opt/oracle/instantclient_19_3" + os.environ["PATH"]
        # print(os.environ["PATH"])
        # os.environ["NLS_LANG"] = ".UTF8"

    start = datetime.now()
    def load_properties(filepath, sep, comment_char):
        props = {}
        with open(filepath, "rt") as f:
            for line in f:
                l = line.strip()
                if l and not l.startswith(comment_char):
                    key_value = l.split(sep)
                    key = key_value[0].strip()
                    value = sep.join(key_value[1:]).strip().strip('"')
                    props[key] = value
        return props

    prop = load_properties(propertyFile, '=', '#')

    def init_db_connection(self,accountName): #Make sure acccount details are provided in properties file.
        # account_id = self.prop[accountName+"AccID"]
        # print("\nConnecting to ...",(self.prop[accountName+"User"]).upper() +"( Account ID :",self.prop[accountName+"AccID"],") --- " + accountName )
        connection_details = self.prop[accountName + "User"] + "/" + self.prop[accountName + "Pass"] + "@" + self.prop[accountName + "URL"]
        # self.dbcon = cx_Oracle.connect(self.prop[accountName+"User"] + "/" + self.prop[accountName+"Pass"] + "@" + self.prop[accountName+"URL"])
        try:
            # with cx_Oracle.connect(connection_details) as dbcon:
            #     self.curs = dbcon.cursor()
            #     return self.curs
            dbcon =  cx_Oracle.connect(connection_details)
            curs = dbcon.cursor()
            return curs

        except Exception as e:
            print("\nError in Connecting to DB using ",accountName.upper())
            print("Error Type : " ,type(e).__name__)
            print("Error : " ,e , "\nExiting the execution")
            exit(1)

    def start_db_connection(self,pod, accountSchema):
        shema_details = self.get_db_connection_details(pod,accountSchema)
        try:
            dbcon =  cx_Oracle.connect(shema_details)
            curs = dbcon.cursor()
            return curs
        except Exception as e:
            print("\nError in Connecting to DB using ",accountSchema.upper())
            print("Error Type : " ,type(e).__name__)
            print("Error : " ,e , "\nExiting the execution")
            exit(1)

    def get_db_connection_details(self,pod,accountSchema):
        file_path = self.ROOT_DIR +"/ConfigFiles/dbconnections.ini"
        user_name = self.read_config_file(file_path,section=pod,option=accountSchema+"User")
        password = self.read_config_file(file_path, section=pod, option=accountSchema + "Pass")
        url = self.read_config_file(file_path, section=pod, option=accountSchema + "URL")
        connection_details = user_name + "/" + password + "@" + url
        return connection_details

    def read_config_file(self,file_path,section,option):
        Config = configparser.ConfigParser()
        Config.read(file_path)
        if Config.has_section(section):
            if Config.has_option(section,option):
                config_value = Config.get(section,option)
                return config_value
            else:
                print("--***** Option '"+ option +"' is not present in file "+ file_path +" *****--")
        else:
            print("--***** Sectio '" + section + "' is not present in file " + file_path + " *****--")

    def close_db_connection(self,curs):
        if  not curs.close():
            return
            # print("\nClosed the connection to ",curs)
        # return

    finish = datetime.now()
    timeTaken = (finish - start).total_seconds()

    def get_run_time(self,timeTaken=timeTaken):
        # timeTaken+=60
        if timeTaken > 3600:
            Hour, R = divmod(int(timeTaken), 3600)  # Qoutient is stored in Hour and Remainder in R
            Minutes, Seconds = divmod(R, 60)
            totalTime = str(Hour) + " hour" + "," + str(Minutes) + " Min" + "," + str(Seconds) + " Sec"
        elif timeTaken > 60:
            Minutes, Seconds = divmod(int(timeTaken), 60)  # Qoutient is stored in Minutes and Remainder in Seconds
            totalTime = str(Minutes) + " Min" + "," + str(Seconds) + " Sec"
        else:
            totalTime = str(int(timeTaken)) + " Sec"
        return totalTime



