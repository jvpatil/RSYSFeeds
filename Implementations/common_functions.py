import csv

import pytz

from Implementations.setup import Setup
from Implementations.db_fuctions import DBFunctions
from Implementations.device_details import DeviceDetails
from datetime import datetime
import sys
import os
import re
import traceback
from collections import defaultdict
import requests
from bs4 import BeautifulSoup

import lxml


class CommonFunctions(Setup):
    input_file_path = Setup.testfilespath
    result_file_path = Setup.resultFilePath
    run_time = datetime.now().strftime("%d%b%Y_%H.%M.%S")  # current time in ddmmyyyy_hh24mmss
    report = "Result_" + str(run_time) + ""

    def get_index(self, search_column, ced_file):
        index_of_search_column, index_of_event_stored_dt = None, None
        try:
            with open(os.path.join(self.input_file_path, ced_file), 'r') as f:
                content = f.readlines()
            for column in content[:1]:
                column = column.strip().replace('\"', '')
                list_of_columns = re.split(',', column)
            index_of_search_column = list_of_columns.index(search_column)
            index_of_event_stored_dt = list_of_columns.index(
                "EVENT_STORED_DT")  # print("Index of Search COlumn is :", index_of_search_column, "and Index of Event Stored Date is :",
            # index_of_event_stored_dt)
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n")
            print(traceback.format_exc())

        return index_of_search_column, index_of_event_stored_dt

    def get_account_id(self, ced_file):
        account_id = re.split(r"_", ced_file)  # Extract Account ID from the filename
        account_id = account_id[0].strip('_')
        return account_id

    def get_search_column(self, cedFile):

        search_key = "LAUNCH_ID"
        if re.match(r'[0-9]+_' + 'OPT', cedFile):
            search_column = 'RIID'
        elif re.match(r'[0-9]+_' + 'SMS_OPT', cedFile):
            search_column = 'RIID'
        elif re.match(r'[0-9]+_' + 'PUSH_OPT', cedFile):
            search_column = 'RIID'
        elif re.match(r'[0-9]+_' + 'FORM', cedFile):
            search_column = 'FORM_ID'
        elif re.match(r'[0-9]+_' + 'FORM_STATE', cedFile):
            search_column = 'FORM_ID'
        elif re.match(r'[0-9]+_' + 'PROGRAM', cedFile):
            search_column = 'PROGRAM_ID'
        elif re.match(r'[0-9]+_' + 'PROGRAM_STATE', cedFile):
            search_column = 'PROGRAM_ID'
        elif re.match(r'[0-9]+_' + 'HOLDOUT', cedFile):
            search_column = 'PROGRAM_ID'
        elif re.match(r'[0-9]+_' + 'PUSH_UNINSTALL', cedFile):
            search_column = 'RIID'
        else:
            search_column = search_key

        return search_column

    def find_files(self):
        count = 0
        files = None
        try:
            files = os.listdir(self.input_file_path)
            for fname in range(len(files)):
                count += 1
            print("\nThere are total", count, "files to process:")
            # print(*files, sep="\n")
            print('\t', *files, sep="\n")
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n")
            print(traceback.format_exc())

        return files

    def compare_counts(self, cedFileName, searchID, dCountFromCED, dCountFromDB, deventsToProcess, deventsProcessed):
        # global account_id
        print("*** Validating count for file :: ", cedFileName, " ***")
        account_id = re.split(r"_", cedFileName)  # Extract Account ID from the filename
        account_id = account_id[0].strip('_')
        resultFile = self.result_file_path + "\\" + account_id + "_FeedsData_CompareResult_" + self.run_time + ".csv"

        header = ["EVENT_TYPE", "FILE_NAME", "KEY", "ID", "COUNT_FROM_CED", "COUNT_FROM_DB", "EventsToBeProcessed", "Eventsalready_processed",
                  "STATUS", "Comments"]

        name = re.split(r"\d+", cedFileName)  # Extract only event name from the filename
        event_type = name[1].strip('_')

        open_file = open(resultFile, "a+")
        writer = csv.writer(open_file, lineterminator="\n", quoting=csv.QUOTE_ALL)
        check_header = open(resultFile, "r").read()
        if check_header == '':
            writer.writerow(header)
        try:
            for id in dCountFromCED:
                ced_count = dCountFromCED[id][0]
                db_count = dCountFromDB[int(id)][0]
                not_processed = deventsToProcess[int(id)][0]
                already_processed = deventsProcessed[int(id)][0]

                if db_count == ced_count:
                    result = "Count for the ID Match"
                    status = "Pass"
                elif db_count != ced_count and not_processed != 0:
                    if type(not_processed) == str:
                        result = "No Data found in " + event_type + " table for " + searchID + " : " + id
                        status = "Fail"
                    else:
                        actual_count = db_count - not_processed
                        if ced_count == actual_count:
                            result = "Count for the ID Match, But there are " + str(not_processed) + " to be processed yet"
                            status = "Pass"
                        # elif ced_count == (db_count-not_processed-already_processed):
                        elif already_processed != 0:
                            result = "Count for the ID does not Match, Seems Few Records are already processed & few are to be processed yet for " \
                                     + event_type + " table"
                            status = "Fail"
                        else:
                            result = "Count for the ID does not Match, Seems Records are Purged for " + event_type + " table"
                            status = "Fail"
                elif db_count != ced_count and already_processed != 0:
                    if type(already_processed) == str:
                        result = "No Data found in " + event_type + " table for " + searchID + " : " + id
                        status = "Fail"
                    else:
                        act_count = db_count - already_processed
                        if ced_count == act_count:
                            result = "Count for the ID does not Match, Looks like " + str(
                                already_processed) + " records are already processed and exported"
                            status = "Fail"
                        else:
                            result = "Count for the ID does not Match, Seems Records are Purged for " + event_type + " table"
                            status = "Fail"
                else:
                    result = "Count for the ID does not Match"
                    status = "Fail"

                writer.writerow([event_type, cedFileName, searchID, id, ced_count, db_count, not_processed, already_processed, status, result])
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n")
            print(traceback.format_exc())

        open_file.close()
        return resultFile

    def is_file_empty(self, ced_file):
        input_file_path = self.testfilespath
        with open(os.path.join(input_file_path, ced_file), 'r') as f:
            content = f.readlines()
        if len(content) != 0:
            return False
        else:
            print("\File ", ced_file, " is empty")
            return True


    def get_headers_from_ced(self, ced_file):
        input_file_path = self.testfilespath
        ced_headers_from_file = defaultdict(list)
        event_type = re.split(r"\d+", ced_file)
        event_type = event_type[1].strip('_')

        with open(os.path.join(input_file_path, ced_file), 'r') as f:
            content = f.readlines()
        if len(content) != 0:
            for header_row in content[:1]:
                col = header_row.strip().replace('\"', '')
                ced_col_names = re.split(';|,|\t|""', col)
                for i in ced_col_names:
                    ced_headers_from_file[event_type].append(i)
            # print("Files are :", CEDFileNames)
            # print("\nColumns from CED are",CEDHeadersFromFile)
        else:
            print("\File ", ced_file, " is empty")
        return ced_headers_from_file

    def get_headers_from_podconfig(self):
        site = "https://interact.qa1.responsys.net/authentication/login/LoginPage"
        BASE_URL = "https://interact-a.qa1.responsys.net/interact/"
        MANAGE_POD = "siteadmin/PodActivityFunctionsAction"
        POD_CONFIG_PAGE = "siteadmin/PodActivityConfigViewAction"

        '''initiate session'''
        session = requests.Session()
        session.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                         'Chrome/44.0.2403.61 Safari/537.36',
                           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.5',
                           'Referer': site}
        '''get login page'''
        resp = session.get(site)
        html = resp.text

        '''get BeautifulSoup object of the html of the login page'''
        soup = BeautifulSoup(html, 'lxml')
        title = soup.find_all('title')[0].text
        if 'service temporarily unavailable' in title.lower():
            print("\n*** Site Under Maintanance - Please Try After Some Time ***".upper())
            exit(1)

        '''scrape login page to get all the needed inputs required for login'''
        data = {}
        form = soup.find('form', {'name': 'loginForm'})
        for field in form.find_all('input'):
            try:
                data[field['name']] = field['value']
            except:
                pass
        data[u'UserName'] = "SYSDM"
        data[u'Password'] = "Welcome1234%"

        '''submit post request with username / password and other needed info'''
        post_resp = session.post('https://interact-a.qa1.responsys.net/authentication/login/LoginAction', data=data)
        post_soup = BeautifulSoup(post_resp.content, 'lxml')

        if post_soup.find_all('title')[0].text == 'Oracle Responsys':
            # print("Login Successfull")
            print("*** Reading CEDs Column headers from PodConfig ***")
            # pc = {"value" :"PodConfig"}
            pcPage = session.post(BASE_URL + POD_CONFIG_PAGE)
            navigate_soup = BeautifulSoup(pcPage.content, 'lxml')
            header_columns_from_podconfig = defaultdict(list)
            header_addon_columns = defaultdict(list)

            table = navigate_soup.find('td', text=re.compile('.EventConnect')).find_parent("table")
            for row in table.find_all("tr"):
                event_name = row.text.split('.')[0]
                for cell in row.find_all("td"):
                    rt = row.text
                    if ".EventConnect" in row.text:
                        if "." not in cell.text:  # reading and appending event's Standard/fixed columns
                            col_names = cell.text.split(',')
                            for i in range(len(col_names)):
                                col_name = col_names[i]
                                header_columns_from_podconfig[event_name].append(col_name)
            session.close()
            return header_columns_from_podconfig
        elif 'password' in (post_soup.find_all('title')[0].text).lower():
            print("Password Expired for user ", data[u'UserName'] ," Please reset and try again")
        else:
            print('\n***LOGIN TO SYSADMIN FAILED - UNABLE TO READ HEADERS FROM THE PodConfig.ini*** ')
            exit()

    """ ADD ON COLUMNS ARE NOT REQUIRED TO APPEND TO PODCONFIG DEFAULT COLUMNS.IF ADD-ON COLUMNS ARE SELECTED IN ACCOUNTS FEED SETTING, IT WILL BE REFLECTING IN SETTINS.INI. 
    While validating columns, we either have to check settings.ini or podconfig.
    first check in settings.ini, if EVENT TYPE present then the columns mentioned in there are the final columns, else podconfig default 
    columns (without addons) are final columns for ced file
    An Entry for for event will be created in settings.ini if atleast 1 add on is selected through SYSADMIN-Manage Event Data (Feeds 
    setting page)"""

            #         if ".AddOn" in row.text:  # reading and appending event's add on columns.
            #             if "." not in cell.text:
            #                 col_names = cell.text.split(',')
            #                 for i in range(len(col_names)):
            #                     col_name = col_names[i]
            #                     header_addon_columns[event_name].append(col_name)
            #
            # for event in header_addon_columns:  # appending add on columns (at the end of standard columns) for the
            #     for j in range(len(header_addon_columns[event])):
            #         header_columns_from_podconfig[event].append(header_addon_columns[event][j])
            # session.close()
            # return header_columns_from_podconfig
        # else:
        #     print('\n***LOGIN TO SYSADMIN FAILED - UNABLE TO READ HEADERS FROM THE PodConfig.ini*** ')
        #     exit()

    def write_headers_to_file(self, account_id, header_columns, passed_param_name):
        if "ced_columns_from_podconfig" == passed_param_name:
            filename = "CEDHeaders_FromPodConfig_" + self.run_time + ".csv"
        elif "ced_columns_from_db" == passed_param_name:
            filename = account_id + "_CEDHeaders_FromSysAdminDB_" + self.run_time + ".csv"
        elif "ced_columns_from_file" == passed_param_name:
            filename = account_id + "_CEDHeaders_FromFile_" + self.run_time + ".csv"
        else:
            filename = "ResultFile_" + self.run_time + ".csv"

        # result_file_name = self.result_file_path+"\\"+account_id+"_CEDHeaders_FromSysAdminDB_"+self.run_time+".csv"
        result_file_name = self.result_file_path + "\\" + filename
        with open(result_file_name, "a+") as f:
            writer = csv.writer(f, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_ALL)

            prev_data = open(result_file_name, "r").read()
            header = ["Event Types", "Column Name"]
            # Add a header only if the fname is empty
            if prev_data == '':
                writer.writerow(header)
            # writer.writerow([event_type, Headers[event_type]])
            for event_type in header_columns.keys():
                writer.writerows([[event_type] + header_columns[event_type]])
        # writerow takes 1-dimensional data (one row), and writerows takes 2-dimensional data (multiple rows).
        result_file = "***INFO : SAVED A FILE (" + filename.upper() + ") WITH ALL EVENTS & THIER HEADERS*****"
        return result_file

    def get_custom_properties(self, curs, account_name):
        print("*** Reading Custom Column for the account ", account_name.upper() , " ***")
        email_column_ids_sorted = None
        sms_column_ids_sorted = None
        query_for_email_columns = "SELECT COLUMN_ID, COLUMN_NAME FROM " + account_name + "_CUST.CUSTOM_EVENT_COLUMN"
        query_for_sms_columns = "SELECT COLUMN_ID, COLUMN_NAME FROM " + account_name + "_CUST.SMS_CUSTOM_EVENT_COLUMN"
        # queries= [query_for_email_columns,query_for_sms_columns]

        email_custom_columns = defaultdict(list)
        sms_custom_columns = defaultdict(list)

        curs.execute(query_for_email_columns)
        email_query_result = curs.fetchall()
        column_id = [i[0] for i in email_query_result]
        for i in range(len(email_query_result)):
            email_custom_columns[int(column_id[i])] = (email_query_result[i][1])
            # provided [1], to append column name with 'i'th ID
            # & 'i'th value in 2nd column of query result.
            # index of NAME column (from queryResult) is 1 i.e 2nd column,
            email_column_ids_sorted = sorted(email_custom_columns)

        # curs.execute(query_for_email_columns)
        # email_query_result = curs.fetchall()
        # column_id = [i[0] for i in email_query_result]

        curs.execute(query_for_sms_columns)
        sms_query_result = curs.fetchall()
        column_id = [i[0] for i in sms_query_result]
        for i in range(len(sms_query_result)):
            # print("ID is",columnID[i])
            sms_custom_columns[int(column_id[i])] = (sms_query_result[i][1])
            sms_column_ids_sorted = sorted(sms_custom_columns)
        return email_column_ids_sorted, email_custom_columns, sms_column_ids_sorted, sms_custom_columns

    def validate_columns_and_save_result(self, account_name, acc_id, file_name, ced_columns_from_file, ced_columns_from_db, built_in_headers_from_db,
                                         ced_columns_from_podconfig, email_custom_columns, sms_custom_columns):
        global account_id, column_presence_status, custom_column_in_ced, status, column_in_ced, short_status, built_in_column_name
        # account_id = self.prop[account_name + "CustAccID"]
        check_for_duplicate_column_index,index_of_present_custom_column=0,0
        account_id = acc_id
        event_name = re.split(r"\d+", file_name)
        event_name = event_name[1].strip('_')

        if event_name in ced_columns_from_db:
            ced_headers_from_db = ced_columns_from_db
        else:
            ced_headers_from_db = ced_columns_from_podconfig

        index_Of_custom_properties = len(ced_headers_from_db[event_name])
        if event_name in ced_headers_from_db:
            if "CUSTOM_PROPERTIES" in ced_headers_from_db[event_name]:
                index_Of_custom_properties = ced_headers_from_db[event_name].index("CUSTOM_PROPERTIES")
                # index_Of_custom_properties = ced_headers_from_db[event_name].index("CUSTOM_PROPERTIES")  #uncomment when SysAdmin get columns from DB works

            if 'SMS' in event_name:
                custom_column_ids = sorted(sms_custom_columns.keys())
                custom_column_names = sms_custom_columns
            else:
                custom_column_ids = sorted(email_custom_columns.keys())
                custom_column_names = email_custom_columns

            for each_column_in_db in ced_headers_from_db[event_name]:
                index_of_db_column = ced_headers_from_db[event_name].index(each_column_in_db)

                if index_of_db_column > index_Of_custom_properties:  # if column present after the custom properties column, then compensate the
                    # index for rest of the columns
                    index_of_db_column = index_of_db_column + len(custom_column_ids) - 1

                try:
                    if each_column_in_db in ced_columns_from_file[event_name]:
                        index_of_present_db_column = ced_headers_from_db[event_name].index(each_column_in_db)
                        column_in_ced = ced_columns_from_file[event_name][index_of_present_db_column]
                        column_presence_status = "Column " + each_column_in_db + " is present in CED"
                        short_status = "Present"

                    elif "$" in each_column_in_db:
                        built_in_column_name = built_in_headers_from_db[event_name][0]
                        # index_of_ced_column = ced_columns_from_file[event_name].index(built_in_column_name)
                        if built_in_column_name in ced_columns_from_file[event_name]:
                            index_of_present_db_column = ced_headers_from_db[event_name].index(each_column_in_db)
                            column_in_ced = ced_columns_from_file[event_name][index_of_present_db_column]
                            column_presence_status = "Column " + built_in_column_name + " (which is configured in headers using built-in " + \
                                                     each_column_in_db + ") is present in CED"
                            short_status = "Present"
                    else:
                        index_of_missing_db_column = ced_headers_from_db[event_name].index(each_column_in_db)
                        column_in_ced = ced_columns_from_file[event_name][index_of_missing_db_column]
                        column_presence_status = "Column " + each_column_in_db + " is missing"
                        short_status = "Missing"

                    if "$" in each_column_in_db:
                        index_of_ced_column = ced_columns_from_file[event_name].index(built_in_column_name)
                    else:
                        index_of_ced_column = ced_columns_from_file[event_name].index(each_column_in_db)

                    if index_of_db_column == index_of_ced_column:
                        column_order_status = "Column is in Order"
                    else:
                        column_order_status = "Column is NOT in Order"

                except Exception as e:
                    index_of_ced_column = "N/A"
                    column_order_status = e
                    column_presence_status = "Column " + each_column_in_db + " is missing"
                    short_status = "Missing"
                    column_in_ced = "N/A"
                    if not "CUSTOM_PROPERTIES" in str(e):
                        print("***There is an Exception :(", e, "),Column", each_column_in_db, "is not present in file", file_name, "***")

                if each_column_in_db == "CUSTOM_PROPERTIES":
                    db_index = ced_headers_from_db[event_name].index("CUSTOM_PROPERTIES")  # capture index of custom properties and
                    # keep increasing till for each of custom column
                    for i in custom_column_ids:  # loop through all the ids (sorted in asc order) of the columns
                        # each_column_in_db = custom_column_names[i][0]  # get each column name using id
                        each_column_in_db = custom_column_names[i]  # get each column name using id
                        try:
                            if each_column_in_db in ced_columns_from_file[event_name]:
                                index_of_present_custom_column = custom_column_names[i].index(each_column_in_db)
                                index_of_present_custom_column += db_index
                                check_for_duplicate_column_index = ced_columns_from_file[event_name].index(each_column_in_db)
                                if check_for_duplicate_column_index < index_of_present_custom_column:
                                    status = "Present"
                                    column_presence_status = "Column " + each_column_in_db + " (from CUSTOM_PROPERTIES) is duplicated and is already present in CED as DEFAULT column"
                                else:
                                    custom_column_in_ced = ced_columns_from_file[event_name][index_of_present_custom_column]
                                    column_presence_status = "Column " + each_column_in_db + " (from CUSTOM_PROPERTIES) is present in CED"
                                    status = "Present"
                            else:
                                index_of_missing_custom_column = custom_column_names[i].index(each_column_in_db)
                                index_of_missing_custom_column += db_index
                                custom_column_in_ced = ced_columns_from_file[event_name][index_of_missing_custom_column]
                                column_presence_status = "Column " + each_column_in_db + " (from CUSTOM_PROPERTIES) is missing"
                                status = "Missing"

                            ced_index = ced_columns_from_file[event_name].index(each_column_in_db)
                            if db_index == ced_index:
                                column_order = "Column is in Order"
                            elif check_for_duplicate_column_index < index_of_present_custom_column:
                                column_order = "Column is NOT in Order Coz, there is already a SYSTEM column with same name"
                            else:
                                column_order = "Column is NOT in Order"
                        except Exception as e:
                            ced_index = "N/A"
                            column_order = e
                            column_presence_status = "Column " + each_column_in_db + " (from CUSTOM_PROPERTIES) is missing"
                            status = "Missing"
                            custom_column_in_ced = "N/A"
                            print("***There is an Exception :(", e, "),Column", each_column_in_db, "is not present in file", file_name, "***")

                        self.writeColumnCheckResult(event_name, file_name, each_column_in_db, column_presence_status, custom_column_in_ced, status,
                                                    db_index, ced_index, column_order)
                        # ced_index+=1  # to compensate for missing column
                        db_index += 1  # increasing index in for loop for next custom column
                    index_of_db_column = db_index
                    continue
                self.writeColumnCheckResult(event_name, file_name, each_column_in_db, column_presence_status, column_in_ced, short_status,
                                            index_of_db_column, index_of_ced_column,
                                            column_order_status)  # self.htmlReport(event_name, cedFileName, each_column_in_db,
                # column_presence_status, short_status, index_of_db_column,)  # self.testHTML(event_name, cedFileName, each_column_in_db,
                # column_presence_status, short_status,index_of_db_column,index_of_ced_column, column_order_status)
        else:
            print("***INFO : Header Details for Event ", event_name, " is not available in DB")
        # print("\nRESULT FILE IS SAVED AT LOCATION :", self.result_file_path)

        return

    def writeColumnCheckResult(self, event_name, file, each_column_in_db, column_presence_status, column_in_ced, short_status, index_of_db_column,
                               index_of_ced_column, column_order_status):

        filename = self.result_file_path + "\\" + account_id + "_CEDHeaders_VerficationResult_" + self.run_time + ".csv"
        f = open(filename, "a+")
        writer = csv.writer(f, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_ALL)

        prev_data = open(filename, "r").read()
        header = ["Event Types", "File Name", "ColumnNames From FeedsSetting", "Present in CED?", "Column in CED", "Result", "DB Column Index",
                  "CED Column Index", "Column Order Result"]
        # Add a header only if the fname is empty
        if prev_data == '':
            writer.writerow(header)
        writer.writerow(
            [event_name, file, each_column_in_db, column_presence_status, column_in_ced, short_status, index_of_db_column, index_of_ced_column,
             column_order_status])
        f.close()
        return

    def validate_data_from_ced_bkp(self, curs, file, search_column, ced_data, index_Of_stored_date, ced_columns_from_file, event_stored_date,
                                   event_type):
        # global eventName
        # eventName = "Test"
        # htmlfile = CommonFunctions.result_file_path + "\\DataValidationReport_" + CommonFunctions.run_time + ".html"
        # hs = open(htmlfile, 'a+')
        # searchID = searchCol

        for id in ced_data:
            # index_Of_date = event_stored_date[id]
            index_Of_date = index_Of_stored_date
            eventTable = DBFunctions.get_event_table(self, event_type)

            queryCOlNames = "SELECT * from " + eventTable + " WHERE rownum=0"  # + searchCol + "='" + str(id) + "'"
            curs.execute(queryCOlNames)
            dbcolnames = [row[0] for row in curs.description]
            totalColumnsInCEDFile = len(ced_columns_from_file[event_type])
            uniqueCEDColumns = []
            filteredColumns = []
            i = 0
            while (i < totalColumnsInCEDFile):
                if ced_columns_from_file[event_type][i] in dbcolnames:
                    filteredColumns.append(ced_columns_from_file[event_type][i])  # removes custom columns (in CED file)
                i += 1
            # when there multiple files of same event, all the columns from all files are populated.so removing duplicates.
            uniqueColumnsFromCED = []
            [uniqueColumnsFromCED.append(item) for item in filteredColumns if
             item not in uniqueColumnsFromCED]  # including columns which are common in CED & DB
            [uniqueCEDColumns.append(item) for item in ced_columns_from_file[event_type] if item not in uniqueCEDColumns]

            # for nxtID in range(len(ced_data)):
            for id in ced_data:
                print("Event Data for ID", id, "is ", event_stored_date[id][0])
                query = "SELECT " + str(",".join(uniqueColumnsFromCED)) + " FROM " + eventTable + " WHERE " + search_column + "='" + str(
                    id) + "' ORDER BY EVENT_STORED_DT"
                # print("Query is :", query)
                curs.execute(query)
                queryResult = curs.fetchall()
                # with open(os.path.join(CommonFunctions.input_file_path, file), 'r') as f:
                #     content = f.readlines()
                rowNum = 0  # for rows in query result
                rn = 0

                trowsForID = len(ced_data[id])
                k = 0

                for row_from_db in queryResult:
                    sDate = ced_data[id][k + index_Of_date]
                    # # storedDateFromCED = datetime.strptime(sDate, '%d-%b-%Y %H:%M:%S')
                    storedDateFromDB = datetime.strftime(row_from_db[index_Of_date], '%d-%b-%Y %H:%M:%S')
                    #
                    # print("\nValidating row_from_db", rowNum, "In file ", file, " for ", str(search_column), ":", id)
                    numberOfColumnsFromDB = range(len(uniqueColumnsFromCED))
                    for j in numberOfColumnsFromDB:  # index for number of columns in a query result

                        print("\nValidating row_from_db", rowNum, "In file ", file, " for ", str(search_column), ":", id)
                        if int(id) in row_from_db and sDate == storedDateFromDB:
                            if k < trowsForID:
                                try:
                                    cedValue = ced_data[id][k]
                                    if cedValue == "":
                                        cedValue = 'None'
                                    dbValue = queryResult[rowNum][j]
                                    format = "%d-%b-%Y %H:%M:%S"  # Date Format in CED File
                                    if type(dbValue) == datetime:
                                        timeFromDB = dbValue
                                        formatTimeInPST = pytz.timezone('US/Pacific').localize(
                                            timeFromDB)  # localizing adds timezone info to the timestamp
                                        # TZ info is required for astimezone()function. Here both CAPTURED & STORED date are converted to PST &
                                        # appended TZ info(-08:00)
                                        convertedToUTC = formatTimeInPST.astimezone(
                                            pytz.timezone('UTC'))  # converts both Captured & Stored date from PSt to UTC

                                        if uniqueColumnsFromCED[j] != 'EVENT_STORED_DT':
                                            if cedValue == str(convertedToUTC.strftime(format)):  # for event_captured_Dt is which is in PST,
                                                # convertedToUTC date is used to compare as CED file has UTC for both Captured & Stored Date
                                                # Status="Data for column "+uniqueColumnsFromCED[j]+" is matching for ID:"+id+".@rowCol:",rowNum,j,
                                                # "Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(convertedToUTC.strftime(format))
                                                Status = uniqueColumnsFromCED[j] + "= Pass"  # print(Status)
                                            else:
                                                Status = "row_from_db=" + str(rowNum) + ",col=" + str(j) + ": Data for column " + \
                                                         uniqueColumnsFromCED[
                                                             j] + " is NOT matching. Data_In_CED:" + cedValue + " & Data_in_DB:" + str(
                                                    convertedToUTC.strftime(format))
                                                print(Status)
                                                # writeHTMLNew(hs, file, file, id, cedValue, dbValue, Status)
                                                CommonFunctions.write_results(file, id, cedValue, dbValue, Status)

                                        elif cedValue == str(timeFromDB.strftime(format)):
                                            # Status= "Data for column "+uniqueColumnsFromCED[j]+" is matching for ID:"+id+".@ rowCol:",rowNum,j,
                                            # " Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(timeFromDB.strftime(format))
                                            Status = uniqueColumnsFromCED[j] + "= Pass"  # print(Status)
                                        else:
                                            Status = "row_from_db=" + str(rowNum) + ",col=" + str(j) + ": Data for column " + uniqueColumnsFromCED[
                                                j] + " is NOT matching. Data_In_CED:" + cedValue + " & Data_in_DB:" + str(timeFromDB.strftime(format))
                                            print(Status)
                                            # writeHTMLNew(hs, file, file, id, cedValue, dbValue, Status)
                                            CommonFunctions.write_results(file, id, cedValue, dbValue, Status)

                                    elif cedValue == str(dbValue):
                                        # Status= "Data for column "+uniqueColumnsFromCED[j]+" is matching for ID:"+id+".@rowCol:",rowNum,j,
                                        # "Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(dbValue)
                                        Status = uniqueColumnsFromCED[j] + "= Pass"
                                        print(Status)

                                    else:
                                        Status = "row_from_db=" + str(rowNum) + ",col=" + str(j) + ": Data for column " + uniqueColumnsFromCED[
                                            j] + " is NOT matching. Data_In_CED:" + cedValue + " & Data_in_DB:" + str(dbValue)
                                        print(Status)
                                        # writeHTMLNew(hs, file, file, id, cedValue, dbValue, Status)
                                        CommonFunctions.write_results(file, id, cedValue, dbValue, Status)

                                # writeHTMLNew(hs, file, file, colData[j], dbValue, Status)
                                except Exception as e:
                                    print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n")
                                    print(traceback.format_exc())
                            else:
                                print("No Match")
                        if j == max(numberOfColumnsFromDB):  # data from ced is stored in sequence, ex if there are
                            # 15 columns in ced, 2nd row from ced will start from 16th column in dictionary
                            totalCEDColumns = len(uniqueCEDColumns)
                            totalDBColumns = len(uniqueColumnsFromCED)
                            CEDnDBColDiff = totalCEDColumns - (totalDBColumns - 1)
                            # if Data[file][nxtID][id][k] == "\n":
                            k += CEDnDBColDiff
                        else:
                            k += 1
                    rowNum += 1

    def covert_acc_tz_to_utc(self, timestamp):
        formatTimeInPST = pytz.timezone('Asia/Calcutta').localize(timestamp)
        # formatTimeInPST = pytz.timezone('US/Pacific').localize(ced_first_event_date)  # localizing adds timezone info to the timestamp
        # TZ info is required for astimezone()function. Here both CAPTURED & STORED date are converted to PST & appended TZ info(-08:00)
        converted_date = formatTimeInPST.astimezone(pytz.timezone('UTC'))  # converts both Captured & Stored date from PSt to UTC
        converted_date = converted_date.strftime("%d-%b-%Y %I:%M:%S")
        return converted_date

    def validate_data_from_ced(self, curs, file, search_column, ced_data, index_Of_stored_date, ced_columns_from_file, event_stored_date, event_type,account_name,email_custom_columns,sms_custom_columns,CEDDatesInAccountTZ):
        event_table = DBFunctions.get_event_table(self, event_type)
        column_name_query = "SELECT * from " +account_name+"_EVENT."+ event_table + " WHERE rownum=0"  # + searchCol + "='" + str(id) + "'"
        curs.execute(column_name_query)

        db_column_names = [row[0] for row in curs.description]
        number_of_columns_in_ced = len(ced_columns_from_file[event_type])

        if 'SMS' in event_table:
            custom_columns = sms_custom_columns
        else:
            custom_columns = email_custom_columns
            
        all_columns_from_ced = []
        ced_columns_exluding_custom_columns = []
        i = 0
        while (i < number_of_columns_in_ced):
            if ced_columns_from_file[event_type][i] in db_column_names:
                ced_columns_exluding_custom_columns.append(ced_columns_from_file[event_type][i])  # To remove custom columns (in CED file)
            i += 1
        # columns_to_be_queried_from_db = []
        columns_to_be_queried_from_db = ced_columns_exluding_custom_columns
        # columns_to_be_queried_from_db = []  # when there multiple files of same event, all the columns from all files are populated.so removing
        # # duplicates.
        # [columns_to_be_queried_from_db.append(item) for item in ced_columns_exluding_custom_columns if
        #  item not in columns_to_be_queried_from_db]  # including columns which are common in CED & DB
        [all_columns_from_ced.append(item) for item in ced_columns_from_file[event_type] if item not in all_columns_from_ced]

        if "USER_AGENT_STRING" in columns_to_be_queried_from_db:
            index_of_user_agent = columns_to_be_queried_from_db.index("USER_AGENT_STRING")
            index_of_riid = columns_to_be_queried_from_db.index("RIID")
            device_ids, device_data = DeviceDetails.get_device_attributes(self, account_name)

        for id in ced_data:
            # print("Event Data for ID", id, "is ", event_stored_date[id][0])
            query = "SELECT " + str(",".join(columns_to_be_queried_from_db)) + " FROM " +account_name+"_EVENT."+ event_table + " WHERE " + search_column + "='" + str(
                id) + "' ORDER BY EVENT_STORED_DT"
            curs.execute(query)
            query_result_for_id = curs.fetchall()
            row_num = 0  # for rows in query result
            number_of_columns_for_id = len(ced_data[id])
            k = 0
            idx_for_ced_col = 0
            row_num_for_ced = 0
            # idx_device_col = 0
            total_cust_columns = 0
            total_CC = 0
            # idx_device_col = 0
            for row_from_db in query_result_for_id:
                # idx_of_event_stored_date_for_ced_file = k + (total_CC) + index_Of_stored_date+idx_device_col
                # event_date_for_record_from_ced = ced_data[id][idx_of_event_stored_date_for_ced_file]
                event_date_for_record_from_ced = ced_data[id][k + index_Of_stored_date]
                event_date_for_record_from_ced = datetime.strptime(event_date_for_record_from_ced, '%d-%b-%Y %H:%M:%S')
                if CEDDatesInAccountTZ:
                    event_date_for_record_from_ced = self.covert_date_utc(event_date_for_record_from_ced)
                # event_date_for_record_from_ced = event_date_for_record_from_ced.strftime("%d-%b-%Y %I:%M:%S")
                event_date_for_record_from_db = datetime.strftime(row_from_db[index_Of_stored_date], '%d-%b-%Y %H:%M:%S')
                number_Of_columns_from_db = len(columns_to_be_queried_from_db)
                idx_device_col = 0

                if int(id) in row_from_db and event_date_for_record_from_ced == event_date_for_record_from_db:
                    print("\nValidating row", row_num_for_ced, "(DB row:", row_num, ")In file ", file, " for ", str(search_column), ":", id)
                    j=0
                    while(j < number_Of_columns_from_db):  # index for number of columns in a query result
                        if ced_columns_from_file[event_type][j] in custom_columns.values():
                            k += 1
                            j += 1
                            total_cust_columns += 1
                            number_Of_columns_from_db += 1
                            continue
                        # total_CC = 3
                        if k < number_of_columns_for_id:
                            try:
                                ced_value = ced_data[id][k]
                                if ced_value == "":
                                    ced_value = 'None'
                                db_value = query_result_for_id[row_num][j-total_cust_columns]
                                format = "%d-%b-%Y %H:%M:%S"  # Date Format in CED File
                                if type(db_value) == datetime:
                                    timeFromDB = db_value
                                    formatTimeInPST = pytz.timezone('US/Pacific').localize(timeFromDB)
                                    convertedToUTC = formatTimeInPST.astimezone(
                                        pytz.timezone('UTC'))  # converts both Captured & Stored date from PSt to UTC

                                    if CEDDatesInAccountTZ:
                                        ced_value = datetime.strptime(ced_value, '%d-%b-%Y %H:%M:%S')
                                        ced_value = self.covert_date_utc(ced_value)
                                    if columns_to_be_queried_from_db[j-total_cust_columns] != 'EVENT_STORED_DT':
                                        if ced_value == str(convertedToUTC.strftime(format)):  # for event_captured_Dt is which is in PST,
                                            Status = columns_to_be_queried_from_db[j-total_cust_columns] + "= Pass"
                                            print(Status)

                                        else:
                                            Status = "row=" + str(row_num) + ",col=" + str(j-total_cust_columns) + ": Data for column " + \
                                                     columns_to_be_queried_from_db[
                                                         j-total_cust_columns] + " is NOT matching. Data_In_CED:" + str(ced_value) + " & Data_in_DB:" + str(
                                                convertedToUTC.strftime(format))
                                            CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                                            print(Status)

                                    elif ced_value == str(timeFromDB.strftime(format)):
                                        Status = columns_to_be_queried_from_db[j-total_cust_columns] + "= Pass"
                                        print(Status)
                                    else:
                                        Status = "row=" + str(row_num) + ",col=" + str(j) + ": Data for column " + columns_to_be_queried_from_db[
                                            j-total_cust_columns] + " is NOT matching. Data_In_CED:" + str(ced_value) + " & Data_in_DB:" + str(timeFromDB.strftime(format))
                                        CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)

                                elif ced_value == str(db_value):
                                    Status = columns_to_be_queried_from_db[j-total_cust_columns] + "= Pass"
                                    print(Status)

                                elif type(db_value) != str and ced_value.isdigit():
                                    if float(ced_value) == float(db_value):  #handling numeric data coz sometimes 10 == 10.0 fails..
                                        Status = columns_to_be_queried_from_db[j-total_cust_columns] + "= Pass"
                                        print(Status)
                                
                                elif all_columns_from_ced[j-total_cust_columns+idx_device_col] in ["BROWSER_TYPE_INFO","BROWSER_INFO","OS_VENDOR_INFO","OPERATING_SYSTEM_INFO","DEVICE_TYPE_INFO"]:
                                    riid = row_from_db[index_of_riid]
                                    # ced_value = ced_data[id][k+idx_device_col]
                                    browser_type, os_vendor, operating_system, device_type, browser = DeviceDetails.get_data(self,device_ids,device_data,riid,event_type)
                                    if all_columns_from_ced[j-total_cust_columns+idx_device_col]=="BROWSER_TYPE_INFO":
                                        CommonFunctions.check_if_data_matches(self,row_num,j-total_cust_columns+idx_device_col,file,all_columns_from_ced[j-total_cust_columns+idx_device_col],id,ced_value,browser_type)
                                        j -= 1
                                        idx_device_col +=1
                                    elif all_columns_from_ced[j-total_cust_columns+idx_device_col] == "BROWSER_INFO":
                                        CommonFunctions.check_if_data_matches(self, row_num, j-total_cust_columns+idx_device_col, file, all_columns_from_ced[j-total_cust_columns+idx_device_col],
                                                                                       id, ced_value, browser)
                                        j -= 1
                                        idx_device_col += 1
                                    elif all_columns_from_ced[j-total_cust_columns+idx_device_col] == "OS_VENDOR_INFO":
                                        CommonFunctions.check_if_data_matches(self, row_num, j-total_cust_columns+idx_device_col, file, all_columns_from_ced[j-total_cust_columns+idx_device_col],
                                                                                       id, ced_value, os_vendor)
                                        j -= 1
                                        idx_device_col += 1
                                    elif all_columns_from_ced[j-total_cust_columns+idx_device_col] == "OPERATING_SYSTEM_INFO":
                                        CommonFunctions.check_if_data_matches(self, row_num, j+idx_device_col, file, all_columns_from_ced[j-total_cust_columns+idx_device_col],
                                                                                       id, ced_value, operating_system)
                                        j -= 1
                                        idx_device_col += 1
                                    elif all_columns_from_ced[j-total_cust_columns+idx_device_col] == "DEVICE_TYPE_INFO":
                                        CommonFunctions.check_if_data_matches(self, row_num, j-total_cust_columns+idx_device_col, file, all_columns_from_ced[j-total_cust_columns+idx_device_col],
                                                                                       id, ced_value, device_type)
                                        j -= 1
                                        idx_device_col += 1
                                else:
                                    Status = "row=" + str(row_num) + ",col=" + str(j) + ": Data for column " + columns_to_be_queried_from_db[
                                        j-total_cust_columns] + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(db_value)
                                    print(Status)
                                    CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                            except Exception as e:
                                print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n")
                                print(traceback.format_exc())
                        else:
                            print("No Match")
                        if j == max(range(number_Of_columns_from_db)):  # data from ced is stored in sequence, ex if there are
                            # 15 columns in ced, 2nd row from ced will start from 16th column in dictionary
                            # number_of_ced_columns = len(all_columns_from_ced)
                            # number_of_db_columns = len(columns_to_be_queried_from_db)
                            # if "USER_AGENT_STRING" in columns_to_be_queried_from_db:
                            #     col_difference = number_of_ced_columns - (number_of_db_columns - 1)-idx_device_col
                            # else:  #coz we have excluded few columns which are present in CED but not in E_RECIPIENT_XXXX tables
                            #     col_difference = number_of_ced_columns - (number_of_db_columns - 1)
                            # # if Data[file][nxtID][id][k] == "\n":
                            # k += col_difference
                            # idx_for_ced_col += col_difference
                            row_num_for_ced += 1
                        else:
                            k += 1
                            # idx_for_ced_col += 1
                        j += 1
                else:
                    # print("\nData is not present for row with ID", int(id), "and RIID ",ced_data[id][index_of_riid], "and date ", event_date_for_record_from_ced)
                    # print("\nData is not present for row with ID", int(id), "and event_stored_date ", event_date_for_record_from_ced)
                    pass
                row_num += 1

    def write_results(self, cedFileName, id, colData, query_result_for_id, Status):
        account_id = re.split(r"_", cedFileName)  # Extract Account ID from the filename
        account_id = account_id[0].strip('_')

        filename = CommonFunctions.result_file_path + "\\" + account_id + "_DataValidationReport_" + CommonFunctions.run_time + ".csv"
        with open(filename, "a+") as f:
            writer = csv.writer(f, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_ALL)

            header = ["FileName", "ID", "DataFromCED", "DataFromDB", "Status"]
            prev_data = open(filename, "r").read()
            # # Add a header only if the fname is empty
            if prev_data == '':
                writer.writerow(header)

            # prev_data = f.read()
            # if prev_data == '':
            #     writer.writerow(header)
            # writer.writerow([event_type, Headers[event_type]])
            writer.writerow([cedFileName, id, colData, query_result_for_id,
                             Status])  # writerow takes 1-dimensional data (one row), and writerows takes 2-dimensional data (multiple rows).

        f.close()
        return


