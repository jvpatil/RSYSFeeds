import csv

import pytz

from BaseFunctions.setup import Setup
from BaseFunctions.db_fuctions import DBFunctions
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
    inputFilesPath = Setup.testfilespath
    resultFilePath = Setup.resultFilePath
    runTime = datetime.now().strftime("%d%b%Y_%H.%M.%S")  # current time in ddmmyyyy_hh24mmss
    report = "Result_" + str(runTime) + ""

    def get_index(self, searchColumn, ced_file):
        try:
            with open(os.path.join(self.inputFilesPath, ced_file), 'r') as f:
                content = f.readlines()
            for column in content[:1]:
                column = column.strip().replace('\"', '')
                listOfColumns = re.split(',', column)
            indexOfSearchCol = listOfColumns.index(searchColumn)
            indexOfEventStoredDt = listOfColumns.index("EVENT_STORED_DT")
            # print("Index of Search COlumn is :", indexOfSearchCol, "and Index of Event Stored Date is :",
            # indexOfEventStoredDt)
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
                  "*****\n")
            print(traceback.format_exc())

        return indexOfSearchCol, indexOfEventStoredDt

    def get_account_id(self,ced_file):
        account_id = re.split(r"_", ced_file)  # Extract Account ID from the filename
        account_id = account_id[0].strip('_')
        return account_id

    def get_search_column(self, cedFile):

        searchKey = "LAUNCH_ID"
        if re.match(r'[0-9]+_' + 'OPT', cedFile):
            searchColumn = 'RIID'
        elif re.match(r'[0-9]+_' + 'SMS_OPT', cedFile):
            searchColumn = 'RIID'
        elif re.match(r'[0-9]+_' + 'FORM', cedFile):
            searchColumn = 'FORM_ID'
        elif re.match(r'[0-9]+_' + 'FORM_STATE', cedFile):
            searchColumn = 'FORM_ID'
        elif re.match(r'[0-9]+_' + 'PROGRAM', cedFile):
            searchColumn = 'PROGRAM_ID'
        elif re.match(r'[0-9]+_' + 'PROGRAM_STATE', cedFile):
            searchColumn = 'PROGRAM_ID'
        elif re.match(r'[0-9]+_' + 'HOLDOUT', cedFile):
            searchColumn = 'PROGRAM_ID'
        elif re.match(r'[0-9]+_' + 'PUSH_UNINSTALL', cedFile):
            searchColumn = 'RIID'
        else:
            searchColumn = searchKey

        return searchColumn

    def find_files(self):
        count = 0
        try:
            files = os.listdir(self.inputFilesPath)
            for fname in range(len(files)):
                count += 1
            print("\nThere are total", count, "files to processed:")
            print(*files, sep="\n")
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
                  "*****\n")
            print(traceback.format_exc())

        return files

    def compare_counts(self, cedFileName, searchID, dCountFromCED, dCountFromDB, deventsToProcess,
            deventsProcessed):
        # global account_id
        account_id = re.split(r"_", cedFileName)  # Extract Account ID from the filename
        account_id = account_id[0].strip('_')
        resultFile = self.resultFilePath + "\\" + account_id + "_FeedsData_CompareResult_" + \
                     self.runTime + ".csv"

        header = ["EVENT_TYPE", "FILE_NAME", "KEY", "ID", "COUNT_FROM_CED", "COUNT_FROM_DB", "EventsToBeProcessed",
                  "EventsAlreadyProcessed", "STATUS", "Comments"]

        name = re.split(r"\d+", cedFileName)  # Extract only event name from the filename
        eventType = name[1].strip('_')

        openFile = open(resultFile, "a+")
        writer = csv.writer(openFile, lineterminator="\n", quoting=csv.QUOTE_ALL)
        checkHeader = open(resultFile, "r").read()
        if checkHeader == '':
            writer.writerow(header)
        try:
            for id in dCountFromCED:
                cedCount = dCountFromCED[id][0]
                dbCount = dCountFromDB[int(id)][0]
                NotProcessed = deventsToProcess[int(id)][0]
                alreadyProcessed = deventsProcessed[int(id)][0]

                if dbCount == cedCount:
                    result = "Count for the ID Match"
                    status = "Pass"
                elif dbCount != cedCount and NotProcessed != 0:
                    if type(NotProcessed) == str:
                        result = "No Data found in " + eventType + " table for " + searchID + " : " + id
                        status = "Fail"
                    else:
                        actualCount = dbCount - NotProcessed
                        if cedCount == actualCount:
                            result = "Count for the ID Match, But there are " + str(
                                NotProcessed) + " to be processed yet"
                            status = "Pass"
                        else:
                            result = "Count for the ID does not Match, Seems Records are Purged for " + eventType + " table"
                            status = "Fail"
                elif dbCount != cedCount and alreadyProcessed != 0:
                    if type(alreadyProcessed) == str:
                        result = "No Data found in " + eventType + " table for " + searchID + " : " + id
                        status = "Fail"
                    else:
                        actCount = dbCount - alreadyProcessed
                        if cedCount == actCount:
                            result = "Count for the ID does not Match, Looks like " + str(
                                alreadyProcessed) + " records are already processed and exported"
                            status = "Fail"
                        else:
                            result = "Count for the ID does not Match, Seems Records are Purged for " + eventType + " table"
                            status = "Fail"
                else:
                    result = "Count for the ID does not Match"
                    status = "Fail"

                writer.writerow(
                    [eventType, cedFileName, searchID, id, cedCount, dbCount, NotProcessed, alreadyProcessed, status,
                     result])
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
                  "*****\n")
            print(traceback.format_exc())

        openFile.close()
        return resultFile

    def get_headers_from_ced(self,ced_file):
        inputFilesPath = self.testfilespath
        ced_headers_from_file = defaultdict(list)
        event_type = re.split(r"\d+", ced_file)
        event_type = event_type[1].strip('_')

        with open(os.path.join(inputFilesPath, ced_file), 'r') as f:
            content = f.readlines()

        for header_row in content[:1]:
            col = header_row.strip().replace('\"', '')
            ced_col_names = re.split(';|,|\t|""', col)
            for i in ced_col_names:
                ced_headers_from_file[event_type].append(i)
        # print("Files are :", CEDFileNames)
        # print("\nColumns from CED are",CEDHeadersFromFile)
        return ced_headers_from_file

    def get_headers_from_podconfig(self):
        site = "https://interact.qa1.responsys.net/authentication/login/LoginPage"
        BASE_URL = "https://interact-a.qa1.responsys.net/interact/"
        MANAGE_POD = "siteadmin/PodActivityFunctionsAction"
        POD_CONFIG_PAGE = "siteadmin/PodActivityConfigViewAction"

        '''initiate session'''
        session = requests.Session()
        session.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/44.0.2403.61 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': site
        }
        '''get login page'''
        resp = session.get(site)
        html = resp.text

        '''get BeautifulSoup object of the html of the login page'''
        soup = BeautifulSoup(html, 'lxml')

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
            print("\n***** Reading CEDs Column headers from PodConfig *****")
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

                    if ".AddOn" in row.text:  # reading and appending event's add on columns.
                        if "." not in cell.text:
                            col_names = cell.text.split(',')
                            for i in range(len(col_names)):
                                col_name = col_names[i]
                                header_addon_columns[event_name].append(col_name)

            for event in header_addon_columns:  # appending add on columns (at the end of standard columns) for the
                for j in range(len(header_addon_columns[event])):
                    header_columns_from_podconfig[event].append(header_addon_columns[event][j])
            # write_headers_podconfig(header_columns_from_podconfig)
            session.close()
            return header_columns_from_podconfig
        else:
            print('\n***LOGIN TO SYSADMIN FAILED - UNABLE TO READ HEADERS FROM THE PodConfig.ini*** ')
            exit()

    def write_headers_to_file(self,account_id, header_columns,passed_param_name):
        
        if "ced_columns_from_podconfig" == passed_param_name:
            filename = "CEDHeaders_FromPodConfig_" + self.runTime + ".csv"
        elif "ced_columns_from_db" == passed_param_name:
            filename = account_id + "_CEDHeaders_FromSysAdminDB_" + self.runTime + ".csv"
        elif "ced_columns_from_file" == passed_param_name:
            filename = account_id + "_CEDHeaders_FromFile_" + self.runTime + ".csv"
        else:
            filename = "ResultFile_" + self.runTime + ".csv"

        # resultFileName = self.resultFilePath+"\\"+account_id+"_CEDHeaders_FromSysAdminDB_"+self.runTime+".csv"
        resultFileName = self.resultFilePath + "\\" + filename
        with open(resultFileName, "a+") as f:
            writer = csv.writer(f, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_ALL)

            prev_data = open(resultFileName, "r").read()
            header = ["Event Types", "Column Name"]
            # Add a header only if the fname is empty
            if prev_data == '':
                writer.writerow(header)
            # writer.writerow([EventType, Headers[EventType]])
            for eventType in header_columns.keys():
                writer.writerows([[eventType] + header_columns[eventType]])
        # writerow takes 1-dimensional data (one row), and writerows takes 2-dimensional data (multiple rows).
        result_file = "\n***INFO : SAVED A FILE (", filename.upper(), ") WITH ALL EVENTS & THIER HEADERS*****"
        return result_file

    def get_custom_properties(self,curs):
        query_for_email_columns = "SELECT COLUMN_ID, COLUMN_NAME FROM CUSTOM_EVENT_COLUMN"
        query_for_sms_columns = "SELECT COLUMN_ID, COLUMN_NAME FROM SMS_CUSTOM_EVENT_COLUMN"
        queries= [query_for_email_columns,query_for_sms_columns]

        email_custom_columns = defaultdict(list)
        sms_custom_columns = defaultdict(list)

        curs.execute(query_for_email_columns)
        query_result = curs.fetchall()
        column_id = [i[0] for i in query_result]
        for i in range(len(query_result)):
            email_custom_columns[int(column_id[i])].append(query_result[i][1]) # provided [1], to append column name with 'i'th ID & 'i'th value in 2nd column of query result.
            # index of NAME column (from queryResult) is 1 i.e 2nd column,

            email_columns_by_id = sorted(email_custom_columns)
            # print("Sorted ID and Columns are :", emailColumnsByID[i], emailCustomColumns[emailColumnsByID[i]])
            # print("Sorted ID and Columns for EMAIL are :", emailCustomColumns, "\n and for SMS are :", smsCustomColumns)

        curs.execute(query_for_email_columns)
        queryResult = curs.fetchall()
        column_id = [i[0] for i in queryResult]

        curs.execute(query_for_sms_columns)
        query_result = curs.fetchall()
        column_id = [i[0] for i in query_result]
        for i in range(len(queryResult)):
            # print("ID is",columnID[i])
            sms_custom_columns[int(column_id[i])].append(query_result[i][
                                                            1])  # provided [1], to append column name with 'i'th ID
            # & 'i'th value in 2nd column of query result.
            # index of NAME column (from queryResult) is 1 i.e 2nd column,

            sms_columns_by_id = sorted(sms_custom_columns)
            # print("Sorted ID and Columns are :", smsColumnsByID[i], smsCustomColumns[smsColumnsByID[i]])
        # print("Sorted ID and Columns for EMAIL are :", emailCustomColumns, "\n and for SMS are :", smsCustomColumns)

        return email_columns_by_id, email_custom_columns,sms_columns_by_id,sms_custom_columns

    def validate_columns_and_save_result(self,account_name, file, ced_columns_from_file,
                                                         ced_columns_from_db, ced_columns_from_podconfig, email_custom_columns,
                                                         sms_custom_columns):
        global account_id
        account_id = self.prop[account_name + "CustAccID"]

        event_name = re.split(r"\d+", file)
        event_name = event_name[1].strip('_')
            # print("\nFile name :",cedFileName, " and EventType is :",event_name)
            # if "CUSTOM_PROPERTIES" in ced_headers_from_db[event_name]:
            #     indexOfCustomProperties = ced_headers_from_db[event_name].index("CUSTOM_PROPERTIES")
        if event_name in ced_columns_from_db:
            ced_headers_from_db = ced_columns_from_db
        else:
            ced_headers_from_db = ced_columns_from_podconfig

        index_Of_custom_properties = len(ced_headers_from_db[event_name])
        if event_name in ced_headers_from_db:
            if "CUSTOM_PROPERTIES" in ced_headers_from_db[event_name]:
                index_Of_custom_properties = ced_headers_from_db[event_name].index("CUSTOM_PROPERTIES")

            if 'SMS' in event_name:
                cust_column_ids = sorted(sms_custom_columns.keys())
                cust_column_names = sms_custom_columns
            else:
                cust_column_ids = sorted(email_custom_columns.keys())
                cust_column_names = email_custom_columns

            for each_column_in_db in ced_headers_from_db[event_name]:
                index_of_db_column = ced_headers_from_db[event_name].index(each_column_in_db) #get index of column configured in setting

                if index_of_db_column > index_Of_custom_properties: #if column present after the custom properties column, then compensate the index for rest of the columns
                    index_of_db_column = index_of_db_column + len(cust_column_ids)-1

                if each_column_in_db in ced_columns_from_file[event_name]:
                    column_presence_status = "Column " + each_column_in_db + " is present in CED"
                    short_status = "Present"
                else:
                    column_presence_status = "Column " + each_column_in_db + " is missing"
                    short_status = "Missing"

                try:
                    index_of_ced_column = ced_columns_from_file[event_name].index(each_column_in_db)
                    if index_of_db_column == index_of_ced_column:
                        column_order_status = "Column is in Order"
                    else:
                        column_order_status = "Column is NOT in Order"

                except Exception as e:
                    index_of_ced_column = "N/A"
                    column_order_status = e
                    print("***There is an Exception :(", e, "),Column", each_column_in_db, "is not present in file", file,"***")

                if each_column_in_db == "CUSTOM_PROPERTIES":
                    db_index = ced_headers_from_db[event_name].index("CUSTOM_PROPERTIES")  #capture index of custom properties and
                    # keep increasing till for each of custom column
                    for i in cust_column_ids:     #loop through all the ids (sorted in asc order) of the columns
                        each_column_in_db = cust_column_names[i][0]  # get each column name using id
                        if each_column_in_db in ced_columns_from_file[event_name]:
                            column_presence_status = "Column " + each_column_in_db + " is present in CED"
                            status = "Present"
                        else:
                            column_presence_status = "Column " + each_column_in_db + " is missing"
                            status = "Missing"

                        try:
                            ced_index = ced_columns_from_file[event_name].index(each_column_in_db)
                            if db_index == ced_index:
                                column_order = "Column is in Order"
                            else:
                                column_order = "Column is NOT in Order"
                        except Exception as e:
                            ced_index = "N/A"
                            column_order = e
                            print("***There is an Exception :(", e, "),Column", each_column_in_db,"is not present in file", file,"***")

                        self.writeColumnCheckResult(event_name, file, each_column_in_db, column_presence_status,status,db_index, ced_index, column_order)
                        # ced_index+=1  # to compensate for missing column
                        db_index += 1   # increasing index in for loop for next custom column
                    index_of_db_column = db_index
                    continue
                self.writeColumnCheckResult(event_name, file, each_column_in_db, column_presence_status, short_status, index_of_db_column,
                                                       index_of_ced_column, column_order_status)
                    # self.htmlReport(event_name, cedFileName, each_column_in_db, column_presence_status, short_status, index_of_db_column,)
                # self.testHTML(event_name, cedFileName, each_column_in_db, column_presence_status, short_status,index_of_db_column,index_of_ced_column, column_order_status)
        else:
            print("***INFO : Header Details for Event ", event_name ," is not available in DB")
        print("\nRESULT FILE IS SAVED AT LOCATION :", self.resultFilePath)

        return

    def writeColumnCheckResult(self,event_name, file, each_column_in_db, column_presence_status, short_status, index_of_db_column,
                                                       index_of_ced_column, column_order_status):

        filename = self.resultFilePath+"\\"+account_id+"_CEDHeaders_VerficationResult_"+self.runTime+".csv"
        f = open(filename, "a+")
        writer = csv.writer(f, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_ALL)

        prev_data = open(filename, "r").read()
        header = ["Event Types", "File Name", "ColumnNames From FeedsSetting", "Present in CED or Not", "Result",
                  "DB Column Index", "CED Column Index", "Column Order Result"]
        # Add a header only if the fname is empty
        if prev_data == '':
            writer.writerow(header)
        writer.writerow([event_name, file, each_column_in_db, column_presence_status, short_status, index_of_db_column,
                                                       index_of_ced_column, column_order_status])
        f.close()
        return

    def validate_data_from_ced_bkp(self,curs,file,search_column, ced_data, index_Of_stored_date, ced_columns_from_file, event_stored_date,event_type):
        # global eventName
        # eventName = "Test"
        # htmlfile = CommonFunctions.resultFilePath + "\\DataValidationReport_" + CommonFunctions.runTime + ".html"
        # hs = open(htmlfile, 'a+')
        # searchID = searchCol

        for id in ced_data:
            # index_Of_date = event_stored_date[id]
            index_Of_date = index_Of_stored_date
            eventTable = DBFunctions.get_event_table(self,event_type)

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
                # with open(os.path.join(CommonFunctions.inputFilesPath, file), 'r') as f:
                #     content = f.readlines()
                rowNum = 0;  # for rows in query result
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

                        print("\nValidating row_from_db", rowNum, "In file ", file, " for ", str(search_column), ":",
                              id)
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
                                        # TZ info is required for astimezone()function. Here both CAPTURED & STORED date are converted to PST & appended TZ info(-08:00)
                                        convertedToUTC = formatTimeInPST.astimezone(pytz.timezone(
                                            'UTC'))  # converts both Captured & Stored date from PSt to UTC

                                        if uniqueColumnsFromCED[j] != 'EVENT_STORED_DT':
                                            if cedValue == str(convertedToUTC.strftime(
                                                    format)):  # for event_captured_Dt is which is in PST,
                                                # convertedToUTC date is used to compare as CED file has UTC for both Captured & Stored Date
                                                # Status="Data for column "+uniqueColumnsFromCED[j]+" is matching for ID:"+id+".@rowCol:",rowNum,j,"Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(convertedToUTC.strftime(format))
                                                Status = uniqueColumnsFromCED[j] + "= Pass"
                                                # print(Status)
                                            else:
                                                Status = "row_from_db=" + str(rowNum) + ",col=" + str(
                                                    j) + ": Data for column " + \
                                                         uniqueColumnsFromCED[
                                                             j] + " is NOT matching. Data_In_CED:" + cedValue + " & Data_in_DB:" + str(
                                                    convertedToUTC.strftime(format))
                                                print(Status)
                                                # writeHTMLNew(hs, file, file, id, cedValue, dbValue, Status)
                                                CommonFunctions.write_results(file, id, cedValue, dbValue, Status)

                                        elif cedValue == str(timeFromDB.strftime(format)):
                                            # Status= "Data for column "+uniqueColumnsFromCED[j]+" is matching for ID:"+id+".@ rowCol:",rowNum,j," Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(timeFromDB.strftime(format))
                                            Status = uniqueColumnsFromCED[j] + "= Pass"
                                            # print(Status)
                                        else:
                                            Status = "row_from_db=" + str(rowNum) + ",col=" + str(
                                                j) + ": Data for column " + \
                                                     uniqueColumnsFromCED[
                                                         j] + " is NOT matching. Data_In_CED:" + cedValue + " & Data_in_DB:" + str(
                                                timeFromDB.strftime(format))
                                            print(Status)
                                            # writeHTMLNew(hs, file, file, id, cedValue, dbValue, Status)
                                            CommonFunctions.write_results(file, id, cedValue, dbValue, Status)

                                    elif cedValue == str(dbValue):
                                        # Status= "Data for column "+uniqueColumnsFromCED[j]+" is matching for ID:"+id+".@rowCol:",rowNum,j,"Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(dbValue)
                                        Status = uniqueColumnsFromCED[j] + "= Pass"
                                        print(Status)

                                    else:
                                        Status = "row_from_db=" + str(rowNum) + ",col=" + str(j) + ": Data for column " + \
                                                 uniqueColumnsFromCED[
                                                     j] + " is NOT matching. Data_In_CED:" + cedValue + " & Data_in_DB:" + str(
                                            dbValue)
                                        print(Status)
                                        # writeHTMLNew(hs, file, file, id, cedValue, dbValue, Status)
                                        CommonFunctions.write_results(file, id, cedValue, dbValue, Status)

                                # writeHTMLNew(hs, file, file, colData[j], dbValue, Status)
                                except Exception as e:
                                    print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",",
                                          type(e).__name__, ":", e, "*****\n")
                                    print(traceback.format_exc())
                            else:
                                print("No Match")
                        if j == max(
                                numberOfColumnsFromDB):  # data from ced is stored in sequence, ex if there are
                            # 15 columns in ced, 2nd row from ced will start from 16th column in dictionary
                            totalCEDColumns = len(uniqueCEDColumns)
                            totalDBColumns = len(uniqueColumnsFromCED)
                            CEDnDBColDiff = totalCEDColumns - (totalDBColumns - 1)
                            # if Data[file][nxtID][id][k] == "\n":
                            k += CEDnDBColDiff
                        else:
                            k += 1
                    rowNum += 1

    def validate_data_from_ced(self,curs,file,search_column, ced_data, index_Of_stored_date, ced_columns_from_file, event_stored_date,event_type):

        event_table = DBFunctions.get_event_table(self,event_type)

        column_name_query = "SELECT * from " + event_table + " WHERE rownum=0"  # + searchCol + "='" + str(id) + "'"
        curs.execute(column_name_query)

        db_column_names = [row[0] for row in curs.description]
        number_of_columns = len(ced_columns_from_file[event_type])
        unique_ced_columns = []
        filtered_columns = []
        i = 0
        while (i < number_of_columns):
            if ced_columns_from_file[event_type][i] in db_column_names:
                filtered_columns.append(ced_columns_from_file[event_type][i])  # removes custom columns (in CED file)
            i += 1

        unique_filtered_columns_from_ced = [] # when there multiple files of same event, all the columns from all files are populated.so removing duplicates.
        [unique_filtered_columns_from_ced.append(item) for item in filtered_columns if
         item not in unique_filtered_columns_from_ced]  # including columns which are common in CED & DB
        [unique_ced_columns.append(item) for item in ced_columns_from_file[event_type] if item not in unique_ced_columns]

        for id in ced_data:
            # print("Event Data for ID", id, "is ", event_stored_date[id][0])
            query = "SELECT " + str(",".join(unique_filtered_columns_from_ced)) + " FROM " + event_table + " WHERE " + search_column + "='" + str(
                id) + "' ORDER BY EVENT_STORED_DT"
            # print("Query is :", query)
            curs.execute(query)
            query_result_for_id = curs.fetchall()
            row_num = 0;  # for rows in query result


            number_of_columns_for_id = len(ced_data[id])
            k = 0
            row_num_for_ced = 0

            for row_from_db in query_result_for_id:
                event_date_for_record_from_ced = ced_data[id][k + index_Of_stored_date]
                event_date_for_record_from_db = datetime.strftime(row_from_db[index_Of_stored_date], '%d-%b-%Y %H:%M:%S')
                number_Of_columns_from_db = range(len(unique_filtered_columns_from_ced))

                if int(id) in row_from_db and event_date_for_record_from_ced == event_date_for_record_from_db:
                    for j in number_Of_columns_from_db:  # index for number of columns in a query result
                        print("\nValidating row", row_num_for_ced, "(DB row:", row_num, ")In file ", file, " for ", str(search_column), ":",
                              id)
                        if k < number_of_columns_for_id:
                            try:
                                ced_value = ced_data[id][k]
                                if ced_value == "":
                                    ced_value = 'None'
                                db_value = query_result_for_id[row_num][j]
                                format = "%d-%b-%Y %H:%M:%S"  # Date Format in CED File
                                if type(db_value) == datetime:
                                    timeFromDB = db_value
                                    formatTimeInPST = pytz.timezone('US/Pacific').localize(
                                        timeFromDB)  # localizing adds timezone info to the timestamp
                                    # TZ info is required for astimezone()function. Here both CAPTURED & STORED date are converted to PST & appended TZ info(-08:00)
                                    convertedToUTC = formatTimeInPST.astimezone(pytz.timezone(
                                        'UTC'))  # converts both Captured & Stored date from PSt to UTC

                                    if unique_filtered_columns_from_ced[j] != 'EVENT_STORED_DT':
                                        if ced_value == str(convertedToUTC.strftime(
                                                format)):  # for event_captured_Dt is which is in PST,
                                            # convertedToUTC date is used to compare as CED file has UTC for both Captured & Stored Date
                                            # Status="Data for column "+unique_filtered_columns_from_ced[j]+" is matching for ID:"+id+".@rowCol:",row_num,j,"Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(convertedToUTC.strftime(format))
                                            Status = unique_filtered_columns_from_ced[j] + "= Pass"
                                            print(Status)
                                        else:
                                            Status = "row=" + str(row_num) + ",col=" + str(
                                                j) + ": Data for column " + \
                                                     unique_filtered_columns_from_ced[
                                                         j] + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(
                                                convertedToUTC.strftime(format))
                                            # print(Status)
                                            # writeHTMLNew(hs, file, file, id, ced_value, db_value, Status)
                                            CommonFunctions.write_results(self,file, id, ced_value, db_value, Status)

                                    elif ced_value == str(timeFromDB.strftime(format)):
                                        # Status= "Data for column "+unique_filtered_columns_from_ced[j]+" is matching for ID:"+id+".@ rowCol:",row_num,j," Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(timeFromDB.strftime(format))
                                        Status = unique_filtered_columns_from_ced[j] + "= Pass"
                                        print(Status)
                                    else:
                                        Status = "row=" + str(row_num) + ",col=" + str(
                                            j) + ": Data for column " + \
                                                 unique_filtered_columns_from_ced[
                                                     j] + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(
                                            timeFromDB.strftime(format))
                                        # print(Status)
                                        # writeHTMLNew(hs, file, file, id, ced_value, db_value, Status)
                                        CommonFunctions.write_results(self,file, id, ced_value, db_value, Status)

                                elif ced_value == str(db_value):
                                    # Status= "Data for column "+unique_filtered_columns_from_ced[j]+" is matching for ID:"+id+".@rowCol:",row_num,j,"Data_In_CED:"+colData[j]+" & Data_in_DB:"+str(db_value)
                                    Status = unique_filtered_columns_from_ced[j] + "= Pass"
                                    print(Status)

                                else:
                                    Status = "row=" + str(row_num) + ",col=" + str(j) + ": Data for column " + \
                                             unique_filtered_columns_from_ced[
                                                 j] + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(
                                        db_value)
                                    print(Status)
                                    # writeHTMLNew(hs, file, file, id, ced_value, db_value, Status)
                                    CommonFunctions.write_results(self,file, id, ced_value, db_value, Status)

                            # writeHTMLNew(hs, file, file, colData[j], db_value, Status)
                            except Exception as e:
                                print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",",
                                      type(e).__name__, ":", e, "*****\n")
                                print(traceback.format_exc())
                        else:
                            print("No Match")
                        if j == max(
                                number_Of_columns_from_db):  # data from ced is stored in sequence, ex if there are
                            # 15 columns in ced, 2nd row from ced will start from 16th column in dictionary
                            number_of_ced_columns = len(unique_ced_columns)
                            number_of_db_columns = len(unique_filtered_columns_from_ced)
                            col_difference = number_of_ced_columns - (number_of_db_columns - 1)
                            # if Data[file][nxtID][id][k] == "\n":
                            k += col_difference
                            row_num_for_ced += 1
                        else:
                            k += 1
                row_num += 1

    def write_results(self, cedFileName, id, colData, query_result_for_id, Status):
        AccountID = re.split(r"_", cedFileName)  # Extract Account ID from the filename
        AccountID = AccountID[0].strip('_')

        filename = CommonFunctions.resultFilePath + "\\" + AccountID + "_DataValidationReport_" + CommonFunctions.runTime + ".csv"
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
            # writer.writerow([EventType, Headers[EventType]])
            writer.writerow([cedFileName, id, colData, query_result_for_id,
                             Status])  # writerow takes 1-dimensional data (one row), and writerows takes 2-dimensional data (multiple rows).

        f.close()
        return


