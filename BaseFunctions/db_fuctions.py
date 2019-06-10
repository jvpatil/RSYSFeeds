import csv
import re
import sys
from collections import defaultdict
import traceback
from datetime import datetime, date, time, timedelta
from xml.etree import ElementTree as ET

from lxml.etree import ElementTree

from BaseFunctions.setup import Setup


class DBFunctions():
    input_files_path = Setup.testfilespath
    result_files_path = Setup.resultFilePath
    run_time = datetime.now().strftime("%d%b%Y_%H.%M.%S")  # current time in ddmmyyyy_hh24mmss
    report = "Result_" + str(run_time) + ""

    def get_count_from_db(self, curs, accountName, schema, event_type, searchColumn, list_unique_id,
            dEventStoredDate):
        d_count_from_db = defaultdict(list)
        d_event_dates_from_db = defaultdict(list)

        try:
            # for id in uniqueIDs:
            #     query = self.getQuery(event_type,searchColumn,id)
            #     # print("Query for Count is :", query)
            #     curs.execute(query)
            #     resultCount = [i[0] for i in curs.fetchall()]
            #     dCountForIDs[id].extend(resultCount)
            query = self.get_query(event_type, searchColumn, list_unique_id)
            curs.execute(query)
            query_result = curs.fetchall()  # if the event table has 0 records for the particular launch_id,
            # then it is not in

            for row in range(len(query_result)):
                launch_id = query_result[row][0]
                cnt = query_result[row][1]
                min_event_date = query_result[row][2]
                max_event_date = query_result[row][3]
                d_count_from_db[launch_id].append(cnt)
                d_event_dates_from_db[launch_id].append(min_event_date)
                d_event_dates_from_db[launch_id].append(max_event_date)

        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
                  "*****\n")
            print(traceback.format_exc())

        return d_count_from_db, d_event_dates_from_db

    def get_missing_events(self, curs, searchColumn, event_type, dEventStoredDate, d_event_dates_from_db, d_count_from_db):
        d_events_to_process = defaultdict(list)
        d_events_processed = defaultdict(list)
        event_table = self.get_event_table(event_type)

        try:
            for launch_id in d_count_from_db:
                first_event_date = min(dEventStoredDate[str(launch_id)])
                ced_first_event_date = datetime.strptime(first_event_date, '%d-%b-%Y %H:%M:%S')
                ced_first_event_date = ced_first_event_date.strftime("%d-%b-%Y %I:%M:%S %p")

                last_event_date = max(dEventStoredDate[str(launch_id)])
                ced_last_event_date = datetime.strptime(last_event_date, '%d-%b-%Y %H:%M:%S')
                ced_last_event_date = ced_last_event_date + timedelta(
                    seconds=60)  # increasing a minute coz, db LAST_EVENT_DATE in db contains milisecond and that record
                # also included when query is run
                ced_last_event_date = ced_last_event_date.replace(second=0, )  # again offsetting seconds to 0
                ced_last_event_date = ced_last_event_date.strftime("%d-%b-%Y %I:%M:%S %p")

                db_first_event_date = min(d_event_dates_from_db[launch_id]).strftime("%d-%b-%Y %I:%M:%S %p")
                db_last_event_date = max(d_event_dates_from_db[launch_id]).strftime("%d-%b-%Y %I:%M:%S.%f %p")

                events_to_be_processed_query = "SELECT count(*) from " + event_table + " WHERE " + searchColumn + "=" + str(
                    launch_id) + " AND EVENT_STORED_DT > '" + ced_last_event_date + "' AND EVENT_STORED_DT <= '" + \
                                           db_last_event_date + "'"

                events_already_processed_query = "SELECT count(*) from " + event_table + " WHERE " + searchColumn + "=" + \
                                              str(
                                                  launch_id) + " AND EVENT_STORED_DT < '" + ced_first_event_date + "' AND " \
                                                                                                               "" \
                                                                                                               "EVENT_STORED_DT >= '" + \
                                              db_first_event_date + "'"

                curs.execute(events_to_be_processed_query)
                records_not_processed = [i[0] for i in curs.fetchall()]
                d_events_to_process[launch_id].extend(records_not_processed)

                curs.execute(events_already_processed_query)
                records_already_processed = [i[0] for i in curs.fetchall()]
                d_events_processed[launch_id].extend(records_already_processed)
        except Exception as e:
            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
                  "*****\n")
            print(traceback.format_exc())

        return d_events_processed, d_events_to_process

    def get_query(self, event_type, searchColumn, uniqueIDs):
        event_table = self.get_event_table(event_type)
        # event_table = event_table_names[event_type]
        # # event_table = DBFunctions.event_table_names[event_type]
        # query = "SELECT COUNT(*) FROM " + str(event_table) + " WHERE " + searchColumn + " = " + ID +""

        all_IDs = (",".join(uniqueIDs))
        totalIds = len(all_IDs)
        group_query_for_count = "select " + searchColumn + ", count(*),MIN(EVENT_STORED_DT),MAX(EVENT_STORED_DT) from " + \
                             event_table + " WHERE " + searchColumn + " in (" + str(
            all_IDs) + ") GROUP BY " + searchColumn

        # events_to_be_processed_query = "SELECT count(*) from " + event_table + " WHERE " + searchColumn + "=" + str(
        #     id) + " AND EVENT_STORED_DT > '" + ced_last_event_date + "' AND EVENT_STORED_DT <= "'" db_last_event_date + "'
        #
        # events_already_processed_query = "SELECT count(*) from " + event_table + " WHERE " + searchColumn + "=" + str(
        #     id) + " AND EVENT_STORED_DT < '" + ced_first_event_date + "' AND EVENT_STORED_DT" >= '" db_first_event_date + "'

        return group_query_for_count

    def get_event_table(self, event_type):
        event_table_names = {
            'SENT': 'E_RECIPIENT_SENT',
            'SKIPPED': 'E_RECIPIENT_SKIPPED',
            'BOUNCE': 'E_RECIPIENT_BOUNCED',
            'OPEN': 'E_RECIPIENT_OPENED',
            'CLICK': 'E_RECIPIENT_CLICKED',
            'CONVERT': 'E_RECIPIENT_CONVERSION',
            'OPT_IN': 'E_RECIPIENT_OPTIN',
            'OPT_OUT': 'E_RECIPIENT_OPTOUT',
            'FAIL': 'E_RECIPIENT_FAILED',
            'COMPLAINT': 'E_RECIPIENT_COMPLAINT',
            'FORM': 'E_RECIPIENT_FORM',
            'FORM_STATE': 'E_STATE_FORM',
            'PROGRAM_': 'E_RECIPIENT_PROGRAM',
            'PROGRAM_STATE': 'E_STATE_PROGRAM',
            'HOLDOUT_GROUP': 'E_RECIPIENT_PROGRAM_HOLDOUT',
            'LAUNCH_STATE': 'E_STATE_LAUNCH',
            'DYNAMIC_CONTENT': 'E_SIG_DYNAMIC_CONTENT',
            'SMS_SENT': 'E_RECIPIENT_SMS_SENT',
            'SMS_OPEN': 'E_RECIPIENT_SMS_OPENED',
            'SMS_CLICK': 'E_RECIPIENT_SMS_CLICKED',
            'SMS_SKIPPED': 'E_RECIPIENT_SMS_SKIPPED',
            'SMS_RECEIVED': 'E_RECIPIENT_SMS_RECEIVED',
            'SMS_CONVERT': 'E_RECIPIENT_SMS_CONVERSION',
            'SMS_FAIL': 'E_RECIPIENT_SMS_FAILED',
            'SMS_OPT_IN': 'E_RECIPIENT_SMS_OPTIN',
            'SMS_OPT_OUT': 'E_RECIPIENT_SMS_OPTOUT',
            'SMS_MO_FW_SENT': 'E_RECIPIENT_SMS_MO_FW_SENT',
            'SMS_MO_FW_FAILED': 'E_RECIPIENT_SMS_MO_FW_FAILED',
            'SMS_DIVERED': 'E_RECIPIENT_SMS_RECEIPT',
            'PUSH_SENT': 'E_RECIPIENT_PUSH_SENT',
            'PUSH_OPENED': 'E_RECIPIENT_PUSH_OPENED',
            'PUSH_CLICKED': 'E_RECIPIENT_PUSH_CLICKED',
            'PUSH_SKIPPED': 'E_RECIPIENT_PUSH_SKIPPED',
            'PUSH_BOUNCED': 'E_RECIPIENT_PUSH_BOUNCED',
            'PUSH_CONVERTED': 'E_RECIPIENT_PUSH_CONVERSION',
            'PUSH_FAILED': 'E_RECIPIENT_PUSH_FAILED',
            'PUSH_OPT_IN': 'E_RECIPIENT_PUSH_OPTIN',
            'PUSH_OPT_OUT': 'E_RECIPIENT_PUSH_OPTOUT',
            'PUSH_BUTTON_CLICKED': 'E_RECIPIENT_PUSH_BTN_CLICKED',
            'PUSH_INBOX_SENT': 'E_RECIPIENT_PUSH_INBOX_SENT',
            'PUSH_INBOX_SKIPPED': 'E_RECIPIENT_PUSH_INBOX_SKIPPED',
            'PUSH_UNINSTALL': 'E_RECIPIENT_PUSH_UNINSTALL',
            'MMS_SENT': 'E_RECIPIENT_MMS_SENT',
            'MMS_FAILED': 'E_RECIPIENT_MMS_FAILED',
            'MMS_SKIPPED': 'E_RECIPIENT_MMS_SKIPPED',
            'APPCLOUD_SENT': 'E_RECIPIENT_APPCLOUD_SENT',
            'APPCLOUD_FAILED': 'E_RECIPIENT_APPCLOUD_FAILED',
            'APPCLOUD_SKIPPED': 'E_RECIPIENT_APPCLOUD_SKIPPED'
        }
        event_table = event_table_names[event_type]
        return event_table

    def get_headers_from_db(self, curs, account_name):
        global account_id
        account_id = self.prop[account_name + "CustAccID"]
        feedHeadersFromDB = {}
        header_query = "SELECT S.SECTION_KEY ,A.SECTION_KEY_VALUE_STRING FROM ACCOUNT_SETTINGS A LEFT OUTER JOIN " \
                       "SECTION_KEY S ON A.SECTION_KEY_ID=S.SECTION_KEY_ID WHERE S.SECTION_NAME LIKE '%EventConnect%' " \
                       "AND A.ACCOUNT_ID=" + str(account_id) + ""
        curs.execute(header_query)

        query_result_all_headers = curs.fetchall()
        if curs.rowcount == 0:
            print("\n ************NO RESULT, PLEASE CHECK IF ACCOUNT", account_id,
                  "HAS ITS SETTINGS IN DB OR NOT**************")
        ced_headers_from_db = defaultdict(list)
        built_in_headers_from_db = defaultdict(list)
        events = []
        for each_header in query_result_all_headers:
            # print("Header From CED is : ",eachHeader)
            if '.' in each_header[0]:
                event_type = each_header[0].split(".")[0].strip()
                events.append(each_header[0].split(".")[0].strip())  # Storing all event types for external use
                # print("EventTYpe is :", EventType)
                col_names = each_header[1].split(",")

                for i in range(len(col_names)):
                    if "$" in col_names[i]:
                        split_built_in_column = col_names[i].split(":")
                        column_name = split_built_in_column[1]
                        built_in_headers_from_db[event_type].append(column_name)
                    ced_headers_from_db[event_type].append(col_names[i])

        return ced_headers_from_db,built_in_headers_from_db

    # def compareCountForIDs(self,dCountFromCED,d_count_from_db,d_events_to_process,d_events_processed):
    #
    #     dStatus = defaultdict(list)
    #     dStatus = defaultdict(list)
    #     d
    #     try:
    #         for id in dCountFromCED:
    #             cedCount = dCountFromCED[id][0]
    #             dbCount = d_count_from_db[int(id)][0]
    #             toBeProcessed = d_events_to_process[int(id)][0]
    #             alreadyProcessed = d_events_processed[int(id)][0]
    #             if cedCount == dbCount:
    #                 status = "Pass"
    #                 result = "Count for the ID Match"
    #             else:
    #                 status = "Fail"
    #                 result = "Count for the ID does not Match"
    #
    #     except Exception as e:
    #         print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
    #         "*****\n")
    #         print(traceback.format_exc())
    #      return

    # def compareCountForIDs(self,cedFileName, searchID, dCountFromCED , d_count_from_db,d_events_to_process,
    # d_events_processed):
    #     AccountID = re.split(r"_", cedFileName)  # Extract Account ID from the filename
    #     AccountID = AccountID[0].strip('_')
    #     resultFile = DBFunctions.result_files_path + "\\" + AccountID + "_FeedsData_CompareResult_" +
    #     DBFunctions.run_time + ".csv"
    #
    #     header = ["EVENT_TYPE", "FILE_NAME", "KEY", "ID", "COUNT_FROM_CED", "COUNT_FROM_DB","EventsToBeProcessed",
    #     "EventsAlreadyProcessed","STATUS","Comments"]
    #
    #     name = re.split(r"\d+", cedFileName)  # Extract only event name from the filename
    #     event_type = name[1].strip('_')
    #
    #     openFile = open(resultFile, "a+")
    #     writer = csv.writer(openFile, lineterminator="\n", quoting=csv.QUOTE_ALL)
    #     checkHeader = open(resultFile, "r").read()
    #     if checkHeader == '':
    #         writer.writerow(header)
    #     try:
    #         for id in dCountFromCED:
    #             cedCount = dCountFromCED[id][0]
    #             dbCount = d_count_from_db[int(id)][0]
    #             NotProcessed = d_events_to_process[int(id)][0]
    #             alreadyProcessed = d_events_processed[int(id)][0]
    #
    #             if dbCount == cedCount:
    #                 result = "Count for the ID Match"
    #                 status = "Pass"
    #             elif dbCount != cedCount and NotProcessed != 0:
    #                 if type(NotProcessed) == str:
    #                     result = "No Data found in " + event_type + " table for " + searchID + " : " + id
    #                     status = "Fail"
    #                 else:
    #                     actualCount = dbCount - NotProcessed
    #                     if cedCount == actualCount:
    #                         result = "Count for the ID Match, But there are " + str(
    #                             NotProcessed) + " to be processed yet"
    #                         status = "Pass"
    #                     else:
    #                         result = "Count for the ID does not Match, Seems Records are Purged for " + event_type +
    #                         " table"
    #                         status = "Fail"
    #             elif dbCount != cedCount and alreadyProcessed != 0:
    #                 if type(alreadyProcessed) == str:
    #                     result = "No Data found in " + event_type + " table for " + searchID + " : " + id
    #                     status = "Fail"
    #                 else:
    #                     actCount = dbCount - alreadyProcessed
    #                     if cedCount == actCount:
    #                         result = "Count for the ID does not Match, Looks like " + str(
    #                             alreadyProcessed) + " records are already processed and exported"
    #                         status = "Fail"
    #                     else:
    #                         result = "Count for the ID does not Match, Seems Records are Purged for " + event_type +
    #                         " table"
    #                         status = "Fail"
    #             else:
    #                 result = "Count for the ID does not Match"
    #                 status = "Fail"
    #
    #             writer.writerow([event_type, cedFileName, searchID, id, cedCount,dbCount,NotProcessed,
    #             alreadyProcessed, status, result ])
    #     except Exception as e:
    #         print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
    #         "*****\n")
    #         print(traceback.format_exc())
    #
    #     openFile.close()
    #     return resultFile

    def write_from_db_to_file(self, writer, event_type, cedFileName, searchID, id, cedCount, dbCount, NotProcessed,
            alreadyProcessed, status, result):
        # writer = csv.writer(openFile, lineterminator="\n", quoting=csv.QUOTE_ALL)
        # checkHeader = open(resultFile, "r").read()
        # header = ["EVENT_TYPE", "FILE_NAME", "KEY", "ID", "COUNT_FROM_CED", "COUNT_FROM_DB", "EventsToBeProcessed",
        #           "EventsAlreadyProcessed", "STATUS", "Comments"]
        # if checkHeader == '':
        #     writer.writerow(header)
        writer.writerow(
            [event_type, cedFileName, searchID, id, cedCount, dbCount, NotProcessed, alreadyProcessed, status, result])

    def extract_custom_properties_data(self,curs):



        query = "SELECT CUSTOM_PROPERTIES FROM E_RECIPIENT_SKIPPED WHERE LAUNCH_ID=19081 ORDER BY EVENT_STORED_DT"
        curs.execute(query)
        custom_properties = curs.fetchall()
        xml_tag = "<?xml version='1.0'?>"
        custom_properties = xml_tag+str(custom_properties)
        tree = ElementTree()
        tree.parse(custom_properties)

        data = {}
        for val in custom_properties:
            val = xml_tag+str(val[0])
            tree.parse(val)
            rows = tree.findall("prop")
            for row in rows:
                for elem in row.findall("*"):
                    if elem.tag == "name":
                        Name = elem.text
                        data[Name] = {}
                    elif elem.tag == "value":
                        Value = elem.text
                        data[Value] = {}

                    else:
                        data[Name][elem.tag.lower()] = elem.text
        return data

        # for row in custom_properties:
        #     root = ET.fromstring(row)
        #     levels = root.findall('.//prop')
        #     for level in levels:
        #         Name = level.find('name').text
        #         Value = level.find('age').text
        #         print(" Name =", Name, "and Age =", Value)
        #

        # for row in custom_properties:
        #     tree = ET.parse(row)
        #     root = tree.getroot()
        #
        #     for i in root.iter('prop'):
        #         print("Name =", i.attrib['name'])
        #         print("data =", i.attrib['value'])





