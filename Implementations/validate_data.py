import pytz
from Implementations.setup import Setup
from Implementations.db_fuctions import DBFunctions
from Implementations.common_functions import CommonFunctions
from Implementations.device_details import DeviceDetails
from datetime import datetime
import sys
import os
import re
import traceback
from collections import defaultdict
import requests
from bs4 import BeautifulSoup

def validate_data_from_ced(self, curs, file, search_column, ced_data, index_Of_stored_date, ced_columns_from_file, event_stored_date, event_type,
                           account_name, email_custom_columns, sms_custom_columns, CEDDatesInAccountTZ):
    event_table = DBFunctions.get_event_table(self, event_type)
    column_name_query = "SELECT * from " + account_name + "_EVENT." + event_table + " WHERE rownum=0"
    curs.execute(column_name_query)
    db_column_names = [row[0] for row in curs.description]

    all_columns_from_ced = []
    [all_columns_from_ced.append(item) for item in ced_columns_from_file[event_type] if item not in all_columns_from_ced]

    if 'SMS' in event_table:
        custom_columns = sms_custom_columns
    else:
        custom_columns = email_custom_columns

    device_attributes = ["BROWSER_TYPE_INFO", "BROWSER_INFO", "OS_VENDOR_INFO", "OPERATING_SYSTEM_INFO", "DEVICE_TYPE_INFO"]
    device_ids, device_data,index_of_riid = None, None,None
    if "USER_AGENT_STRING" in all_columns_from_ced:
        index_of_riid = all_columns_from_ced.index("RIID")
        device_ids, device_data = DeviceDetails.get_device_attributes(self, account_name)
        add_remove_column_for_query(self,all_columns_from_ced,device_attributes)

    if "CUSTOM_PROPERTIES" in db_column_names:
        add_remove_column_for_query(self, all_columns_from_ced, custom_columns)

    columns_to_be_queried_from_db = all_columns_from_ced
    for id in ced_data:
        # print("Event Data for ID", id, "is ", event_stored_date[id][0])
        query = "SELECT " + str(
            ",".join(columns_to_be_queried_from_db)) + " FROM " + account_name + "_EVENT." + event_table + " WHERE " + search_column + "='" + str(
            id) + "' ORDER BY EVENT_STORED_DT"
        curs.execute(query)
        query_result_for_id = curs.fetchall()
        # db_row_num = 0
        ced_row_num = 0
        while (ced_row_num < len(ced_data[id])):
            db_row_num = 0
            Status = None
            for row_from_db in query_result_for_id:
                event_date_for_record_from_db = datetime.strftime(row_from_db[index_Of_stored_date], '%d-%b-%Y %H:%M:%S')
                event_date_for_record_from_ced = ced_data[id][ced_row_num][index_Of_stored_date]
                if CEDDatesInAccountTZ:
                    event_date_for_record_from_ced = datetime.strptime(event_date_for_record_from_ced, '%d-%b-%Y %H:%M:%S')
                    event_date_for_record_from_ced = self.covert_acc_tz_to_utc(event_date_for_record_from_ced)

                if 'SMS' in event_type or 'MMS' in event_type or 'PUSH' in event_type:
                    index_of_uuid = ced_columns_from_file[event_type].index('EVENT_UUID')
                    uuid_from_db = row_from_db[index_of_uuid]
                    uuid_from_ced = ced_data[id][ced_row_num][index_of_uuid]
                    ced_db_match = check_for_match(self,uuid_from_ced,uuid_from_db,event_date_for_record_from_ced, event_date_for_record_from_db)
                else:
                    ced_db_match = check_for_match(self, event_date_for_record_from_ced, event_date_for_record_from_db)
                if int(id) in row_from_db and ced_db_match:
                # if int(id) in row_from_db and event_date_for_record_from_ced == event_date_for_record_from_db:
                    print("\nValidating row", ced_row_num, "(DB row:", db_row_num, ")In file ", file, " for ", str(search_column), ":", id)
                    for col_index, col_value in enumerate(row_from_db):
                        try:
                            ced_value = ced_data[id][ced_row_num][col_index]
                            if ced_value == "":
                                ced_value = 'None'
                            db_value = col_value
                            date_format = "%d-%b-%Y %H:%M:%S"

                            col_name = all_columns_from_ced[col_index].replace("null as ",'')
                            if all_columns_from_ced[col_index].replace("null as ",'') in custom_columns.values():
                                print("--- SKIPPING CUSTOM COLUMN FOR NOW ---")
                                continue

                            # elif col_name in ["BROWSER_TYPE_INFO", "BROWSER_INFO","OS_VENDOR_INFO", "OPERATING_SYSTEM_INFO","DEVICE_TYPE_INFO"]:
                            elif col_name in device_attributes:
                                riid = row_from_db[index_of_riid]
                                validate_device_data(self,device_ids, device_data, riid, event_type,db_row_num, col_index, col_name, file,ced_value)

                            elif type(db_value) == datetime:
                                formatTimeInPST = pytz.timezone('US/Pacific').localize(db_value)
                                convertedToUTC = formatTimeInPST.astimezone(pytz.timezone('UTC'))

                                if CEDDatesInAccountTZ:
                                    if ced_value != None:
                                        ced_value = datetime.strptime(ced_value, '%d-%b-%Y %H:%M:%S')
                                        ced_value = self.covert_acc_tz_to_utc(ced_value)
                                if columns_to_be_queried_from_db[col_index] != 'EVENT_STORED_DT':
                                    if ced_value == str(convertedToUTC.strftime(date_format)):  # for event_captured_Dt is which is in PST,
                                        Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                        print(Status)

                                    else:
                                        Status = "row=" + str(db_row_num) + ",col=" + str(col_index) + ": Data for column " + \
                                                 columns_to_be_queried_from_db[col_index] + " is NOT matching. Data_In_CED:" + str(
                                            ced_value) + " & Data_in_DB:" + str(convertedToUTC.strftime(date_format))
                                        CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                                        print(Status)

                                elif ced_value == str(db_value.strftime(date_format)):
                                    Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                    print(Status)

                                else:
                                    Status = "row=" + str(db_row_num) + ",col=" + str(col_index) + ": Data for column " + columns_to_be_queried_from_db[col_index] + " is NOT matching. Data_In_CED:" + str(ced_value) + " & Data_in_DB:" + str(
                                        db_value.strftime(date_format))
                                    CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)


                            elif ced_value == str(db_value):
                                Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                print(Status)

                            elif type(db_value) != str and db_value != None and ced_value.isdigit():
                                if float(ced_value) == float(db_value):  # handling numeric data coz sometimes 10 == 10.0 fails..
                                    Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                    print(Status)
                            else:
                                Status = "row=" + str(db_row_num) + ",col=" + str(col_index) + ": Data for column " + str(columns_to_be_queried_from_db[col_index]) + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(db_value)
                                print(Status)
                                CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                        except Exception as e:
                            print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n")
                            print(traceback.format_exc())

                else:
                    pass
                db_row_num += 1
            if Status == None:
                print("\nNO MATCHING RECORD FOUND IN DB FOR THE ROW ",ced_row_num ," OF ID ",id, " IN FILE ", file)
            ced_row_num += 1

def validate_device_data(self,device_ids, device_data, riid, event_type,db_row_num, col_index, col_name, file,ced_value):
    browser_type, os_vendor, operating_system, device_type, browser = DeviceDetails.get_data(self, device_ids, device_data, riid, event_type)
    if col_name == "BROWSER_TYPE_INFO":
        check_if_data_matches(self, db_row_num, col_index, file, col_name, id, ced_value, browser_type)

    elif col_name == "BROWSER_INFO":
        check_if_data_matches(self, db_row_num, col_index, file, col_name, id, ced_value, browser)

    elif col_name == "OS_VENDOR_INFO":
        check_if_data_matches(self, db_row_num, col_index, file, col_name, id, ced_value, os_vendor)

    elif col_name == "OPERATING_SYSTEM_INFO":
        check_if_data_matches(self, db_row_num, col_index, file, col_name, id, ced_value, operating_system)

    elif col_name == "DEVICE_TYPE_INFO":
        check_if_data_matches(self, db_row_num, col_index, file, col_name, id, ced_value, device_type)

def check_if_data_matches(self, row_num,col_num, file,column_name,id, ced_value, db_value):
    if ced_value == str(db_value):
        Status = column_name + "= Pass"
        print(Status)
    else:
        Status = "row=" + str(row_num) + ",col=" + str(col_num) + ": Data for column " + str(column_name) + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(db_value)
        print(Status)
        CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
    return
    
def add_remove_column_for_query(self, all_columns_from_ced,column_to_be_inserted):
    try:
        duplicate_column = check_for_duplicate_custom_column(self, all_columns_from_ced, column_to_be_inserted)
        for key in sorted(column_to_be_inserted):
            if type(column_to_be_inserted) == defaultdict or type(column_to_be_inserted) == dict:
                col = column_to_be_inserted[key]
                column_to_be_removed_index = all_columns_from_ced.index(col)
            else:
                col = key
                column_to_be_removed_index = all_columns_from_ced.index(col)

            if duplicate_column:
                if duplicate_column == all_columns_from_ced[column_to_be_removed_index]:
                    deleted_column = column_to_be_inserted.pop(key, duplicate_column +" Not in custom columns")
                    continue
            all_columns_from_ced.insert(column_to_be_removed_index, "null as "+ str(col))
            all_columns_from_ced.pop(column_to_be_removed_index+1)

    except Exception as e:
        print("Error in inserting dummy columns for sql query " + str(e))
    return all_columns_from_ced

def check_for_duplicate_custom_column(self,all_columns_from_ced,custom_columns):
    duplicate_col = None
    if len(custom_columns) != 0 and len(custom_columns) is not None:
        if len(custom_columns) > 1:
            try:
                if type(custom_columns) == defaultdict or type(custom_columns) == dict:
                    custom_column_names = custom_columns.values()
                    custom_column_names = list(custom_column_names)
                else:
                    custom_column_names = custom_columns

                #custom columns will be one after the other in ced, hence check if all custom columns are continues, if not consider the column lower index (bcoz system column appear first)as duplicate column
                for i in range(len(custom_column_names)-1):
                    index_of_current_col = all_columns_from_ced.index(custom_column_names[i])
                    index_of_next_col = all_columns_from_ced.index(custom_column_names[i+1])
                    if (index_of_next_col-index_of_current_col) != 1:
                        if index_of_next_col < index_of_current_col:
                            duplicate_col_index = index_of_next_col
                            duplicate_col = all_columns_from_ced[duplicate_col_index]
                            return
                        else:
                            duplicate_col_index = all_columns_from_ced[index_of_current_col]
                            duplicate_col = all_columns_from_ced[duplicate_col_index]
                            return duplicate_col
            except Exception as e:
                print("Error in identifying duplicate custom columns " + str(e))
        else:
            pass
    return duplicate_col

def check_for_match(self, ced_value, db_value,ced_stored_dt=None, db_stored_dt=None):
    if ced_value==db_value and ced_stored_dt==db_stored_dt:
        return True
    else:
        return False
