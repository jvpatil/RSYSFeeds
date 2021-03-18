import pytz

# import read_xml
from ConfigFiles import paths
from Implementations.common_functions import CommonFunctions
from Implementations.device_details import DeviceDetails
from datetime import datetime
import sys
import traceback
from collections import defaultdict
from ConfigFiles import tables
# from Implementations.common_functions import CommonFunctions
from ConfigFiles import logger_util

class ValidateDataImpl:
    def __init__(self, test_class_name):
        super().__init__(test_class_name)
        self.val_imp_log = logger_util.get_logger(test_class_name +" :: " +__name__)
        self.path = paths

    def validate_data_from_ced_progress(self, barStatus, fileStatus,curs, file, search_column, ced_data, index_Of_stored_date, ced_columns_from_file, event_stored_date, event_type,
                               account_name, email_custom_columns, sms_custom_columns, CEDDatesInAccountTZ,acc_timezone):
        global primary_key
        # event_table = DBFunctions.get_event_table(self, event_type)
        event_table = tables.event_table_names[event_type]
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
        device_ids, device_data,index_of_unique_id = None, None,None

        if event_type=="DYNAMIC_CONTENT":
            index_of_unique_id = all_columns_from_ced.index("EVENT_UUID")
        elif event_type == "FORM_STATE":
            index_of_unique_id = all_columns_from_ced.index("FORM_ID")
        elif event_type == "LAUNCH_STATE":
            index_of_unique_id = all_columns_from_ced.index("LAUNCH_ID")
        elif event_type == "PROGRAM_STATE":
            index_of_unique_id = all_columns_from_ced.index("PROGRAM_ID")
        else:
            index_of_unique_id = all_columns_from_ced.index("RIID")

        if "USER_AGENT_STRING" in all_columns_from_ced:
            if 'PUSH' in event_type.upper():
                self.val_imp_log.info('As per configuration DEVICE DETAILS not required for push channel')
            elif self.path.dataSeeded == False:
                device_ids, device_data = DeviceDetails.get_device_attributes(self, account_name)
                ValidateDataImpl.add_remove_column_for_query(self,all_columns_from_ced,device_attributes)
            else:
                ValidateDataImpl.add_remove_column_for_query(self, all_columns_from_ced, device_attributes)
            #
        if "CUSTOM_PROPERTIES" in db_column_names:
            index_of_custom_properties = db_column_names.index('CUSTOM_PROPERTIES')
            ValidateDataImpl.add_remove_column_for_query(self, all_columns_from_ced, custom_columns)
            # cust_property_data = read_xml.get_attributes()

        columns_to_be_queried_from_db = all_columns_from_ced
        # self.val_imp_log.info("\n")
        dreport = defaultdict(list)
        row_error = []
        for id in ced_data:
            query = "SELECT " + str(
                ",".join(columns_to_be_queried_from_db)) + " FROM " + account_name + "_EVENT." + event_table + " WHERE " + search_column + "='" + str(
                id) + "' ORDER BY EVENT_STORED_DT"
            self.val_imp_log.info('\n')
            # self.val_imp_log.info('Firing the query on DB')
            curs.execute(query)
            query_result_for_id = curs.fetchall()
            ced_row_num = 0
            primary_key = columns_to_be_queried_from_db[index_of_unique_id]
            if len(query_result_for_id)==0:
                self.val_imp_log.info("Skipping validation for file : " +str(file))
                # primary_key = columns_to_be_queried_from_db[index_of_unique_id]
                dreport[id].append("Skip Reason for "+str(event_type)+" : Query returned null. No result from DB for "+str(primary_key)+" : " +str(id)+"_"+str(len(ced_data[id])))
                self.val_imp_log.info("Skip Reason for "+str(event_type)+" : Query returned null. No result from DB for "+str(primary_key)+" : " +str(id))
                break
            while (ced_row_num < len(ced_data[id])):
                CommonFunctions.clear(self)
                CommonFunctions.print_status(barStatus,fileStatus)
                db_row_num = 0
                Status = None

                for row_from_db in query_result_for_id:
                    event_date_for_record_from_db = datetime.strftime(row_from_db[index_Of_stored_date], '%d-%b-%Y %H:%M:%S')
                    event_date_for_record_from_ced = ced_data[id][ced_row_num][index_Of_stored_date]
                    unique_id_from_ced = ced_data[id][ced_row_num][index_of_unique_id]
                    unique_id_from_db = row_from_db[index_of_unique_id]
                    if CEDDatesInAccountTZ:
                        event_date_for_record_from_ced = datetime.strptime(event_date_for_record_from_ced, '%d-%b-%Y %H:%M:%S')
                        event_date_for_record_from_ced = CommonFunctions.covert_acc_tz_to_utc(self,event_date_for_record_from_ced, acc_timezone)

                    if 'SMS' in event_type or 'MMS' in event_type or 'PUSH' in event_type:
                        index_of_uuid = ced_columns_from_file[event_type].index('EVENT_UUID')
                        uuid_from_db = row_from_db[index_of_uuid]
                        uuid_from_ced = ced_data[id][ced_row_num][index_of_uuid]
                        ced_db_match = ValidateDataImpl.check_for_match(self,uuid_from_ced,uuid_from_db,event_date_for_record_from_ced, event_date_for_record_from_db)
                    else:
                        ced_db_match = ValidateDataImpl.check_for_match(self,unique_id_from_ced,unique_id_from_db,event_date_for_record_from_ced, event_date_for_record_from_db)
                    if int(id) in row_from_db and ced_db_match:
                        # print("\nValidating row", ced_row_num, "(DB row:", db_row_num, ")In file ", file, " for ", str(search_column), ":", id)
                        self.val_imp_log.info("Validating row " +str(ced_row_num)+ "(DB row:" +str(db_row_num)+ ")In file " +str(file)+ " for " +str(
                            search_column)+ " : "+ str(id) + " & unique ID :" + str(unique_id_from_db))
                        device_id = None
                        # cust_property_data = read_xml.read_custom_properties(row_from_db[db_column_names.index('CUSTOM_PROPERTIES')])
                        for col_index, col_value in enumerate(row_from_db):
                            try:
                                ced_value = ced_data[id][ced_row_num][col_index]
                                if ced_value == "":
                                    ced_value = 'None'
                                db_value = col_value
                                date_format = "%d-%b-%Y %H:%M:%S"

                                col_name = all_columns_from_ced[col_index].replace("null as ",'')
                                if all_columns_from_ced[col_index].replace("null as ",'') in custom_columns.values():

                                    Status = "--- SKIPPING CUSTOM COLUMN FOR NOW ---"
                                    # self.val_imp_log.info(Status)
                                    continue
                                elif col_name in device_attributes:
                                    user_agent = row_from_db[ced_columns_from_file[event_type].index('USER_AGENT_STRING')]
                                    riid = row_from_db[index_of_unique_id]
                                    if device_id == None and self.path.dataSeeded == False:
                                        device_id = DeviceDetails.get_device_id_for_riid(self, device_ids, device_data, riid, event_type)
                                    # if device_id != None:
                                    Status = ValidateDataImpl.validate_device_data(self,device_ids, device_data, id,device_id,db_row_num, col_index,
                                                                                   col_name, file,ced_value,user_agent)
                                    # self.val_imp_log.info(Status)

                                elif type(db_value) == datetime:
                                    formatTimeInPST = pytz.timezone('US/Pacific').localize(db_value)
                                    convertedToUTC = formatTimeInPST.astimezone(pytz.timezone('UTC'))

                                    if CEDDatesInAccountTZ:
                                        if ced_value != None:
                                            ced_value = datetime.strptime(ced_value, '%d-%b-%Y %H:%M:%S')
                                            ced_value = CommonFunctions.covert_acc_tz_to_utc(ced_value,acc_timezone)
                                    if columns_to_be_queried_from_db[col_index] != 'EVENT_STORED_DT':
                                        if ced_value == str(convertedToUTC.strftime(date_format)):  # for event_captured_Dt is which is in PST,
                                            Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                            # self.val_imp_log.info(Status)

                                        else:
                                            Status = "row=" + str(db_row_num) + ",col=" + str(col_index) + ": Data for column " + \
                                                     columns_to_be_queried_from_db[col_index] + " is NOT matching. Data_In_CED:" + str(
                                                ced_value) + " & Data_in_DB:" + str(convertedToUTC.strftime(date_format))
                                            CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                                            # self.val_imp_log.info(Status)

                                    elif ced_value == str(db_value.strftime(date_format)):
                                        Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                        # self.val_imp_log.info(Status)

                                    else:
                                        Status = "row=" + str(db_row_num) + ",col=" + str(col_index) + ": Data for column " + columns_to_be_queried_from_db[col_index] + " is NOT matching. Data_In_CED:" + str(ced_value) + " & Data_in_DB:" + str(
                                            db_value.strftime(date_format))
                                        CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                                        # self.val_imp_log.info(Status)

                                elif ced_value == str(db_value) or ced_value == "Unknown" or ced_value == "UI - Data Viewer" or ced_value == "Unsubscribe Page/Link":
                                    Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                    # self.val_imp_log.info(Status)

                                elif type(db_value) != str and db_value != None and ced_value.isdigit():
                                    if float(ced_value) == float(db_value):  # handling numeric data coz sometimes 10 == 10.0 fails..
                                        Status = columns_to_be_queried_from_db[col_index] + "= Pass"
                                        # self.val_imp_log.info(Status)
                                else:
                                    Status = "row=" + str(db_row_num) + ",col=" + str(col_index) + ": Data for column " + str(columns_to_be_queried_from_db[col_index]) + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(db_value)
                                    CommonFunctions.write_results(self, file, id, ced_value, db_value, Status)
                                    # self.val_imp_log.info(Status)

                            except Exception as e:
                                Status = '*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****\n"
                                # self.val_imp_log.info(Status)

                            if "Pass" in str(Status):
                                row_error.append(True)
                            else:
                                row_error.append(False)
                        row_status=False if False in row_error else True

                    else:
                        pass
                    db_row_num += 1

                if Status == None:
                    row_status="Skip Reason "+str(event_type)+" : No matching record found in DB for row "+str(ced_row_num)+" (from CED) of "+str(primary_key)+" : "+ str(id) +"_1"
                    self.val_imp_log.info(row_status)
                ced_row_num += 1
                dreport[id].append(row_status)
        return dreport

    def validate_device_data(self,device_ids, device_data, id, device_id,db_row_num, col_index, col_name, file,ced_value,user_agent):
        if self.path.dataSeeded == True:
            os_vendor,operating_system, device_type, browser, browser_type = DeviceDetails.get_device_details(user_agent)
        elif device_id != None:
            browser_type, os_vendor, operating_system, device_type, browser = [row for row in device_data[device_id][0]]
        else:
            browser_type, os_vendor, operating_system, device_type, browser = None,None,None,None,None

        status = None
        if col_name == "BROWSER_TYPE_INFO":
            status = ValidateDataImpl.check_if_data_matches(self,db_row_num, col_index, file, col_name, id, ced_value, browser_type)

        elif col_name == "BROWSER_INFO":
            status = ValidateDataImpl.check_if_data_matches(self,db_row_num, col_index, file, col_name, id, ced_value, browser)

        elif col_name == "OS_VENDOR_INFO":
            status = ValidateDataImpl.check_if_data_matches(self,db_row_num, col_index, file, col_name, id, ced_value, os_vendor)

        elif col_name == "OPERATING_SYSTEM_INFO":
            status = ValidateDataImpl.check_if_data_matches(self,db_row_num, col_index, file, col_name, id, ced_value, operating_system)

        elif col_name == "DEVICE_TYPE_INFO":
            status = ValidateDataImpl.check_if_data_matches(self,db_row_num, col_index, file, col_name, id, ced_value, device_type)

        return status

    def check_if_data_matches(self, row_num,col_num, file,column_name,id, ced_value, db_value):
        if ced_value == str(db_value):
            Status = column_name + "= Pass"
            # self.val_imp_log.info(Status)
        else:
            Status = "row=" + str(row_num) + ",col=" + str(col_num) + ": Data for column " + str(column_name) + " is NOT matching. Data_In_CED:" + ced_value + " & Data_in_DB:" + str(db_value)
            CommonFunctions.write_results(self, file, id, ced_value, str(db_value), Status)
            # self.val_imp_log.info(Status)
        return Status

    def add_remove_column_for_query(self, all_columns_from_ced,column_to_be_inserted):
        try:
            duplicate_column = ValidateDataImpl.check_for_duplicate_custom_column(self, all_columns_from_ced, column_to_be_inserted)
            for key in sorted(column_to_be_inserted):
                try:
                    if type(column_to_be_inserted) == defaultdict or type(column_to_be_inserted) == dict:
                        col = column_to_be_inserted[key]
                        column_to_be_removed_index = all_columns_from_ced.index(col)
                    else:
                        col = key
                        column_to_be_removed_index = all_columns_from_ced.index(col)

                    if duplicate_column:
                        if duplicate_column == all_columns_from_ced[column_to_be_removed_index]:
                            if type(column_to_be_inserted) == defaultdict or type(column_to_be_inserted) == dict:
                                deleted_column = column_to_be_inserted.pop(key, duplicate_column +" Not in custom columns")
                                continue
                            else:
                                # deleted_column = column_to_be_inserted.pop(column_to_be_inserted.index(duplicate_column))
                                # continue
                                pass
                    all_columns_from_ced.insert(column_to_be_removed_index, "null as "+ str(col))
                    all_columns_from_ced.pop(column_to_be_removed_index+1)
                except ValueError as val_error:
                    message = "*** Undable to add dummy column (null as " + str(col) + ") and replace old column in query"
                    self.val_imp_log.error('%s - ERROR ON LINE %s , %s : %s ***' % (message, format(sys.exc_info()[-1].tb_lineno), type(val_error).__name__, val_error))
        except Exception as e:
            message = "Error in inserting dummy columns for sql query " + str(e)
            self.val_imp_log.error('%s +  - ERROR ON LINE %s , %s : %s ***' % (message, format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e))
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
                        try:
                            index_of_current_col = all_columns_from_ced.index(custom_column_names[i])
                            index_of_next_col = all_columns_from_ced.index(custom_column_names[i+1])
                            if (index_of_next_col-index_of_current_col) != 1:
                                if index_of_next_col < index_of_current_col:
                                    duplicate_col_index = index_of_next_col
                                    duplicate_col = all_columns_from_ced[duplicate_col_index]
                                    return
                                else:
                                    # duplicate_col_index = all_columns_from_ced.index(index_of_current_col)
                                    duplicate_col_index = index_of_current_col
                                    duplicate_col = all_columns_from_ced[duplicate_col_index]
                                    return duplicate_col
                        except ValueError as val_error:
                            message = "*** Undable to Identify duplicate column"
                            self.val_imp_log.error('%s - ERROR ON LINE %s , %s : %s ***' % (message, format(sys.exc_info()[-1].tb_lineno), type(val_error).__name__, val_error))
                except Exception as e:
                    message = "Error in identifying duplicate custom columns. Error :: " + str(e)
                    self.val_imp_log.error('%s +  - ERROR ON LINE %s , %s : %s ***' % (message, format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e))
                    self.val_imp_log.error(traceback.format_exc())
            else:
                pass
        return duplicate_col

    def check_for_match(self, ced_value, db_value,ced_stored_dt=None, db_stored_dt=None):
        if str(ced_value)==str(db_value) and ced_stored_dt==db_stored_dt:
            return True
        else:
            return False
