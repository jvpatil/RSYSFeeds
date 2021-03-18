import sys
from collections import defaultdict
from datetime import datetime

from Implementations import generate_html_report
from Implementations.device_details import DeviceDetails
from Implementations.setup import Setup
from Implementations.ced_functions import CEDFunctions
from Implementations.db_fuctions import DBFunctions
from Implementations.common_functions_progress import CommonFunctions
from TestFunctions.OverwrittenColumnFromDB import ColumnsFromDB
from ConfigFiles import paths, logger_util

class ValidateColumns(DBFunctions,CommonFunctions,DeviceDetails):
    def __init__(self):
        self.test_class_name =  __class__.__name__
        super().__init__(self.test_class_name)

    def verifyColumns(self):
        start_time = datetime.now()
        if len(sys.argv) > 1:
            account_name = sys.argv[1]
        elif paths.account_name.isalnum():
            account_name = paths.account_name
        else:
            account_name = input("\n***PLEASE PROVIDE THE ACCOUNT NAME  :: ")

        ced_files = CEDFunctions.find_files(self)
        account_id = CommonFunctions.get_account_id(self,ced_files[0])
        curs = Setup.init_db_connection(self,"sysAdmin")
        curs = Setup.start_db_connection(self,paths.pod,"sysAdmin")
        ced_columns_from_db,built_in_headers_from_db = DBFunctions.get_headers_from_db(self, curs, account_id)
        result_file=CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_db, "ced_columns_from_db")
        CommonFunctions.close_db_connection(self, curs)

        ced_columns_from_podconfig = CommonFunctions.get_headers_from_podconfig(self)
        result_file= CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_podconfig, "ced_columns_from_podconfig")

        # syslocalCust_curs = Setup.init_db_connection(self, "syslocalCust")
        syslocalCust_curs = Setup.start_db_connection(self,paths.pod,"syslocalCust")
        email_columns_by_id, email_custom_columns, sms_columns_by_id, sms_custom_columns = CommonFunctions.get_custom_columns(syslocalCust_curs, account_name)
        CommonFunctions.close_db_connection(self, syslocalCust_curs)

        status_report = defaultdict(list)
        empty_files = []
        iteration = 0
        for file in ced_files:
            barStatus, fileStatus =CommonFunctions.status_progress(iteration + 1, len(ced_files), file)
            CommonFunctions.clear(self)
            CommonFunctions.print_status(barStatus)
            # CommonFunctions.print_status(barStatus, fileStatus)
            if  not CommonFunctions.is_file_empty(self,file):
                delimiter,qoute_char = CommonFunctions.get_file_info(self,file)
                # ced_columns_from_file = CommonFunctions.get_headers_from_ced(self,file)
                ced_columns_from_file = CommonFunctions.get_headers_from_ced(self,file,delimiter,qoute_char)
                CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_file,"ced_columns_from_file")
                report = CommonFunctions.validate_columns_and_save_result(self,account_name,account_id, file, ced_columns_from_file,
                                                                 ced_columns_from_db,built_in_headers_from_db, ced_columns_from_podconfig, email_custom_columns,
                                                                 sms_custom_columns)
                status_report[file] = report
            else:
                empty_files.append(file)
            iteration += 1
        # if not syslocalCust_curs.close():
        #     print("\nClosed the connection to ", syslocalCust_curs)

        CommonFunctions.clear(self)
        end_time = datetime.now()
        CommonFunctions.print_status(barStatus)
        timeTaken = (end_time - start_time).total_seconds()
        total_time = Setup.get_run_time(self, timeTaken)
        CommonFunctions.print_results_of_column_validation(self, status_report, total_time, len(ced_files), empty_files)
        generate_html_report.generate_report(status_report, total_time, len(ced_files), empty_files, "validate_columns")

if __name__ == '__main__':
    ValidateColumns().verifyColumns()