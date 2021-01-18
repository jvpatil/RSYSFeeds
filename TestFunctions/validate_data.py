import sys
from collections import defaultdict
from datetime import datetime

from Implementations import generate_html_report
from Implementations.setup import Setup
from Implementations.ced_functions import CEDFunctions
from Implementations.common_functions_progress import CommonFunctions
from Implementations.device_details import DeviceDetails
# import Implementations.ssh as SSH
# import Implementations.ssh_tunnel as SSH
from Implementations.validate_data_progress import ValidateDataImpl
# from ConfigFiles.paths import *
from ConfigFiles import paths, logger_util


class TestValidateData(ValidateDataImpl,CEDFunctions,DeviceDetails):
    def __init__(self):
        self.test_class_name =  __class__.__name__
        super().__init__(self.test_class_name)

    if paths.ExportInAccountTimeZone:
        CEDDatesInAccountTZ = paths.ExportInAccountTimeZone
    else:
        CEDDatesInAccountTZ = False

    def test_validate_data(self):
        start_time = datetime.now()
        if len(sys.argv) > 1:
            account_name = sys.argv[1]
        elif paths.account_name.isalnum():
            account_name = paths.account_name
        else:
            account_name = input("\n***PLEASE PROVIDE THE ACCOUNT NAME  :: ")

        if self.CEDDatesInAccountTZ:
            # sysadmin_curs = CommonFunctions.init_db_connection(self, "sysAdmin")
            sysadmin_curs = CommonFunctions.start_db_connection(self,paths.pod, "sysAdmin")
            acc_timezone = self.get_account_timezone_info(sysadmin_curs, account_name)
            CommonFunctions.close_db_connection(self, sysadmin_curs)
        else:
            acc_timezone = paths.accountTimeZone

        # syslocalCust_curs = CommonFunctions.init_db_connection(self, "syslocalCust")
        syslocalCust_curs = CommonFunctions.start_db_connection(self,paths.pod, "syslocalCust")
        email_columns_by_id, email_custom_columns, sms_columns_by_id, sms_custom_columns = CommonFunctions.get_custom_properties(self, syslocalCust_curs,account_name)
        CommonFunctions.close_db_connection(self, syslocalCust_curs)

        # syslocalEvent_curs = CommonFunctions.init_db_connection(self, "syslocalEvent")
        syslocalEvent_curs = CommonFunctions.start_db_connection(self,paths.pod, "syslocalEvent")
        ced_files = CEDFunctions.find_files(self)
        status_report = defaultdict(list)
        empty_files=[]
        iteration = 0
        for file in ced_files:
            CommonFunctions.clear(self)
            barStatus, fileStatus = CommonFunctions.status_progress(iteration + 1, len(ced_files), file)
            CommonFunctions.print_status(barStatus, fileStatus)
            if not CommonFunctions.is_file_empty(self, file):
                ced_columns_from_file = CEDFunctions.get_headers_from_ced(self, file)
                IDs, unique_IDs, event_stored_date, search_column, event_type = CEDFunctions.get_ids_from_ced(self, file)
                ced_data, index_Of_stored_date = CEDFunctions.read_ced_data_for_validation(self, file, unique_IDs, search_column)
                report = ValidateDataImpl.validate_data_from_ced_progress(self, barStatus, fileStatus,syslocalEvent_curs, file, search_column, ced_data, index_Of_stored_date,
                                                       ced_columns_from_file, event_stored_date, event_type,account_name,email_custom_columns,sms_custom_columns,self.CEDDatesInAccountTZ,acc_timezone)
                status_report[file]=report
            else:
                empty_files.append(file)
            iteration += 1
        if not syslocalEvent_curs.close():
            print("\nClosed the connection to ", syslocalEvent_curs)

        CommonFunctions.clear(self)
        end_time = datetime.now()
        CommonFunctions.print_status(barStatus)
        timeTaken = (end_time - start_time).total_seconds()
        total_time = Setup.get_run_time(self,timeTaken)
        CommonFunctions.print_results_of_data_validation(self,status_report,total_time,len(ced_files),empty_files)
        generate_html_report.generate_report(status_report, total_time, len(ced_files), empty_files, "validate_data")


if __name__ == '__main__':
    TestValidateData().test_validate_data()

