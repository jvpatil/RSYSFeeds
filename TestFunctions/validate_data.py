import sys
from collections import defaultdict
from datetime import datetime

from Implementations import generate_html_report
from Implementations.setup import Setup
from Implementations.ced_functions import CEDFunctions
from Implementations.common_functions import CommonFunctions
from Implementations.device_details import DeviceDetails
# import Implementations.ssh as SSH
# import Implementations.ssh_tunnel as SSH
from Implementations.validate_data import ValidateDataImpl
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
            # sysadmin_curs = self.init_db_connection(self, "sysAdmin")
            sysadmin_curs = self.init_db_connection("sysAdmin")
            acc_timezone = self.get_account_timezone_info(sysadmin_curs, account_name)
            self.close_db_connection(sysadmin_curs)
        else:
            acc_timezone = paths.accountTimeZone

        # syslocalCust_curs = self.init_db_connection(self, "syslocalCust")
        syslocalCust_curs = self.init_db_connection("syslocalCust")
        email_columns_by_id, email_custom_columns, sms_columns_by_id, sms_custom_columns = self.get_custom_columns(syslocalCust_curs, account_name)
        self.close_db_connection(syslocalCust_curs)

        # syslocalEvent_curs = self.init_db_connection(self, "syslocalEvent")
        syslocalEvent_curs = self.init_db_connection("syslocalEvent")
        ced_files = CEDFunctions.find_files(self)
        status_report = defaultdict(list)
        empty_files=[]
        iteration = 0
        for file in ced_files:
            self.clear()
            barStatus, fileStatus = self.status_progress(iteration + 1, len(ced_files), file)
            self.print_status(barStatus, fileStatus)
            if not self.is_file_empty(file):
                delimiter, qoutechar = self.get_file_info(file)
                ced_columns_from_file = self.get_headers_from_ced(file,delimiter, qoutechar)
                IDs, unique_IDs, event_stored_date, search_column, event_type = self.get_ids_from_ced(file,delimiter,qoutechar)
                ced_data, index_Of_stored_date = self.read_ced_data_for_validation(file, unique_IDs, search_column,delimiter,qoutechar)
                report = self.validate_data_from_ced_progress(barStatus, fileStatus,syslocalEvent_curs, file, search_column, ced_data, index_Of_stored_date,
                                                             ced_columns_from_file, event_stored_date, event_type,account_name,email_custom_columns,
                                                              sms_custom_columns,
                                                              self.CEDDatesInAccountTZ,acc_timezone)
                status_report[file]=report
            else:
                empty_files.append(file)
            iteration += 1
        if not syslocalEvent_curs.close():
            print("\nClosed the connection to ", syslocalEvent_curs)

        self.clear()
        end_time = datetime.now()
        self.print_status(barStatus)
        timeTaken = (end_time - start_time).total_seconds()
        total_time = Setup.get_run_time(timeTaken)
        self.print_results_of_data_validation(status_report,total_time,len(ced_files),empty_files)
        generate_html_report.generate_report(status_report, total_time, len(ced_files), empty_files, "validate_data")


if __name__ == '__main__':
    TestValidateData().test_validate_data()

