import traceback
import unittest
import sys
from Implementations.setup import Setup
from Implementations.ced_functions import CEDFunctions
from Implementations.db_fuctions import DBFunctions
from Implementations.common_functions import CommonFunctions
# import Implementations.ssh as SSH
# import Implementations.ssh_tunnel as SSH
from Implementations.validate_data import validate_data_from_ced

import pytz
import logging


class ValidateData(CommonFunctions):
    CEDDatesInAccountTZ = True

    def validate_data(self):
        acc_name = "ipush"
        if len(sys.argv) > 1:
            account_name = sys.argv[1]
        elif acc_name is not None:
            account_name = acc_name
        else:
            account_name = input("\n***PLEASE PROVIDE THE ACCOUNT NAME  :: ")
        # curs = CommonFunctions.init_db_connection(self, account_name + "Event")
        # SSH.getSSHConnection()
        curs = CommonFunctions.init_db_connection(self, "syslocalCust")
        email_columns_by_id, email_custom_columns, sms_columns_by_id, sms_custom_columns = CommonFunctions.get_custom_properties(self, curs,account_name)
        CommonFunctions.close_db_connection(self, curs)

        curs = CommonFunctions.init_db_connection(self, "syslocalEvent")
        # print("Connection Returnted is:", curs)
        ced_files = CEDFunctions.find_files(self)

        for file in ced_files:
            print("\n*** START PROCESSING FILE :: " +str(file)+ " ***")
            if not CommonFunctions.is_file_empty(self, file):
                ced_columns_from_file = CEDFunctions.get_headers_from_ced(self, file)
                IDs, unique_IDs, event_stored_date, search_column, event_type = CEDFunctions.get_ids_from_ced(self, file)
                # ced_data, index_Of_stored_date = CEDFunctions.read_data_from_ced(self, file, unique_IDs, search_column)
                ced_data, index_Of_stored_date = CEDFunctions.read_ced_data_for_validation(self, file, unique_IDs, search_column)
                validate_data_from_ced(self, curs, file, search_column, ced_data, index_Of_stored_date,
                                                       ced_columns_from_file, event_stored_date, event_type,account_name,email_custom_columns,sms_custom_columns,self.CEDDatesInAccountTZ)
                # DBFunctions.extract_custom_properties_data(self,curs)
        if not curs.close():
            print("\nClosed the connection to ", curs)
        CEDFunctions.get_run_time(self)
        return

if __name__ == '__main__':
    ValidateData().validate_data()

