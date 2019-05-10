import traceback
import unittest

from BaseFunctions.setup import Setup
from BaseFunctions.ced_functions import CEDFunctions
from BaseFunctions.db_fuctions import DBFunctions
from BaseFunctions.common_functions import CommonFunctions

import pytz
import logging


class ValidateData(CommonFunctions):

    def validate_data(self):
        account_name = "progtm"
        curs = CommonFunctions.init_db_connection(self, account_name + "Event")
        print("Connection Returnted is:", curs)
        ced_files = CEDFunctions.find_files(self)

        for file in ced_files:
            ced_columns_from_file = CEDFunctions.get_headers_from_ced(self, file)
            IDs, unique_IDs, event_stored_date, search_column, event_type = CEDFunctions.get_ids_from_ced(self, file)
            ced_data, index_Of_stored_date = CEDFunctions.read_data_from_ced(self, file, unique_IDs, search_column)
            CommonFunctions.validate_data_from_ced(self, curs, file, search_column, ced_data, index_Of_stored_date,
                                                   ced_columns_from_file, event_stored_date, event_type)
            # DBFunctions.extract_custom_properties_data(self,curs)

        return


ValidateData().validate_data()
