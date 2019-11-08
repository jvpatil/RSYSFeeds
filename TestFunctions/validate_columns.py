import sys

from Implementations.setup import Setup
from Implementations.ced_functions import CEDFunctions
from Implementations.db_fuctions import DBFunctions
from Implementations.common_functions import CommonFunctions
from TestFunctions.OverwrittenColumnFromDB import ColumnsFromDB


class ValidateColumns(CommonFunctions):
    # def __init__(self, *args):
    #     self.account_name = args
    # account_name = "ipush"
    # account_id = ""

    def verifyColumns(self):
        accountName = "ipush"
        if len(sys.argv) > 1:
            account_name = sys.argv[1]
        elif accountName is not None:
            account_name = accountName
        else:
            account_name = input("\n***PLEASE PROVIDE THE ACCOUNT NAME  :: ")

        ced_files = CEDFunctions.find_files(self)
        account_id = CommonFunctions.get_account_id(self,ced_files[0])
        curs = Setup.init_db_connection(self,"sysAdmin")
        ced_columns_from_db,built_in_headers_from_db = DBFunctions.get_headers_from_db(self, curs, account_id)
        result_file=CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_db, "ced_columns_from_db")
        CommonFunctions.close_db_connection(self, curs)
        # print(result_file)
        # ced_columns_from_db,built_in_headers_from_db = ColumnsFromDB()

        ced_columns_from_podconfig = CommonFunctions.get_headers_from_podconfig(self)
        result_file= CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_podconfig, "ced_columns_from_podconfig")
        # print(result_file)

        curs = Setup.init_db_connection(self, "syslocalCust")
        email_columns_by_id, email_custom_columns, sms_columns_by_id, sms_custom_columns = CommonFunctions.get_custom_properties(self, curs,account_name)
        CommonFunctions.close_db_connection(self, curs)

        for file in ced_files:
            if  not CommonFunctions.is_file_empty(self,file):
                print("\nVALIDATING COLUMNS FOR CED FILE ::", file)
                ced_columns_from_file = CommonFunctions.get_headers_from_ced(self,file)
                CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_file,"ced_columns_from_file")
                CommonFunctions.validate_columns_and_save_result(self,account_name,account_id, file, ced_columns_from_file,
                                                                 ced_columns_from_db,built_in_headers_from_db, ced_columns_from_podconfig, email_custom_columns,
                                                                 sms_custom_columns)
                print("Validation Completed")
        CEDFunctions.get_run_time(self)
        return

if __name__ == '__main__':
    ValidateColumns().verifyColumns()