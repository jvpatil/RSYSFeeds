from BaseFunctions.setup import Setup
from BaseFunctions.ced_functions import CEDFunctions
from BaseFunctions.db_fuctions import DBFunctions
from BaseFunctions.common_functions import CommonFunctions


class ValidateColumns(CommonFunctions):
    account_name = "progtm"

    def verifyColumns(self):
        ced_files = CEDFunctions.find_files(self)
        account_id = CommonFunctions.get_account_id(self,ced_files[0])
        curs = Setup.init_db_connection(self,"sysAdmin")

        ced_columns_from_db = DBFunctions.get_headers_from_db(self, curs, self.account_name)
        result_file=CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_db, "ced_columns_from_db")
        print(result_file)

        ced_columns_from_podconfig = CommonFunctions.get_headers_from_podconfig(self)
        result_file= CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_podconfig, "ced_columns_from_podconfig")
        print(result_file)

        CommonFunctions.close_db_connection(self, curs)

        curs = Setup.init_db_connection(self, self.account_name + "Cust")
        email_columns_by_id, email_custom_columns, smsemail_columns_by_id, sms_custom_columns = \
            CommonFunctions.get_custom_properties(self, curs)

        CommonFunctions.close_db_connection(self, curs)

        for file in ced_files:
            print("\nVALIDATING COLUMNS FOR CED FILE ::", file)
            ced_columns_from_file = CommonFunctions.get_headers_from_ced(self,file)
            CommonFunctions.write_headers_to_file(self,account_id, ced_columns_from_file,"ced_columns_from_file")
            CommonFunctions.validate_columns_and_save_result(self,self.account_name, file, ced_columns_from_file,
                                                             ced_columns_from_db, ced_columns_from_podconfig, email_custom_columns,
                                                             sms_custom_columns)
        return

ValidateColumns().verifyColumns()