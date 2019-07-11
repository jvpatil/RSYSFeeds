from BaseFunctions.setup import Setup
from BaseFunctions.ced_functions import CEDFunctions
from BaseFunctions.db_fuctions import DBFunctions
from BaseFunctions.common_functions import CommonFunctions
from BaseFunctions.omniture import OmnitureFunctions

class Omniture(CommonFunctions):
    accountName = "ipush"
    def read_files(self):
        files = CEDFunctions.find_files(self)
        # print(files)
        for file in files:
            if 'Metric' in file:
                print("Found Omniture File ::", file)
                IDs,metrics_data, file_headers = OmnitureFunctions.get_ids_from_metric_file(self,file)
                curs = CEDFunctions.init_db_connection(self, self.accountName + "DM")
                OmnitureFunctions.read_data_from_dw_db(self, curs,IDs,metrics_data, file_headers)

Omniture().read_files()