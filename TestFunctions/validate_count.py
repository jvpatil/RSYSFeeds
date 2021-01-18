from Implementations.setup import *
from Implementations.ced_functions import *
from Implementations.db_fuctions import *
from ConfigFiles import logger_util
from Implementations import generate_html_report


class ValidateCount(CEDFunctions,DBFunctions, CommonFunctions):
    def __init__(self):
        self.test_class_name = __class__.__name__
        super().__init__(self.test_class_name)

    # accountName = 'ipush'
    eventSchema = "Event"
    custSchema = "Cust"
    CEDDatesInAccountTZ = "False"
    resultFile = None

    def verifyCount(self):
        start_time = datetime.now()
        if len(sys.argv) > 1:
            account_name = sys.argv[1]
        elif paths.account_name.isalnum():
            account_name = paths.account_name
        else:
            account_name = input("\n***PLEASE PROVIDE THE ACCOUNT NAME  :: ")

        ced_files = CommonFunctions.find_files(self)
        # syslocalEvent_curs = CEDFunctions.init_db_connection(self, "syslocalEvent")
        syslocalEvent_curs = CEDFunctions.start_db_connection(self,paths.pod, "syslocalEvent")

        status_report = defaultdict(list)
        empty_files = []
        iteration = 0
        for file in ced_files:
            barStatus, fileStatus = CommonFunctions.status_progress(iteration + 1, len(ced_files), file)
            CommonFunctions.clear(self)
            CommonFunctions.print_status(barStatus, fileStatus)
            if not CommonFunctions.is_file_empty(self, file):
                IDs, luniqueIDs, dEventStoredDateFromCED, searchColumn, eventType = CEDFunctions.get_ids_from_ced(self, file)
                dCountFromCED = CEDFunctions.get_count_from_ced(self, IDs, luniqueIDs)
                dCountFromDB, dEventDatesFromDB = DBFunctions.get_count_from_db(self, syslocalEvent_curs, account_name,
                                                                                      self.eventSchema,
                                                                                      eventType, searchColumn, luniqueIDs,
                                                                                      dEventStoredDateFromCED)
                dEventsProcessed, dEventsToProcess = DBFunctions.get_missing_events(self, syslocalEvent_curs, account_name,
                                                                                      self.eventSchema,searchColumn, eventType,
                                                                                  dEventStoredDateFromCED, dEventDatesFromDB,
                                                                                  dCountFromDB, self.CEDDatesInAccountTZ)
                report = CommonFunctions.compare_counts(self, file, searchColumn, dCountFromCED, dCountFromDB,
                                                                dEventsToProcess, dEventsProcessed)
                status_report[file] = report
            else:
                empty_files.append(file)
            iteration += 1
        if not syslocalEvent_curs.close():
            print("\nClosed the connection to ", syslocalEvent_curs)

        CommonFunctions.clear(self)
        end_time = datetime.now()
        CommonFunctions.print_status(barStatus)
        timeTaken = (end_time - start_time).total_seconds()
        total_time = Setup.get_run_time(self, timeTaken)
        CommonFunctions.print_results_of_count_validation(self, status_report, total_time, len(ced_files), empty_files)
        # generate_html_report.print_header()
        generate_html_report.generate_report(status_report, total_time, len(ced_files), empty_files,"validate_count")

if __name__ == '__main__':
    ValidateCount().verifyCount()
