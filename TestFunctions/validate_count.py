from Implementations.setup import *
from Implementations.ced_functions import *
from Implementations.db_fuctions import *
from ConfigFiles import logger_util
from Implementations import generate_html_report


class ValidateCount(CEDFunctions,DBFunctions):
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

        ced_files = self.find_files()
        # syslocalEvent_curs = self.init_db_connection(self, "syslocalEvent")
        syslocalEvent_curs = self.start_db_connection(paths.pod, "syslocalEvent")

        status_report = defaultdict(list)
        empty_files = []
        iteration = 0
        for file in ced_files:
            barStatus, fileStatus = self.status_progress(iteration + 1, len(ced_files), file)
            self.clear()
            self.print_status(barStatus, fileStatus)
            if not self.is_file_empty(file):
                delimiter, qoutechar = self.get_file_info(file)
                IDs, luniqueIDs, dEventStoredDateFromCED, searchColumn, eventType = self.get_ids_from_ced(file,delimiter, qoutechar )
                dCountFromCED = self.get_count_from_ced(IDs, luniqueIDs)
                dCountFromDB, dEventDatesFromDB = self.get_count_from_db(syslocalEvent_curs, account_name,self.eventSchema,eventType, searchColumn, luniqueIDs,dEventStoredDateFromCED)
                dEventsProcessed, dEventsToProcess = self.get_missing_events(syslocalEvent_curs, account_name,self.eventSchema,searchColumn, eventType,
                                                                            dEventStoredDateFromCED, dEventDatesFromDB,dCountFromDB, self.CEDDatesInAccountTZ)
                report = self.compare_counts(file, searchColumn, dCountFromCED, dCountFromDB,dEventsToProcess, dEventsProcessed)
                status_report[file] = report
            else:
                empty_files.append(file)
            iteration += 1
        if not syslocalEvent_curs.close():
            print("\nClosed the connection to ", syslocalEvent_curs)

        self.clear()
        end_time = datetime.now()
        self.print_status(barStatus)
        timeTaken = (end_time - start_time).total_seconds()
        total_time = self.get_run_time(timeTaken)
        self.print_results_of_count_validation(status_report, total_time, len(ced_files), empty_files)
        # generate_html_report.print_header()
        generate_html_report.generate_report(status_report, total_time, len(ced_files), empty_files,"validate_count")

if __name__ == '__main__':
    ValidateCount().verifyCount()
