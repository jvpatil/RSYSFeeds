from Implementations.setup import *
from Implementations.ced_functions import *
from Implementations.db_fuctions import *


class ValidateCount(CEDFunctions, CommonFunctions, DBFunctions):
    # accountName = 'ipush'
    eventSchema = "Event"
    custSchema = "Cust"
    CEDDatesInAccountTZ = "False"
    resultFile = None

    def verifyCount(self):
        accountName = "ipush"
        if len(sys.argv) > 1:
            accountName = sys.argv[1]
        elif accountName is not None:
            accountName = accountName
        else:
            accountName = input("\nPLEASE PROVIDE THE ACCOUNT NAME  :: ")
        cedFiles = CommonFunctions.find_files(self)
        # curs = CEDFunctions.init_db_connection(self, self.accountName + "Event")
        curs = CEDFunctions.init_db_connection(self, "syslocalEvent")

        for cedFile in cedFiles:
            if not CommonFunctions.is_file_empty(self, cedFile):
                logging.info("Started processing file %s", cedFile)
                IDs, luniqueIDs, dEventStoredDateFromCED, searchColumn, eventType = CEDFunctions.get_ids_from_ced(self, cedFile)
                dCountFromCED = CEDFunctions.get_count_from_ced(self, IDs, luniqueIDs)
                dCountFromDB, dEventDatesFromDB = DBFunctions.get_count_from_db(self, curs, accountName,
                                                                                      self.eventSchema,
                                                                                      eventType, searchColumn, luniqueIDs,
                                                                                      dEventStoredDateFromCED)
                dEventsProcessed, dEventsToProcess = DBFunctions.get_missing_events(self, curs, accountName,
                                                                                      self.eventSchema,searchColumn, eventType,
                                                                                  dEventStoredDateFromCED, dEventDatesFromDB,
                                                                                  dCountFromDB, self.CEDDatesInAccountTZ)
                resultFile = CommonFunctions.compare_counts(self, cedFile, searchColumn, dCountFromCED, dCountFromDB,
                                                                dEventsToProcess, dEventsProcessed)
                print("Validation of count completed")
        print("\n***Result in File :", resultFile, "***")
        if not curs.close():
            print("\nClosed the connection to ", curs)
        CEDFunctions.get_run_time(self)

if __name__ == '__main__':
    ValidateCount().verifyCount()
