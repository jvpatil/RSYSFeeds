from BaseFunctions.setup import *
from BaseFunctions.ced_functions import *
from BaseFunctions.db_fuctions import *


class ValidateCount(CEDFunctions, CommonFunctions, DBFunctions):
    accountName = 'qa1cloudc001'
    eventSchema = "Event"
    custSchema = "Cust"
    CEDDatesInAccountTZ = "True"

    def verifyCount(self):

        cedFiles = CommonFunctions.find_files(self)
        # curs = CEDFunctions.init_db_connection(self, self.accountName + "Event")
        curs = CEDFunctions.init_db_connection(self, "syslocalEvent")

        for cedFile in cedFiles:
            logging.info("Started processing file %s", cedFile)
            IDs, luniqueIDs, dEventStoredDateFromCED, searchColumn, eventType = CEDFunctions.get_ids_from_ced(self, cedFile)
            dCountFromCED = CEDFunctions.get_count_from_ced(self, IDs, luniqueIDs)
            dCountFromDB, dEventDatesFromDB = DBFunctions.get_count_from_db(self, curs, self.accountName,
                                                                                  self.eventSchema,
                                                                                  eventType, searchColumn, luniqueIDs,
                                                                                  dEventStoredDateFromCED)
            dEventsProcessed, dEventsToProcess = DBFunctions.get_missing_events(self, curs, self.accountName,
                                                                                  self.eventSchema,searchColumn, eventType,
                                                                              dEventStoredDateFromCED, dEventDatesFromDB,
                                                                              dCountFromDB, self.CEDDatesInAccountTZ)
            resultFile = CommonFunctions.compare_counts(self, cedFile, searchColumn, dCountFromCED, dCountFromDB,
                                                            dEventsToProcess, dEventsProcessed)
        print("\n***Result in File :", resultFile, "***")
        if not curs.close():
            print("\nClosed the connection to ", curs)
        CEDFunctions.get_run_time(self)

ValidateCount().verifyCount()
