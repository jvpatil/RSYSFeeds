from __future__ import print_function
from Implementations.setup import Setup
from Implementations.common_functions_progress import CommonFunctions

import sys
import os
import re
import traceback
from collections import defaultdict
from datetime import datetime
from ConfigFiles import paths,logger_util


class CEDFunctions(CommonFunctions):
    def __init__(self,test_class_name):
        super().__init__(test_class_name)
        self.ced_func_log = logger_util.get_logger(test_class_name +" :: " +__name__)

    # log = CommonFunctions.log
    if paths.input_files_path is not None:
        input_file_path = paths.input_files_path
    else:
        input_file_path = Setup.testfilespath

    # input_files_path = Setup.testfilespath
    result_files_path = Setup.resultFilePath
    runTime = datetime.now().strftime("%d%b%Y_%H.%M.%S")  # current time in ddmmyyyy_hh24mmss
    report = "Result_" + str(runTime) + ""

    # def getIndex(self,searchColumn,content):
    #     try:
    #         for row in content:
    #             row = row.strip().replace('\"', '')
    #             listOfColumns = re.split(',',row)
    #         indexOfSearchCol = listOfColumns.index(searchColumn)
    #         indexOfEventStoredDt = listOfColumns.index("EVENT_STORED_DT")
    #         # print("Index of Search COlumn is :", indexOfSearchCol, "and Index of Event Stored Date is :",
    #         indexOfEventStoredDt)
    #     except Exception as e:
    #         print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
    #         "*****\n")
    #         print(traceback.format_exc())
    #
    #     return indexOfSearchCol, indexOfEventStoredDt
    #
    # def getSearchColumn(self,cedFile):
    #
    #     searchKey = "LAUNCH_ID"
    #     if re.match(r'[0-9]+_' + 'OPT', cedFile):
    #         searchColumn = 'RIID'
    #     elif re.match(r'[0-9]+_' + 'SMS_OPT', cedFile):
    #         searchColumn = 'RIID'
    #     elif re.match(r'[0-9]+_' + 'FORM', cedFile):
    #         searchColumn = 'FORM_ID'
    #     elif re.match(r'[0-9]+_' + 'FORM_STATE', cedFile):
    #         searchColumn = 'FORM_ID'
    #     elif re.match(r'[0-9]+_' + 'PROGRAM', cedFile):
    #         searchColumn = 'PROGRAM_ID'
    #     elif re.match(r'[0-9]+_' + 'PROGRAM_STATE', cedFile):
    #         searchColumn = 'PROGRAM_ID'
    #     elif re.match(r'[0-9]+_' + 'HOLDOUT', cedFile):
    #         searchColumn = 'PROGRAM_ID'
    #     elif re.match(r'[0-9]+_' + 'PUSH_UNINSTALL', cedFile):
    #         searchColumn = 'RIID'
    #     else:
    #         searchColumn = searchKey
    #
    #     return searchColumn
    #
    # def findFiles(self):
    #     count = 0
    #     try:
    #         files = os.listdir(self.input_file_path)
    #         for fname in range(len(files)):
    #             count += 1
    #         print("\nThere are total", count, "files to processed:")
    #         print(*files, sep="\n")
    #     except Exception as e:
    #         print('\n*****ERROR ON LINE {}'.format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e,
    #         "*****\n")
    #         print(traceback.format_exc())
    #
    #     return files

    def get_ids_from_ced(self, ced_file):

        IDs = []
        event_stored_date = defaultdict(list)
        unique_IDs = defaultdict(list)

        try:
            with open(os.path.join(CEDFunctions.input_file_path, ced_file), 'r') as f:
                content = f.readlines()
            header_row = content[:1]
            event_type = re.split(r"\d+", ced_file)
            event_type = event_type[1].strip('_')

            search_column = self.get_search_column(ced_file)
            index_Of_search_column, index_Of_event_stored_date = self.get_index(search_column, ced_file)

            for row in content[1:]:
                row = row.strip().replace('\"', '')
                col_data = re.split(';|,|\t|\||""', row)
                id_value = col_data[index_Of_search_column]
                event_stored_time = col_data[index_Of_event_stored_date]
                IDs.append(id_value)
                event_stored_date[id_value].append(event_stored_time)

            unique_IDs = set(IDs)
            self.ced_func_log.info("There are %s records in file %s & the Unique %s are :: %s" %(len(IDs),ced_file ,search_column,len(unique_IDs)))

            return IDs, unique_IDs, event_stored_date, search_column, event_type
            # print("*** There are", len(IDs), "records in file ", ced_file, " & the Unique", search_column, "are :: ",
            #       len(unique_IDs)," :: ", unique_IDs)
            # logging.info("There are {}, records in file {} and the Unique {} are {}, {}".format(len(IDs), cedFile,
            # searchColumn, len(lUniqueIDs), lUniqueIDs))
            # print("\tThere are", len(IDs), "records in CED file ", ced_file, " & the Unique", search_column,
            #       "are :: ",  len(unique_IDs))

        except Exception as e:
            self.ced_func_log.error('*** ERROR ON LINE %s , %s : %s ***' % (format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e))



    def get_count_from_ced(self, IDs, uniqueIDs):
        d_count_from_ced = defaultdict(list)
        try:
            self.ced_func_log.info("Getting count from CED file.")
            for id in uniqueIDs:
                count = IDs.count(id)
                d_count_from_ced[id].append(count)
                # print("Count for the ID " +str(id) + " is : " + str(count) )
        except Exception as e:
            message = "*****ERROR ON LINE {}".format(sys.exc_info()[-1].tb_lineno), ",", type(e).__name__, ":", e, "*****"
            self.ced_func_log.info(message)
        return d_count_from_ced

    def read_data_from_ced(self,file, unique_IDs, search_column):
        print("*** Reading Data From File :: ", file , " for each ID's ***")
        self.ced_func_log.info("Reading Data From File :: ", file , " for each ID's")
        ced_data = defaultdict(list)

        for id in unique_IDs:
            with open(os.path.join(CEDFunctions.input_file_path, file), 'r') as f:
                content = f.readlines()

            for row in content[:1]:
                stripped_row = row.strip().replace('\"', '')
                all_columns = re.split(';|,|\t|""', stripped_row)
                for i in all_columns:
                    if i == 'EVENT_STORED_DT':
                        index_Of_stored_date = all_columns.index(i)
                        break
                # index_Of_stored_date[file].append(stored_date_index)

            for row in content[1:]:
                if id in row:
                    row = row.strip()
                    split_columns = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)',row)
                    # strippedRow = row.strip().replace('\"', '')
                    # strippedRow = row.strip()
                    pattern = ',|\t|""'
                    # all_columns_Of_row = re.split(pattern, strippedRow)
                    # all_columns_Of_row = (lambda col : col.strip('"') ,split_columns)
                    for eachColumn in split_columns:
                        eachColumn = eachColumn.strip('"')
                        ced_data[id].append(eachColumn)
            # all_data[id].append(event_data)

            # print("Data for ID ", id, " is:", allData)
            # print("Data for file ", cedFile," is:",allData[cedFile])
        # print("\nData is :\n",ced_data)
        return ced_data, index_Of_stored_date

    def read_ced_data_for_validation(self,file, unique_IDs, search_column):
        # print("*** Reading Data From File :: ", file , " ***")
        ced_data = defaultdict(lambda: defaultdict(list))
        index_Of_stored_date = None

        with open(os.path.join(CEDFunctions.input_file_path, file), 'r') as f:
            content = f.readlines()

            for row in content[:1]:
                stripped_row = row.strip().replace('\"', '')
                # all_columns = re.split(';|,|\t|""', stripped_row)
                all_columns = re.split(';|,|\t|\||""', stripped_row) # split column either by ; or , or | or \t or "
                for i in all_columns:
                    if i == 'EVENT_STORED_DT':
                        index_Of_stored_date = all_columns.index(i)
                        break
                # index_Of_stored_date[file].append(stored_date_index)

            for id in unique_IDs:
                rownum = 0
                for rowData in content[1:]:
                    if id in rowData:
                        rowData = rowData.strip()
                        split_columns = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)',rowData)
                        # split_columns = re.split('\|,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)',rowData)
                        for eachColumn in split_columns:
                            eachColumn = eachColumn.strip('"')
                            ced_data[id][rownum].append(eachColumn)
                        rownum +=1
                        # rownum = 0
        return ced_data, index_Of_stored_date

# cedFiles = CEDFunctions().findFiles()
# # CEDFunctions().getIndex()
# CEDFunctions().getIDsFromCED(cedFiles)
