from Implementations.setup import *
from Implementations.ced_functions import *
from Implementations.db_fuctions import *

class OmnitureFunctions(CEDFunctions, CommonFunctions, DBFunctions):

    def get_ids_from_metric_file(self, metric_file):
        file_headers = None
        IDs = None
        try:
            with open(os.path.join(CEDFunctions.input_file_path, metric_file), 'r') as f:
                content = f.readlines()

                file_headers = content[:1][0].strip().split("\t")  #strips new line at end and splits by TAB
                #reader 1st row (header row) which stored in content as list and 0th index. then split it by delimiter TAB
                # DATE,ID,SENT,DELIVERED,OPENED,CLICKED,UNSUBSCRIBED,TOTAL_BOUNCES = content[:1][0].split("\t")
                file_details = re.split(r"_", metric_file)
                run_date = file_details[0].strip('_')
                suite_name = file_details[1].strip('_')
                integration_number = file_details[2].strip('_')
                print(run_date, suite_name, integration_number)

                index_of_id = file_headers.index("Message ID")
                IDs = []
                metrics_data = defaultdict(list)


                for row in content[1:]:
                    data = {}
                    split_row = row.split("\t")
                    id = split_row[index_of_id]
                    IDs.append(split_row[index_of_id])
                    data["SENT"]=split_row[file_headers.index("Sent")]
                    data["DELIVERED"] = split_row[file_headers.index("Delivered")]
                    data["OPENED"] = split_row[file_headers.index("Opened")]
                    data["CLICKED"] = split_row[file_headers.index("Clicked")]
                    data["UNSUBSCRIBED"] = split_row[file_headers.index("Unsubscribed")]
                    data["BOUNCED"] = split_row[file_headers.index("Total Bounces")].strip()
                    metrics_data[id].append(data)
            print(IDs)
            print(metrics_data, end="")
        except Exception as e:
            print(e)
        return  IDs,metrics_data, file_headers

    def read_data_from_dw_db(self,curs, IDs,metrics_data, file_headers):

        for id in metrics_data:
            query = "SELECT count(*) FROM STG_RECIPIENT_SENT WHERE LAUNCH_ID= '"+id+"'"
            curs.execute(query)
            result = curs.fetchone()
            if str(result[0])== metrics_data[id][0]["SENT"]:
                print("SENT Count for ID :" , id, " Match with DB.")
            else:
                print("SENT Count for ID :" , id, " Does not Match with DB.  count_in_file =",metrics_data[id][0]["SENT"], " and count_in_db =", result[0])

