#https://github.com/thinkwelltwd/device_detector
from collections import defaultdict

from Implementations.setup import Setup
from device_detector import DeviceDetector
from ConfigFiles import paths, logger_util

class DeviceDetails(Setup):
    def __init__(self,test_class_name):
        super().__init__(test_class_name)
        self.dev_details_log = logger_util.get_logger(test_class_name +" :: " +__name__)

    def get_device_details(user_agent_string = None):
        # ua = 'Mozilla/5.0 (Linux; Android 4.3; C5502 Build/10.4.1.B.0.101) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.136 Mobile Safari/537.36'
        # ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 SA/61.0'

        # Parse UA string and load data to dict of 'operating_system', 'client', 'device' keys
        if not user_agent_string is None:
            try:
                device = DeviceDetector(user_agent_string).parse()

                operating_system = device.os_name()
                device_type = device.device_type()
                browser = device.client_name()
                browser_type = device.client_type()
                manfacturer = None

                print(operating_system, device_type, browser, browser_type)
                if operating_system.lower == 'windows':
                    manfacturer = "Microsoft"
                if operating_system.lower == 'mac':
                    manfacturer = "Apple"
                if operating_system.lower == 'Android':
                    manfacturer = "Google"

                return manfacturer,operating_system, device_type, browser, browser_type

            except Exception as e:
                print(e)

        else:
            print("Blank user agent supplied")
            exit(1)

    def get_device_attributes_direct_query(self,ua, account_schema,event_type,  riid,):
        if event_type.lower() == 'open':
            query_for_device_id = "SELECT NEW_DEVICE_ID FROM " + str(account_schema)+ "_CUST.PROFILE_DEVICE_SUMMARY where RIID_ = " +str(riid)+ " and LAST_CLICK_DATE is null"
        if event_type.lower() == 'click' or event_type.lower() == 'convert':
            query_for_device_id = "SELECT NEW_DEVICE_ID FROM " + str(account_schema)+ "_CUST.PROFILE_DEVICE_SUMMARY where RIID_ = " +str(riid)+ " and LAST_CLICK_DATE is not null"

        curs = Setup.init_db_connection(self,"syslocalCust")
        curs.execute(query_for_device_id)
        device_id = curs.fetchone()

        query_for_device_data = "select * from V_DEVICE_ATTR_VALUE where  DEVICE_ID= " + str(device_id[0])
        curs.execute(query_for_device_data)
        device_data = [row for row in curs.fetchone()]
        browser_type, vendor, os, device, browser = [data for data in device_data[1:]]
        print(device_data)
        print(browser_type, vendor, os, device, browser)
        curs.close()
        return vendor, os, device,browser,browser_type

    def get_device_attributes(self,account_schema):
        # if event_type.lower() == 'open':
        self.dev_details_log.info("*** Getting device attributes details******")
        query_for_device_id = "SELECT RIID_,NEW_DEVICE_ID,LAST_CLICK_DATE,LAST_OPEN_DATE FROM " + str(account_schema)+ "_CUST.PROFILE_DEVICE_SUMMARY"
        # if event_type.lower() == 'click' or event_type.lower() == 'convert':
        #     query_for_device_id = "SELECT NEW_DEVICE_ID FROM " + str(account_schema)+ "_CUST.PROFILE_DEVICE_SUMMARY where RIID_ = " +str(riid)+ " and LAST_CLICK_DATE is not null"

        with Setup.init_db_connection(self,"syslocalCust") as syslocal_cust:
            syslocal_cust.execute(query_for_device_id)
            device_ids = defaultdict(list)
            # d_id = []
            d_id = [row for row in syslocal_cust.fetchall()]
            for row_num in range(len(d_id)):
                riid = d_id[row_num][0]
                details_for_riid = d_id[row_num][1:]
                device_ids[riid].append(details_for_riid)
                # device_ids['device_id'].append(d_id[i][1])
                # device_ids['last_click_date'].append(d_id[i][2])
                # device_ids['last_open_date'].append(d_id[i][3])

            query_for_device_data = "select * from V_DEVICE_ATTR_VALUE"
            syslocal_cust.execute(query_for_device_data)

            d_data = [row for row in syslocal_cust.fetchall()]
            device_data = defaultdict(list)

            for row_num in range(len(d_data)):
                riid = d_data[row_num][0]
                attribute_details_for_riid = d_data[row_num][1:]
                device_data[riid].append(attribute_details_for_riid)
                # device_data['browser_type'].append(d_data[i][1])
                # device_data['os_vendor'].append(d_data[i][2])
                # device_data['operating_system'].append(d_data[i][3])
                # device_data['device_type'].append(d_data[i][4])
                # device_data['browser'].append(d_data[i][5])
            # browser_type, vendor, os, device, browser = [data for data in device_data[1:]]
            # syslocal_cust.close()
        return device_ids, device_data

    def get_device_id(self,device_ids, device_data, riid,event_type):
        browser_type, os_vendor, operating_system, device_type, browser = None,None,None,None,None
        try:
            device_id = None
            device_id_index = 0
            last_click_date_index = 1   #1 holds data for last_click_date in dictionary
            last_open_date_index = 2    #2 holds data for last_open_date in dictionary
            if "open" in event_type.lower():
                for row in range(len(device_ids[riid])):
                    last_click_date = device_ids[riid][row][last_click_date_index]
                    if last_click_date is None:  #last_click_date is null
                        device_id = device_ids[riid][row][device_id_index]
                        # print(device_id)
            elif "click" in event_type.lower()  or "convert" in event_type.lower():
                for row in range(len(device_ids[riid])):
                    last_click_date = device_ids[riid][row][last_click_date_index]
                    if last_click_date is not None:  # last_click_date is null
                        device_id = device_ids[riid][row][device_id_index]
            return device_id
            # browser_type, os_vendor,operating_system,device_type,browser = [row for row in device_data[device_id][0]]
        except IndexError as e:
            self.dev_details_log.info("--- SKIPPING AS THERE ARE NO DEVICE DETAILS AVAILABLE FOR RIID : " + str(riid))
            pass
        # print(browser_type, os_vendor,operating_system,device_type,browser)
        # return browser_type, os_vendor, operating_system, device_type, browser


# ua = "Mozilla/5.0 (Linux; Android 4.3; C5502 Build/10.4.1.B.0.101) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.136 Mobile Safari/537.36"
# if __name__ == '__main__':
    # device_ids, device_data = DeviceDetails.get_device_attributes("dataqa1")
    # DeviceDetails.get_data(device_ids, device_data, 8121,"open")