from collections import defaultdict
ced_columns_from_db = defaultdict(list)
built_in_headers_from_db = defaultdict(list)

def ColumnsFromDB():
    # BOUNCE = ["ACCOUNT_ID","EVENT_TYPE_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL","EMAIL_FORMAT","BOUNCE_TYPE","REASON","REASON_CODE","SUBJECT","CONTACT_INFO","RECIPIENT_ORG_ID","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","CUSTOM_PROPERTIES"]
    # ced_columns_from_db['BOUNCE']=("ACCOUNT_ID","EVENT_TYPE_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL","EMAIL_FORMAT","BOUNCE_TYPE","REASON","REASON_CODE","SUBJECT","CONTACT_INFO","RECIPIENT_ORG_ID","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","CUSTOM_PROPERTIES")
    # ced_columns_from_db['CLICK']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL_FORMAT","OFFER_NAME","OFFER_NUMBER","OFFER_CATEGORY","OFFER_URL","USER_AGENT_STRING","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","BROWSER_TYPE_INFO","BROWSER_INFO","OS_VENDOR_INFO","OPERATING_SYSTEM_INFO","DEVICE_TYPE_INFO","REMOTE_ADDR","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['COMPLAINT']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL_FORMAT","REASON","EMAIL","EMAIL_ISP","COMPLAINER_EMAIL","SPAM_TYPE","CONTACT_INFO","COMPLAINT_DT","RECIPIENT_ORG_ID","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['CONVERT']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","SOURCE","EMAIL_FORMAT","OFFER_NAME","OFFER_NUMBER","OFFER_CATEGORY","OFFER_URL","ORDER_ID","ORDER_TOTAL","ORDER_QUANTITY","USER_AGENT_STRING","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","BROWSER_TYPE_INFO","BROWSER_INFO","OS_VENDOR_INFO","DEVICE_TYPE_INFO","OPERATING_SYSTEM_INFO","REMOTE_ADDR","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['OPEN']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL_FORMAT","USER_AGENT_STRING","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","BROWSER_TYPE_INFO","BROWSER_INFO","OS_VENDOR_INFO","OPERATING_SYSTEM_INFO","DEVICE_TYPE_INFO","REMOTE_ADDR","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['SENT']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL","EMAIL_ISP","EMAIL_FORMAT","OFFER_SIGNATURE_ID","DYNAMIC_CONTENT_SIGNATURE_ID","MESSAGE_SIZE","SEGMENT_INFO","CONTACT_INFO","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['SMS_CLICK']={"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","AGGREGATOR_ID","COUNTRY_CODE","MOBILE_CODE","MOBILE_NUMBER","OFFER_NUMBER","REMOTE_ADDR","USER_AGENT_STRING","BROWSER_TYPE_INFO","BROWSER_INFO","OS_VENDOR_INFO","OPERATING_SYSTEM_INFO","DEVICE_TYPE_INFO"}
    # ced_columns_from_db['SMS_CONVERT']={"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","AGGREGATOR_ID","COUNTRY_CODE","MOBILE_CODE","MOBILE_NUMBER","OFFER_NUMBER","ORDER_ID","ORDER_TOTAL","ORDER_QUANTITY","USER_AGENT_STRING","BROWSER_TYPE_INFO","BROWSER_INFO","OS_VENDOR_INFO","OPERATING_SYSTEM_INFO","DEVICE_TYPE_INFO"}
    # ced_columns_from_db['FAIL']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL","EMAIL_ISP","EMAIL_FORMAT","OFFER_SIGNATURE_ID","DYNAMIC_CONTENT_SIGNATURE_ID","MESSAGE_SIZE","SEGMENT_INFO","CONTACT_INFO","REASON","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['LAUNCH_STATE']={"ACCOUNT_ID","LIST_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EXTERNAL_CAMPAIGN_ID","SF_CAMPAIGN_ID","CAMPAIGN_NAME","LAUNCH_NAME","LAUNCH_STATUS","LAUNCH_TYPE","LAUNCH_CHARSET","PURPOSE","SUBJECT","DESCRIPTION","PRODUCT_CATEGORY","PRODUCT_TYPE","MARKETING_STRATEGY","MARKETING_PROGRAM","LAUNCH_ERROR_CODE","LAUNCH_STARTED_DT","LAUNCH_COMPLETED_DT","DISPATCHABLE_TYPE","PROGRAM_ID","BRAND_ID","CREATOR_NAME","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['OPT_OUT']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL_FORMAT","SOURCE","REASON","EMAIL","CONTACT_INFO","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['SKIPPED']={"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","EMAIL","EMAIL_ISP","EMAIL_FORMAT","OFFER_SIGNATURE_ID","DYNAMIC_CONTENT_SIGNATURE_ID","MESSAGE_SIZE","SEGMENT_INFO","CONTACT_INFO","REASON","CAMPAIGN_VERSION_ID","PERSONALIZATION_DT","RECIPIENT_ORG_ID","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['SMS_RECEIVED'] = {"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","AGGREGATOR_ID","COUNTRY_CODE","MOBILE_CODE","MOBILE_NUMBER","MOBILE_KEYWORD","MOBILE_CARRIER","AGGREGATOR_MESSAGE_ID","MSG_BODY"}
    # ced_columns_from_db['PUSH_SENT'] = {"ACCOUNT_ID","EVENT_UUID","EVENT_TYPE_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","PUSH_ID","USER_ID","APP_ID","PLATFORM_TYPE"}
    # ced_columns_from_db['SMS_FAIL'] = {"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","AGGREGATOR_ID","COUNTRY_CODE","MOBILE_CODE","MOBILE_NUMBER","MOBILE_CHANNEL","MOBILE_KEYWORD","OFFER_SIGNATURE_ID","REASON","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['SMS_SKIPPED'] = {"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","AGGREGATOR_ID","COUNTRY_CODE","MOBILE_CODE","MOBILE_NUMBER","MOBILE_CHANNEL","MOBILE_KEYWORD","OFFER_SIGNATURE_ID","REASON","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['PUSH_OPT_IN'] = {"USER_ID","APP_ID","EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","PUSH_ID","PUSHIO_API_KEY","PLATFORM_TYPE"}
    # ced_columns_from_db['SMS_SENT'] = {"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","AGGREGATOR_ID","COUNTRY_CODE","MOBILE_CODE","MOBILE_NUMBER","MOBILE_CHANNEL","MOBILE_KEYWORD","OFFER_SIGNATURE_ID","MSG_SPLIT_COUNT","MESSAGE","CUSTOM_PROPERTIES"}
    # ced_columns_from_db['PUSH_BUTTON_CLICKED'] = {"ACCOUNT_ID","EVENT_TYPE_ID","EVENT_UUID","EVENT_CAPTURED_DT","RIID","CUSTOMER_ID","EVENT_STORED_DT","LIST_ID","LAUNCH_ID","CAMPAIGN_ID","PROGRAM_ID","PUSH_ID","PUSHIO_API_KEY","APP_ID","PLATFORM_TYPE","USER_AGENT_STRING","BUTTON_ID","CATEGORY_ID","BUTTON_NAME","BUTTON_TYPE","ACTION_URL","CATEGORY_NAME","CATEGORY_IDENTIFIER"}
    # ced_columns_from_db['PUSH_CLICKED'] = {"EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID","PROGRAM_ID","PUSH_ID","PUSHIO_API_KEY","APP_ID","PLATFORM_TYPE","OPERATING_SYSTEM","OFFER_NAME","OFFER_NUMBER","OFFER_CATEGORY","OFFER_URL"}
    # ced_columns_from_db['PUSH_OPT_OUT'] = {"USER_ID","EVENT_UUID","EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID","EVENT_CAPTURED_DT","EVENT_STORED_DT","PUSH_ID","PUSHIO_API_KEY","APP_ID","PLATFORM_TYPE"}

    BOUNCE = ["ACCOUNT_ID", "EVENT_TYPE_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID",
              "EMAIL", "EMAIL_FORMAT", "BOUNCE_TYPE", "REASON", "REASON_CODE", "SUBJECT", "CONTACT_INFO", "RECIPIENT_ORG_ID", "CAMPAIGN_VERSION_ID",
              "PERSONALIZATION_DT", "CUSTOM_PROPERTIES"]
    CLICK = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                    "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL_FORMAT", "OFFER_NAME", "OFFER_NUMBER", "OFFER_CATEGORY", "OFFER_URL",
                                    "USER_AGENT_STRING", "CAMPAIGN_VERSION_ID", "PERSONALIZATION_DT", "RECIPIENT_ORG_ID", "BROWSER_TYPE_INFO",
                                    "BROWSER_INFO", "OS_VENDOR_INFO", "OPERATING_SYSTEM_INFO", "DEVICE_TYPE_INFO", "REMOTE_ADDR", "CUSTOM_PROPERTIES"]
    COMPLAINT = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                        "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL_FORMAT", "REASON", "EMAIL", "EMAIL_ISP", "COMPLAINER_EMAIL", "SPAM_TYPE",
                                        "CONTACT_INFO", "COMPLAINT_DT", "RECIPIENT_ORG_ID", "CUSTOM_PROPERTIES"]
    CONVERT = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                      "CAMPAIGN_ID", "LAUNCH_ID", "SOURCE", "EMAIL_FORMAT", "OFFER_NAME", "OFFER_NUMBER", "OFFER_CATEGORY",
                                      "OFFER_URL", "ORDER_ID", "ORDER_TOTAL", "ORDER_QUANTITY", "USER_AGENT_STRING", "CAMPAIGN_VERSION_ID",
                                      "PERSONALIZATION_DT", "RECIPIENT_ORG_ID", "BROWSER_TYPE_INFO", "BROWSER_INFO", "OS_VENDOR_INFO",
                                      "DEVICE_TYPE_INFO", "OPERATING_SYSTEM_INFO", "REMOTE_ADDR", "CUSTOM_PROPERTIES"]
    OPEN = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                   "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL_FORMAT", "USER_AGENT_STRING", "CAMPAIGN_VERSION_ID", "PERSONALIZATION_DT",
                                   "RECIPIENT_ORG_ID", "BROWSER_TYPE_INFO", "BROWSER_INFO", "OS_VENDOR_INFO", "OPERATING_SYSTEM_INFO",
                                   "DEVICE_TYPE_INFO", "REMOTE_ADDR", "CUSTOM_PROPERTIES"]
    SENT = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                   "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL", "EMAIL_ISP", "EMAIL_FORMAT", "OFFER_SIGNATURE_ID",
                                   "DYNAMIC_CONTENT_SIGNATURE_ID", "MESSAGE_SIZE", "SEGMENT_INFO", "CONTACT_INFO", "CAMPAIGN_VERSION_ID",
                                   "PERSONALIZATION_DT", "RECIPIENT_ORG_ID", "CUSTOM_PROPERTIES"]
    SMS_CLICK = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                        "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "AGGREGATOR_ID", "COUNTRY_CODE", "MOBILE_CODE",
                                        "MOBILE_NUMBER", "OFFER_NUMBER", "REMOTE_ADDR", "USER_AGENT_STRING", "BROWSER_TYPE_INFO", "BROWSER_INFO",
                                        "OS_VENDOR_INFO", "OPERATING_SYSTEM_INFO", "DEVICE_TYPE_INFO"]
    SMS_CONVERT = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                          "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "AGGREGATOR_ID", "COUNTRY_CODE", "MOBILE_CODE",
                                          "MOBILE_NUMBER", "OFFER_NUMBER", "ORDER_ID", "ORDER_TOTAL", "ORDER_QUANTITY", "USER_AGENT_STRING",
                                          "BROWSER_TYPE_INFO", "BROWSER_INFO", "OS_VENDOR_INFO", "OPERATING_SYSTEM_INFO", "DEVICE_TYPE_INFO"]
    FAIL = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                   "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL", "EMAIL_ISP", "EMAIL_FORMAT", "OFFER_SIGNATURE_ID",
                                   "DYNAMIC_CONTENT_SIGNATURE_ID", "MESSAGE_SIZE", "SEGMENT_INFO", "CONTACT_INFO", "REASON", "CAMPAIGN_VERSION_ID",
                                   "PERSONALIZATION_DT", "RECIPIENT_ORG_ID", "CUSTOM_PROPERTIES"]
    LAUNCH_STATE = ["ACCOUNT_ID", "LIST_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID",
                                           "EXTERNAL_CAMPAIGN_ID", "SF_CAMPAIGN_ID", "CAMPAIGN_NAME", "LAUNCH_NAME", "LAUNCH_STATUS", "LAUNCH_TYPE",
                                           "LAUNCH_CHARSET", "PURPOSE", "SUBJECT", "DESCRIPTION", "PRODUCT_CATEGORY", "PRODUCT_TYPE",
                                           "MARKETING_STRATEGY", "MARKETING_PROGRAM", "LAUNCH_ERROR_CODE", "LAUNCH_STARTED_DT", "LAUNCH_COMPLETED_DT",
                                           "DISPATCHABLE_TYPE", "PROGRAM_ID", "BRAND_ID", "CREATOR_NAME", "CUSTOM_PROPERTIES"]
    OPT_OUT = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                      "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL_FORMAT", "SOURCE", "REASON", "EMAIL", "CONTACT_INFO", "CAMPAIGN_VERSION_ID",
                                      "PERSONALIZATION_DT", "RECIPIENT_ORG_ID", "CUSTOM_PROPERTIES"]
    SKIPPED = ["EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT", "EVENT_STORED_DT",
                                      "CAMPAIGN_ID", "LAUNCH_ID", "EMAIL", "EMAIL_ISP", "EMAIL_FORMAT", "OFFER_SIGNATURE_ID",
                                      "DYNAMIC_CONTENT_SIGNATURE_ID", "MESSAGE_SIZE", "SEGMENT_INFO", "CONTACT_INFO", "REASON", "CAMPAIGN_VERSION_ID",
                                      "PERSONALIZATION_DT", "RECIPIENT_ORG_ID", "CUSTOM_PROPERTIES"]
    SMS_RECEIVED = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                           "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "AGGREGATOR_ID", "COUNTRY_CODE",
                                           "MOBILE_CODE", "MOBILE_NUMBER", "MOBILE_KEYWORD", "MOBILE_CARRIER", "AGGREGATOR_MESSAGE_ID", "MSG_BODY"]
    PUSH_SENT = ["ACCOUNT_ID", "EVENT_UUID", "EVENT_TYPE_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                        "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "PUSH_ID", "USER_ID", "APP_ID", "PLATFORM_TYPE"]
    SMS_FAIL = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                       "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "AGGREGATOR_ID", "COUNTRY_CODE", "MOBILE_CODE",
                                       "MOBILE_NUMBER", "MOBILE_CHANNEL", "MOBILE_KEYWORD", "OFFER_SIGNATURE_ID", "REASON", "CUSTOM_PROPERTIES"]
    SMS_SKIPPED = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                          "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "AGGREGATOR_ID", "COUNTRY_CODE", "MOBILE_CODE",
                                          "MOBILE_NUMBER", "MOBILE_CHANNEL", "MOBILE_KEYWORD", "OFFER_SIGNATURE_ID", "REASON", "CUSTOM_PROPERTIES"]
    PUSH_OPT_IN = ["USER_ID", "APP_ID", "EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID",
                                          "EVENT_CAPTURED_DT", "EVENT_STORED_DT", "PUSH_ID", "PUSHIO_API_KEY", "PLATFORM_TYPE"]
    SMS_SENT = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                       "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "AGGREGATOR_ID", "COUNTRY_CODE", "MOBILE_CODE",
                                       "MOBILE_NUMBER", "MOBILE_CHANNEL", "MOBILE_KEYWORD", "OFFER_SIGNATURE_ID", "MSG_SPLIT_COUNT", "MESSAGE",
                                       "CUSTOM_PROPERTIES"]
    PUSH_BUTTON_CLICKED = ["ACCOUNT_ID", "EVENT_TYPE_ID", "EVENT_UUID", "EVENT_CAPTURED_DT", "RIID", "CUSTOMER_ID",
                                                  "EVENT_STORED_DT", "LIST_ID", "LAUNCH_ID", "CAMPAIGN_ID", "PROGRAM_ID", "PUSH_ID", "PUSHIO_API_KEY",
                                                  "APP_ID", "PLATFORM_TYPE", "USER_AGENT_STRING", "BUTTON_ID", "CATEGORY_ID", "BUTTON_NAME",
                                                  "BUTTON_TYPE", "ACTION_URL", "CATEGORY_NAME", "CATEGORY_IDENTIFIER"]
    PUSH_CLICKED = ["EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID", "EVENT_CAPTURED_DT",
                                           "EVENT_STORED_DT", "CAMPAIGN_ID", "LAUNCH_ID", "PROGRAM_ID", "PUSH_ID", "PUSHIO_API_KEY", "APP_ID",
                                           "PLATFORM_TYPE", "OPERATING_SYSTEM", "OFFER_NAME", "OFFER_NUMBER", "OFFER_CATEGORY", "OFFER_URL"]
    PUSH_OPT_OUT = ["USER_ID", "EVENT_UUID", "EVENT_TYPE_ID", "ACCOUNT_ID", "LIST_ID", "RIID", "CUSTOMER_ID",
                                           "EVENT_CAPTURED_DT", "EVENT_STORED_DT", "PUSH_ID", "PUSHIO_API_KEY", "APP_ID", "PLATFORM_TYPE"]

    eventNames = ["BOUNCE","CLICK","CONVERT","COMPLAINT","OPEN","SENT","SMS_CLICK","SMS_CONVERT","FAIL","LAUNCH_STATE","OPT_OUT","SKIPPED","SMS_RECEIVED","PUSH_SENT","SMS_FAIL","SMS_SKIPPED","PUSH_OPT_IN","PUSH_OPT_IN","SMS_SENT","PUSH_BUTTON_CLICKED","PUSH_CLICKED","PUSH_OPT_OUT"]
    # for event, jp in [BOUNCE,CLICK,CONVERT,COMPLAINT,OPEN,SENT,SMS_CLICK,SMS_CONVERT,FAIL,LAUNCH_STATE,OPT_OUT,SKIPPED,SMS_RECEIVED,PUSH_SENT,SMS_FAIL,SMS_SKIPPED,PUSH_OPT_IN,PUSH_OPT_IN,SMS_SENT,PUSH_BUTTON_CLICKED,PUSH_CLICKED,PUSH_OPT_OUT]:
    column_names = [BOUNCE,CLICK,CONVERT,COMPLAINT,OPEN,SENT,SMS_CLICK,SMS_CONVERT,FAIL,LAUNCH_STATE,OPT_OUT,SKIPPED,SMS_RECEIVED,PUSH_SENT,SMS_FAIL,SMS_SKIPPED,PUSH_OPT_IN,PUSH_OPT_IN,SMS_SENT,PUSH_BUTTON_CLICKED,PUSH_CLICKED,PUSH_OPT_OUT]

    # for (event, col_list) in zip(eventName, column_names):
    #     for i in range(len(event)):
    #         ced_columns_from_db[eventName][i].extend(col_list)

    for i in range(len(eventNames)):
        # for cols in column_names:
        ced_columns_from_db[eventNames[i]].extend(column_names[i])
    return ced_columns_from_db,built_in_headers_from_db