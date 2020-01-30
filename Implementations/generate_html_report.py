import re
from collections import defaultdict
# from html_report_code import HTML
import HTML
#
# def print_header():
#     HTMLFILE = 'CountValidationReport.html'
#     f = open(HTMLFILE, 'w')
#     t = HTML.Table(header_row=['File Name', 'Total Unique IDs', 'Passed', 'Failed', 'Skipped'])
#     htmlcode = str(t)
#     # print(htmlcode)
#     f.write(htmlcode)
#     f.write('<p>')

def generate_report(status_report, total_time, ced_files, empty_files, test_name):
    line_no = 0
    false_reason = {}
    header_2 = None
    f,t1 = None,None

    if test_name.lower() == "validate_count":
        HTMLFILE = '../HTML_Reports/CountValidationReport.html'
        with open(HTMLFILE, 'w') as f:

            header_cols = ['Sl.No', 'File Name', 'Total Unique IDs', 'Passed', 'Failed', 'Skipped']
            t1 = HTML.Table(header_row=HTML.TableRow(header_cols, attribs={'align': 'center', 'bgcolor': '#33cccc'}, header=True),
                            attribs={'align': 'center'}, style='border-style: solid',
                            col_align=['center', 'left', 'center', 'center', 'center', 'center'])
            if header_2:
                t1.rows.append(header_2)

            for file in status_report:
                true_count, false_count, skip_count, total_ids = 0, 0, 0, 0
                line_no += 1
                for id in status_report[file]:
                    true_count += status_report[file][id].count(True)
                    false_count += status_report[file][id].count(False)
                    if 'does not match' in str(status_report[file][id]):
                        false_reason[file] = (status_report[file][id][1:])
                    if 'skip' in str(status_report[file][id]):
                        try:
                            skip_count += int(str(status_report[file][id][1]).split('_')[1])
                        except Exception as e:
                            skip_count += int(str(status_report[file][id][0]).split('_')[1])
                    total_ids += len(status_report[file][id])
                    if total_ids != (true_count + false_count + skip_count):
                        total_ids = true_count + false_count + skip_count
                t1.rows.append([line_no, file, total_ids, true_count, false_count, skip_count])

            htmlcode = str(t1)
            f.write(htmlcode)
            f.write('<p>')


    elif test_name == "validate_columns":
        HTMLFILE = '../HTML_Reports/ColumnValidationReport.html'
        with open(HTMLFILE, 'w') as f:

            # header_1 = ["Sl.No", "File Name", "Total Unique IDs", "Column Name Validation", "Column Order Validation"]
            header_2 = ['','', '', 'Passed', 'Failed', 'Passed', 'Failed']

            t1 = HTML.Table(header_row=HTML.TableRow((
                ["Sl.No","File Name", "Total Unique IDs", HTML.TableCell("Column Name Validation", style='font-weight : bold', attribs={'colspan': 2}),
                 HTML.TableCell("Column Order Validation", style='font-weight : bold', attribs={'colspan': 2})]), header=True,
                attribs={'bgcolor': '#33cccc'}), attribs={'align': 'center'}, style='border-style: solid',
                col_align=['center', 'left', 'center', 'center', 'center', 'center', 'center'])

            t1.rows.append(HTML.TableRow(header_2, attribs={'align': 'center', 'bgcolor': '#d6f5f5'}, header=True))
            for file in status_report:
                total_cols, col_name_passed, col_name_failed, col_order_passed, col_order_failed = 0, 0, 0, 0, 0
                line_no += 1
                for col_status in status_report[file]:
                    total_cols = len(status_report[file][col_status])
                    if col_status.lower() == "column_name":
                        col_name_passed = status_report[file][col_status].count(True)
                        col_name_failed = status_report[file][col_status].count(False)
                    else:
                        col_order_passed = status_report[file][col_status].count(True)
                        col_order_failed = status_report[file][col_status].count(False)

                    if total_cols != (col_name_passed + col_name_failed):
                        total_cols = col_name_passed + col_name_failed

                t1.rows.append([line_no, file, total_cols, col_name_passed, col_name_failed, col_order_passed, col_order_failed])

            htmlcode = str(t1)
            f.write(htmlcode)
            f.write('<p>')

    elif test_name.lower() == "validate_data":
        HTMLFILE = '../HTML_Reports/DataValidationReport.html'
        # with open(HTMLFILE, 'w') as f:
        with open(HTMLFILE, 'w') as f:

            header_cols = ["Sl.No", "File Name ", "Total Rows ", "Passed Rows", "Failed Rows ", "Skipped Rows "]
            t1 = HTML.Table(header_row=HTML.TableRow(header_cols, header=True, attribs={'bgcolor': '#33cccc'}), attribs={'align': 'center'},
                            style='border-style: solid', col_align=['center', 'left', 'center', 'center', 'center', 'center'])
            skip_reasons = defaultdict(list)
            line_no = 0
            for file in status_report:
                true_count, false_count, skip_count, total_rows = 0, 0, 0, 0
                line_no += 1
                for id in status_report[file]:
                    true_count += status_report[file][id].count(True)
                    false_count += status_report[file][id].count(False)
                    if 'skip' in str(status_report[file][id]).lower():
                        try:
                            split_message = str(status_report[file][id][0]).rsplit("_", 1)
                            skip_count += int(split_message[1])
                            skip_reasons[file].append(split_message[0])
                        except Exception as e:
                            split_message = str(status_report[file][id][1]).rsplit("_", 1)
                            skip_count += int(split_message[1])
                            skip_reasons[file].append(split_message[0])
                    total_rows += len(status_report[file][id])
                    if total_rows != (true_count + false_count + skip_count):
                        total_rows = true_count + false_count + skip_count

                t1.rows.append([line_no, file, total_rows, true_count, false_count, skip_count])

            htmlcode = str(t1)
            f.write(htmlcode)
            f.write('<p>')
