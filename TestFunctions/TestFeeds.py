from BaseFunctions.setup import Setup
from BaseFunctions.ced_functions import CEDFunctions
from BaseFunctions.db_fuctions import DBFunctions
from TestFunctions.validate_columns import ValidateColumns
from TestFunctions.validate_count import ValidateCount
from TestFunctions.validate_data import ValidateData

import unittest

# class TestFeeds(unittest.testcase):
#     ValidateColumns()
#     ValidateCount()
#     ValidateData()

def suite():
    suite = unittest.TestSuite()
    suite.addTest(ValidateColumns)
    suite.addTest(ValidateCount)
    suite.addTest(ValidateData)
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())

