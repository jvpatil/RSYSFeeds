from Implementations.setup import Setup
from Implementations.ced_functions import CEDFunctions
from Implementations.db_fuctions import DBFunctions
from TestFunctions.validate_columns import ValidateColumns
from TestFunctions.validate_count import ValidateCount
from TestFunctions.validate_data import ValidateData

import unittest

# class RsysFeeds(unittest.testcase):
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
#included test should not have 'if __name__== '--main__'. instead should have "ClassName().testMethodName()"
