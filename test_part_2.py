'''
This module is to show how unit testing works using the unittest library.
Testing the functions that interact with MySQL would require mocking a database.
'''

import os, sys
import unittest
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
from part_2 import build_report_string

class SomeTests(unittest.TestCase):        
    
    def test_build_report_string(self):
        mock_input = ['US','CA',-10,20]
        self.assertEqual(build_report_string(mock_input),\
                         'The minimum and maximum temperatures in CA between 1990 and 2000 were respectively -10 and 20.')

if __name__ == '__main__':
    unittest.main()

