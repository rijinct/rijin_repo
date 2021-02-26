'''
Created on 26-Feb-2020

@author: npn
'''
import os
import filecmp
import unittest
import sys
sys.path.insert(0,'com/rijin/dqhi')
sys.path.insert(1,'../com/rijin/dqhi')
from test import db_export_test_util
from com.rijin.dqhi import dqhi_score_generator_client
from com.rijin.dqhi import kpi_cde_score_generator


class Test(unittest.TestCase):

    def test_data_quality_statistics_xml(self):
        dqhi_score_generator_client.main(["dqhi_score_generator_client.py", "2017-02-05"])
        kpi_cde_score_generator.main(["kpi_cde_score_generator.py"])
        db_export_test_util.genereate_data_quality_statistics()
        actual_xml = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/actual/dataQualityStatistics.xml"))
        expected_xml = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/expected/dataQualityStatistics.xml"))
        print("test_data_quality_statistics_xml")
        print("test_data_quality_statistics_xml-actual")
        with open(actual_xml) as reader:
            line = reader.readline()
            while line != '':
                print(line, end='')
                line = reader.readline()
        print("test_data_quality_statistics_xml-expected")
        with open(expected_xml) as reader:
            line = reader.readline()
            while line != '':
                print(line, end='')
                line = reader.readline()
        self.assertTrue(filecmp.cmp(actual_xml, expected_xml))

    def test_data_quality_result_xml(self):
        dqhi_score_generator_client.main(["dqhi_score_generator_client.py", "2017-02-05"])
        db_export_test_util.generated_data_quality_result()
        actual_xml = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/actual/dataQualityResult.xml"))
        expected_xml = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/expected/dataQualityResult.xml"))
        print("test_data_quality_result_xml")
        print("test_data_quality_result_xml-actual")        
        with open(actual_xml) as reader:
            line = reader.readline()
            while line != '':
                print(line, end='')
                line = reader.readline()
        print("test_data_quality_result_xml-expected")
        with open(expected_xml) as reader:
            line = reader.readline()
            while line != '':
                print(line, end='')
                line = reader.readline()        
        self.assertTrue(filecmp.cmp(actual_xml, expected_xml))
         
if __name__ == "__main__":
    unittest.main()
