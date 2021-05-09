import unittest
import usage_table_count
import dbConnectionManager
from defusedxml import minidom


class TestUsageTableCount(unittest.TestCase):
    def execute_sql(self, hiveSql):
        if 'us_gncp_1' in hiveSql:
            return "2,1345\n14,55"
        elif 'us_bcsi_1' in hiveSql:
            return "0,63\n15,0\n23,452"
        else:
            return ""

    def get_xml(self):
        return minidom.parseString('''<?xml version="1.0" encoding="UTF-8"?>
<config>
    <USAGETABLE>
        <property Name="us_gncp_1" value="yes" type="fixed_line" />
        <property Name="us_bcsi_1" value="yes" type="mobile" />
        <property Name="us_broadband_1" value="no"/>
    </USAGETABLE>
</config> ''')

    def set_up_dependencies(self):
        usage_table_count.Date = ''
        usage_table_count.tz = ''
        usage_table_count.HOURS_PER_DAY = 24
        usage_table_count.min_dt, usage_table_count.max_dt = '', ''
        usage_table_count.get_required_xml = self.get_xml
        usage_table_count.MIN_EPOCH, usage_table_count.MAX_EPOCH\
            = usage_table_count.get_epoch_thresholds()
        dbConnectionManager.DbConnection.getHiveConnectionAndExecute = \
            self.execute_sql

    def test_get_usage_tables(self):
        self.set_up_dependencies()
        usage_tables = usage_table_count.get_usage_tables()
        self.assertEqual(usage_tables, {
            'us_gncp_1': 'fixed_line',
            'us_bcsi_1': 'mobile'
        })

    def test_get_usage_table_counts(self):
        self.set_up_dependencies()
        usage_tables = usage_table_count.get_usage_tables()
        table_counts = usage_table_count.get_usage_table_counts(usage_tables)
        self.assertEqual(table_counts, [('us_gncp_1', {
            0: 0,
            1: 0,
            2: 1345,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
            7: 0,
            8: 0,
            9: 0,
            10: 0,
            11: 0,
            12: 0,
            13: 0,
            14: 55,
            15: 0,
            16: 0,
            17: 0,
            18: 0,
            19: 0,
            20: 0,
            21: 0,
            22: 0,
            23: 0,
            24: 1400
        }),
                                        ('us_bcsi_1', {
                                            0: 63,
                                            1: 0,
                                            2: 0,
                                            3: 0,
                                            4: 0,
                                            5: 0,
                                            6: 0,
                                            7: 0,
                                            8: 0,
                                            9: 0,
                                            10: 0,
                                            11: 0,
                                            12: 0,
                                            13: 0,
                                            14: 0,
                                            15: 0,
                                            16: 0,
                                            17: 0,
                                            18: 0,
                                            19: 0,
                                            20: 0,
                                            21: 0,
                                            22: 0,
                                            23: 452,
                                            24: 515
                                        })])

    def test_get_content_for_graph(self):
        self.set_up_dependencies()
        usage = 'us_bcsi_1'
        counts = {
            0: 63,
            1: 988,
            2: 144,
            3: 56,
            4: 69,
            5: 0,
            6: 0,
            7: 0,
            8: 0,
            9: 0,
            10: 0,
            11: 0,
            12: 0,
            13: 0,
            14: 0,
            15: 0,
            16: 0,
            17: 0,
            18: 0,
            19: 0,
            20: 0,
            21: 0,
            22: 0,
            23: 452,
            24: 515
        }
        content = usage_table_count.get_content_for_graph(
            usage, counts, 'mobile')
        org_content = [{
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "0",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "63"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "1",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "988"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "2",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "144"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "3",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "56"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "4",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "69"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "5",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "6",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "7",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "8",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "9",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "10",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "11",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "12",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "13",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "14",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "15",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "16",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "17",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "18",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "19",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "20",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "21",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "22",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "0"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "23",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "452"
        }, {
            "Date": "",
            "DateInLocalTZ": "",
            "Hour": "24",
            "TableName": "us_bcsi_1",
            "Type": "mobile",
            "Count": "515"
        }]
        for usage in org_content:
            usage["DateInLocalTZ"] = usage_table_count.get_yesterday_in_epoch(
                int(usage["Hour"]))
        self.assertEqual(content, '%s' % str(org_content).replace("'", '"'))

    def test_get_agg_usages_per_hour(self):
        self.set_up_dependencies()
        usages = usage_table_count.get_usage_tables()
        hour = 0
        table_counts = usage_table_count.get_usage_table_counts(usages)
        counts = usage_table_count.get_agg_usages_per_hour(
            table_counts, hour, usages)
        self.assertEqual(counts, [63, 0])
