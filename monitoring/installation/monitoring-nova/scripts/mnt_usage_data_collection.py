#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 1
# Purpose: This script send usage details of MNT volume to mariadb.
#
# Date:  03-09-2020
#############################################################################
#############################################################################
from datetime import datetime
import json
import shutil
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from dbUtils import DBUtil
from enum_util import AlarmKeys, TypeOfUtils
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils, MonitoringUtils
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm


create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

def main():
    _execute()

def _execute():
    raw_data = Collector().collect()
    processed_data = Processor(raw_data).process()
    Presenter(processed_data).present()


class Collector():
    def collect(self):
        logger.info('Getting MNT Volume Usage')
        mnt_usage = shutil.disk_usage('/mnt')
        return mnt_usage

class Processor:
    def __init__(self, raw_data):
        self._data = raw_data
    
    def process(self):
        total, used, free = self._data
        total_tb = MonitoringUtils.convert_to_tb(total)
        used_tb = MonitoringUtils.convert_to_tb(used)
        free_tb = MonitoringUtils.convert_to_tb(free)
        used_perc = (used_tb/total_tb)*100  
        mnt_stats_json_list = [{"Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Total Capacity(Tb)": str(total_tb), "Used Capacity(Tb)": str(used_tb), "Available Capacity(Tb)": str(free_tb), "MNT Used(%)": str(used_perc)}]
        return mnt_stats_json_list
    
    
class Presenter:
    
    ALARM_KEY = AlarmKeys.APPLICATION_NODE_MNT_USAGE.value
    TYPE_OF_UTIL = TypeOfUtils.MNT_STATS_UTIL.value

    def __init__(self, data):
        self._data = data
    
    def present(self):
        self.__send_alert(self._data)
        file_name = '{0}_{1}.csv'.format(Presenter.TYPE_OF_UTIL, datetime.now().strftime("%Y-%m-%d_%H_%M"))
        CsvUtil().writeDictToCsvDictWriter(Presenter.TYPE_OF_UTIL,self._data[0], file_name)
        DBUtil().jsonPushToMariaDB(json.dumps(self._data),Presenter.TYPE_OF_UTIL)
    
    def __send_alert(self, mnt_stats_json_list):
        json_list_for_alert = []
        mnt_used_per = float(mnt_stats_json_list[0]['MNT Used(%)'])
        thresholdData = self.__configured_value()
        major_threshold, critical_threshold = float(thresholdData['MajorThreshold']),float(thresholdData['CriticalThreshold'])
        
        if mnt_used_per > major_threshold:
            json_list_for_alert.append({ key:mnt_stats_json_list[0][key] for key in mnt_stats_json_list[0].keys() })
            severity = "MAJOR"
            threshold = major_threshold
            
            if mnt_used_per > critical_threshold:
                severity = "CRITICAL"
                threshold = critical_threshold
            
        if len(json_list_for_alert) > 0:
            self.__frame_html_and_send(json_list_for_alert, severity, mnt_used_per, threshold)   
        
    def __configured_value(self):
        propertyTag=XmlParserUtils.extract_child_tags('MntVolume', 'totalUsedPerc')
        return {'MajorThreshold': propertyTag[0].attributes['MajorThreshold'].value,'CriticalThreshold': propertyTag[0].attributes['CriticalThreshold'].value}
    
    def __frame_html_and_send(self, json_list_for_alert, severity, mnt_perc_occupied, threshold_value):
        html = HtmlUtil().generateHtmlFromDictList("Total MNT Volume Usage", json_list_for_alert)
        EmailUtils().frameEmailAndSend("[{0} ALERT]: MNT Volume Usage is {1}% which is greater than the threshold {2}%".format(severity, mnt_perc_occupied, threshold_value), html, Presenter.ALARM_KEY)
        SnmpAlarm.send_alarm(Presenter.ALARM_KEY, "MNT Used(%): "+str(mnt_perc_occupied), severity)

        
if __name__ == "__main__":
    main()
