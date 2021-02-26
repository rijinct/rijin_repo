'''
Created on 07-Dec-2020

@author: rithomas

'''
import os, time, sys
today_date = time.time()
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi import constants

def delete_files(path, ret_days):
    for file in os.listdir(path):
        file = os.path.join(path, file)
        if os.stat(file).st_mtime < today_date - ret_days * 86400:
            os.remove(os.path.join(path, file))

def execute():
    delete_files(constants.DQHI_LOG_PATH_NOVA, 3)
    delete_files(constants.DQHI_OUTPUT, 14)

if __name__ == "__main__":
    execute()
