'''
Created on 04-May-2020

@author: rithomas
'''
import glob
import os

def get_latest_file_or_folder_based_pattern(zerodha_file_path, pattern):
    files = glob.glob(zerodha_file_path + '\\' + pattern +'*')
    latest_file =  max(files, key=os.path.getctime)
    return latest_file
