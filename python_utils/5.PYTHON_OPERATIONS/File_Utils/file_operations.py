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


def get_files_based_pattern(path, pattern):
    files = glob.glob(path  + '/' + pattern +'*')
    return files

def delete_files_pattern(path, pattern):
    files = glob.glob(pattern)
    for file in files:
        os.remove(file)



if __name__ == "__main__":
    file = get_files_based_pattern('.','rt') 

    if not file: 
        print('Day file empty, running for today')