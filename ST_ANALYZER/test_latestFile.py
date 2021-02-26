'''
Created on 27-Apr-2020

@author: rithomas
'''
import glob
import os

file = r'C:\Users\rithomas\AppData\Roaming\Microsoft\Excel'
for name in glob.glob(file):
    print (name)
    
    
def get_latest_file_or_folder(zerodha_file_path, pattern):
    #files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    files = glob.glob(zerodha_file_path + '\\' + pattern +'*')
    latest_file =  max(files, key=os.path.getctime)
    return latest_file

print(get_latest_file_or_folder(file,'Def'))