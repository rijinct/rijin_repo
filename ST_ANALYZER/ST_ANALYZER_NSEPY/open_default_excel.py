'''
Created on 20-Oct-2020

@author: rithomas
'''
import subprocess
import win32com.client as win32
import time


def open_excelDesktop(file):
    excel = win32.Dispatch("Excel.Application")  
    file = r'C:\\Users\\rithomas\\Desktop\\{}'.format(file)
    wb = excel.Workbooks.Open(file)
    excel.Visible = True
    #excel.WindowState = win32.constants.xlMaximized 
    time.sleep(5)
    
def main():
    try: 
        open_excelDesktop('Default MW.xlsx')
        open_excelDesktop('MyMW MW.xlsx')
    except:
        print("Opening the default final excels failed, retrying")
        time.sleep(5)
        open_excelDesktop('Default MW.xlsx')
        open_excelDesktop('MyMW MW.xlsx')
        
if __name__ == '__main__':
    main()
