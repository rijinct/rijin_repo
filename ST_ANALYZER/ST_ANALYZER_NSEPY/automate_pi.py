'''
Created on 16-Aug-2020

@author: rithomas


'''
import subprocess
import win32com.client as win32
import time
import mouse
import os
import pygetwindow as gw
import automate_pi_addMW
import open_default_excel
import createMW
import link_and_save_excelDesktop

shell = win32.Dispatch("WScript.Shell")

def startPi():
    pi_dir = 'C:\\Zerodha\\Pi'
    os.chdir(pi_dir)
    subprocess.Popen('Pi.exe')
    #p.wait()
    time.sleep(30)
    shell.SendKeys("zaq12wsx")
    time.sleep(2)
    shell.SendKeys("{ENTER}")
    time.sleep(5)
    shell.SendKeys("402253")
    shell.SendKeys("{ENTER}")
    time.sleep(10)

if __name__ == '__main__':
    startPi()
    link_and_save_excelDesktop.main('Default')
    time.sleep(5)
    createMW.main()
    link_and_save_excelDesktop.main('MyMW')
    #open_excelDesktop('Default MW.xlsx')
    #open_excelDesktop('MyMW MW.xlsx')
    open_default_excel.main()



