'''
Created on 20-Oct-2020

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

shell = win32.Dispatch("WScript.Shell")

def maximize_window(pattern):
    default_window = gw.getWindowsWithTitle(pattern)[0]
    default_window.maximize()
    print(default_window.isMaximized)
    
def window_foreground(pattern):
    default_window = gw.getWindowsWithTitle(pattern)[0]
    default_window.maximize()
    default_window.activate()

def minimize_window(pattern):
    default_window = gw.getWindowsWithTitle(pattern)[0]
    default_window.minimize()

def link_to_excel():
    mouse.move("1200", "500")
    time.sleep(2)
    mouse.right_click()
    time.sleep(5)
    mouse.move("1247", "800")
    mouse.click('left')
    time.sleep(2)
    shell.SendKeys("{ENTER}")
    time.sleep(10)

def link_and_save_excelDesktop(pattern):
    maximize_window('Pi [Version : 1.0.0.0] ZY0655, 13906, ZERODHA -All')
    #To link to excel
    time.sleep(5)
    link_to_excel() 
    #To maximize pi
    maximize_window('Pi [Version : 1.0.0.0] ZY0655, 13906, ZERODHA -All')
    time.sleep(30)
    print('hi')
    ##To minimize pi
    minimize_window('Pi [Version : 1.0.0.0] ZY0655, 13906, ZERODHA -All')
    ##To maximize DefaultSheet
    maximize_window(pattern)
    time.sleep(15) 
    ### To click on file
    mouse.move("21", "48")
    print('sheet file position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(5)
### To click saveAs
    mouse.move("84", "312")
    print('sheet saveAs position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(5) ###To click browse
    mouse.move("254", "461")
    print('click browse in desktop position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(5) ###To click save in desktop
    mouse.move("84", "151")
    print('sheet save in desktop position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(5)
###To click save
    mouse.move("1446", "826")
    print('sheet save in desktop position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(5) ###To click confirm- save
    mouse.move("827", "438")
    print('sheet confirm-save in desktop position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(5) ###To close
    mouse.move("1575", "4")
    print('sheet close position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(2) ###To confirm-close
    mouse.move("800", "459")
    print('sheet confirm-close position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(2)
    
def main(pattern):
    try: 
        link_and_save_excelDesktop(pattern)
    except:
        print("Linking Saving excel failed for {}, retrying".format(pattern))
        time.sleep(5)
        link_and_save_excelDesktop(pattern)
if __name__ == '__main__':
    main('Default')
    main('MyMW')