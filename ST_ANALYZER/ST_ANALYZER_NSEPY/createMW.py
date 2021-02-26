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
import link_and_save_excelDesktop

def window_foreground(pattern):
    default_window = gw.getWindowsWithTitle(pattern)[0]
    default_window.maximize()
    default_window.activate()

def createMW():
    window_foreground('Pi [Version : 1.0.0.0] ZY0655, 13906, ZERODHA -All')
    time.sleep(5)
    ##To close orderBook
    print('OrderBook position: {}'.format(mouse.get_position()))
    mouse.move("170", "90")
    mouse.click('left')
    time.sleep(2)
    automate_pi_addMW.main()
    time.sleep(30)
    ##To click on MW 
    print('MV position: {}'.format(mouse.get_position()))
    mouse.move("132", "82")
    mouse.click('left')
    time.sleep(10)
    #To link to excel
    link_and_save_excelDesktop('MyMW')
    #To maximize pi
    time.sleep(2)
    
def main():
    try: 
        createMW()
    except:
        print("creating MW failed, trying again")
        time.sleep(5)
        createMW()
if __name__ == '__main__':
    main()