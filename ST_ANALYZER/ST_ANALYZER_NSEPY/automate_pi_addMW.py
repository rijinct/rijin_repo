'''
Created on 08-Jul-2020

@author: rithomas
Referred 
https://www.thepythoncode.com/article/control-mouse-python

'''
import subprocess
import win32com.client as win32
import time
import mouse
import os
import sys
#sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
import st_common_constants as COMM_CONSTANTS
import pyautogui 

def startPi():
    pi_dir = 'C:\\Zerodha\\Pi'
    os.chdir(pi_dir)
    subprocess.Popen('Pi.exe')
    #p.wait()
    time.sleep(10)
    shell = win32.Dispatch("WScript.Shell")
    time.sleep(2)
    shell.SendKeys("zaq12wsx")
    time.sleep(2)
    shell.SendKeys("{ENTER}")
    time.sleep(2)
    shell.SendKeys("402253")
    shell.SendKeys("{ENTER}")
    time.sleep(10)
    return shell


def set_symbol_mw(sym):
    #time.sleep(2)
    print('To click MyMW: {}'.format(mouse.get_position()))
    mouse.move("129", "89")
    mouse.click('left')
    time.sleep(1)
    print('To select NSE drop: {}'.format(mouse.get_position()))
    mouse.move("20", "109")
    mouse.click('left')
    #time.sleep(1)
    print('To type NSE: {}'.format(mouse.get_position()))
    pyautogui.write('NSE', interval=0.10)
    #time.sleep(5)
    print('To type symbol: {}'.format(mouse.get_position()))
    mouse.move("243", "107")
    mouse.click('left')
    time.sleep(1)
    pyautogui.write(sym, interval=0.10)
    #time.sleep(2)
    print('To Close invalid popup: {}'.format(mouse.get_position()))
    mouse.move("950", "311")
    mouse.click('left')
    #time.sleep(2)
    print('To Close another invalid popup: {}'.format(mouse.get_position()))
    mouse.move("145", "85")
    mouse.click('left')
    #time.sleep(2)
    print('To add symbol: {}'.format(mouse.get_position()))
    mouse.move("740", "112")
    mouse.click('left')
    #Utime.sleep(2)
    print('To ok script already present symbol: {}'.format(mouse.get_position()))
    mouse.move("840", "508")
    mouse.click('left')

def create_new_mw(shell):
    time.sleep(2) 
    print('To go to  File: {}'.format(mouse.get_position()))
    mouse.move("14", "32")
    mouse.click('left')
    time.sleep(1)
    ##To create MW
    print('To create MW: {}'.format(mouse.get_position()))
    mouse.move("52", "56")
    mouse.click('left')
    time.sleep(1)
    shell.SendKeys("MyMW")
    time.sleep(2)
    print('To Final create: {}'.format(mouse.get_position()))
    mouse.move("942", "443")
    mouse.click('left')
    time.sleep(2)
    for sym in COMM_CONSTANTS.st_non_nifty:
        set_symbol_mw(sym)
    #time.sleep(15) 

def main():
    shell = win32.Dispatch("WScript.Shell")
    create_new_mw(shell)

if __name__ == '__main__':
    main()
    #time.sleep(2)
    #set_symbol_mw(shell,'CADILAHC')
    #set_symbol_mw(shell,'CENTURYTEX')
    #set_symbol_mw(shell,'CONCOR')
    #set_symbol_mw(shell,'CUMMINDIND')

