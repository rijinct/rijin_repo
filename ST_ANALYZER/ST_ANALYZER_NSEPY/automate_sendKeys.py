'''
Created on 22-Aug-2020

@author: rithomas
'''

import keyboard
import win32com.client as win32
import pyautogui 

shell = win32.Dispatch("WScript.Shell")


shell.SendKeys("The quick brown fox jumps over the lazy dog.")
keyboard.write('The quick brown fox jumps over the lazy dog.')
pyautogui.write('The quick brown fox jumps over the lazy dog', interval=0.10)
