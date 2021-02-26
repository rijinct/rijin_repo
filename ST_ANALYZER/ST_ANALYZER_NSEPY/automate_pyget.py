'''
Created on 20-Aug-2020

@author: rithomas
'''

import pygetwindow as gw


def maximize_window(pattern):
    default_window = gw.getWindowsWithTitle(pattern)[0]
    default_window.maximize()
    print(default_window.isMaximized)
    
def window_foreground(pattern):
    default_window = gw.getWindowsWithTitle(pattern)[0]
    default_window.maximize()
    default_window.activate()
    
#window_foreground('Pi [Version : 1.0.0.0] ZY0655, 13906, ZERODHA -All')

print(gw.getAllTitles())