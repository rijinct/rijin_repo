'''
Created on 20-Oct-2020

@author: rithomas
'''

# import win32com.client as win32
import time
import mouse
import pygetwindow as gw
# import win32com.client as win32
import time

import mouse
import pygetwindow as gw


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


def delete_strategy():
    ### To execute scan now
    time.sleep(1)
    mouse.move("1276", "452")
    print('sheet file position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)
    mouse.move("896", "326")
    print('sheet file position: {}'.format(mouse.get_position()))
    mouse.click('left')
    # time.sleep(3)


def main(pattern):
    # try:
    # window_foreground(pattern)
    # maximize_window(pattern)
    i = 1
    while (i < 95):
        # print('hi')
        delete_strategy()
        i += 1


#     except:
#         print("Linking Saving excel failed for {}, retrying".format(pattern))
#         time.sleep(5)
if __name__ == '__main__':
    main('Streak | Create')
