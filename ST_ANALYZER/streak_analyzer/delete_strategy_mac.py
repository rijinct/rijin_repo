'''
Created on 20-Oct-2020

@author: rithomas
'''
import time
import pygetwindow as gw
import sys

from pynput.mouse import Button, Controller

mouse = Controller()

def delete_strategy():
    ### To execute scan now
    time.sleep(1)
    mouse.scroll(0,5)
    mouse.position = (1364.631103515625, 485.48028564453125)
    print('mouse.position = {}'.format(mouse.position))
    time.sleep(1)
    mouse.click(Button.left)
    time.sleep(1)
    mouse.position = (965.660400390625, 378.60101318359375)
    time.sleep(1)
    mouse.click(Button.left)
    print('mouse.position = {}'.format(mouse.position))


def main():
    i = 1
    while (i < 97):
        delete_strategy()
        i += 1

if __name__ == '__main__':
    main()
