'''
Created on 20-Oct-2020

@author: rithomas
'''

import subprocess
#import win32com.client as win32
import time
import os
import pygetwindow as gw
import pyautogui
from pynput.mouse import Button, Controller
#shell = win32.Dispatch("WScript.Shell")

mouse = Controller()

def write_symbol(sym):
    print('To type NSE: {}'.format(mouse.position))
    pyautogui.write('NSE', interval=0.10)
    #time.sleep(5)

    
   
def create_strategy(symbol,lot,i):
    symbol_nse = symbol + '21NOV'
    #symbol_nse = symbol + ' '
    ### To copy
    #time.sleep(2)
     
    if i%5 == 0:
        #mouse.scroll(0,5)
        time.sleep(3)
        mouse.position = (92.72381591796875, 66.39004516601562)
        print('refresh position = {}'.format(mouse.position))
        mouse.click(Button.left)
        time.sleep(5)
        mouse.position = (849.9644165039062, 139.5840606689453)
        print('click strategies tab position: {}'.format(mouse.position))
        mouse.click(Button.left)
        time.sleep(5)

        mouse.position = (122.05677795410156, 357.8404541015625)
        print('click My strategies tab position: {}'.format(mouse.position))
        mouse.click(Button.left)
        time.sleep(3)

    mouse.position = (1249.0784912109375, 322.9400634765625)
    print('Copy position = {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)

    #Delete old symbol
    time.sleep(3)
    mouse.scroll(0,5)
    mouse.position = (564.7468872070312, 495.0382080078125)
    print('Delete position = {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(1)

    #Add new symbol
    mouse.position = (474.73748779296875, 448.20166015625)
    print('clicking position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(1)
    #pyautogui.write(symbol_fut, interval=0.10)
    pyautogui.write(symbol_nse, interval=0.10)
    time.sleep(1)
    mouse.position = (468.5357360839844, 493.092529296875)
    print('clicking selection: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(1)

    #Add lot value
    mouse.position = (862.942138671875, 609.4589233398438)
    print('Lot clicking position: {}'.format(mouse.position))
    mouse.click(Button.left)
    pyautogui.press('backspace', 40)
    time.sleep(1)
    pyautogui.write(lot, interval=0.10)



    #Scrolling down
    #time.sleep(3)
    mouse.scroll(0,-15)

    print('Moving to 1st symbol backspace position')
    mouse.position = (539.6891479492188, 637.28955078125)
    #time.sleep(3)
    print('1st symbol backspace position: {}'.format(mouse.position))
    mouse.click(Button.left)
    pyautogui.press('backspace', 40)
    time.sleep(1)

    print('typing 1st symbol: {}'.format(mouse.position))
    pyautogui.write(symbol_nse, interval=0.10)
    time.sleep(1)

    #time.sleep(3)
    mouse.position = (467.2941589355469, 683.9609375)
    print('click position3: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(1)

    #Scrolling down
    mouse.scroll(0,-60)

    print('Going to write Strategy')
    #time.sleep(3)
    mouse.position = (516.357421875, 627.8709106445312)
    #mouse.position = (473.6171875, 671.1348266601562)
    print('writing strategy name: {}'.format(mouse.position))
    mouse.click(Button.left)
    strategy_name = '{i} {s}'.format(i=i,s=symbol)
    pyautogui.write(strategy_name, interval=0.10)
    time.sleep(1)

    print('Going to Save and backtest')
    #time.sleep(3)
    mouse.position = (466.046875, 739.27734375)
    #mouse.position = (444.85693359375, 794.59912109375)
    print('save and backtest position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(5)


    mouse.position = (1361.253662109375, 495.6614074707031)
    print('select script position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(1)


    mouse.position = (1233.2625732421875, 493.2364196777344)
    print('download position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)
    #-----------------
 
#######To live-deploy
    # mouse.move("1819", "461")
    # print('Deploy position: {}'.format(mouse.position))
    # mouse.click(Button.left)
    # time.sleep(1)
    #
    # mouse.move("1037", "389")
    # print('select dropdown: {}'.format(mouse.position))
    # mouse.click(Button.left)
    # time.sleep(1)
    #
    # mouse.move("1019", "576")
    # print('select week: {}'.format(mouse.position))
    # mouse.click(Button.left)
    # time.sleep(1)
    #
    # mouse.move("1246", "739")
    # print('click take live: {}'.format(mouse.position))
    # mouse.click(Button.left)
    # time.sleep(1)
#######################
    time.sleep(3)
    mouse.position = (852.9703979492188, 137.91082763671875)
    print('click strategies tab position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)

    mouse.position = (110.52717590332031, 356.0643005371094)
    print('click My strategies tab position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)
    exit(0)

#############################
#     #mouse.wheel(-3)
#     #time.sleep(3)
#
#
#     #mouse.move("1732", "779")
#     #mouse.move("1741", "746")
#     mouse.move("1746", "775")
#     time.sleep(3)
#     print('edit sample: {}'.format(mouse.position))
#     mouse.click(Button.left)
#     time.sleep(3)
###################################

    mouse.position = (113.07766723632812, 223.4617156982422)
    print('search sample: {}'.format(mouse.position))
    mouse.click(Button.left)
    pyautogui.write('MyStrategy2', interval=0.10)
    time.sleep(5)

    mouse.position = (92.53096008300781, 274.94775390625)
    print('search sample click position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(5)
     
    mouse.position = (1156.4605712890625, 257.1590270996094)
    print('edit sample click position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)

    mouse.scroll(0,-70)
    time.sleep(2)
    
    mouse.position = (418.46466064453125, 752.505126953125)
    print('click re-deploy: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)
    
    mouse.position = (865.9976806640625, 124.64920043945312)
    print('click strategies tab position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(5)

    mouse.position = (137.76771545410156, 356.3389892578125)
    print('click My strategies tab position: {}'.format(mouse.position))
    mouse.click(Button.left)
    time.sleep(3)

def main(pattern):
    #try: 
        #window_foreground(pattern)
        #maximize_window(pattern)
        i=42
        file1 = open('/Users/rthomas/Desktop/Rijin/rijin_git/ST_ANALYZER/streak_analyzer/sym_list.txt')
        Lines = file1.readlines()
        #Lines=['HINDPETRO','ZEEL']
        for sym in Lines:

            print(sym)
            #delete_strategy()
            symbol,lot=sym.split(',')
            create_strategy(symbol,lot,i)
            i += 1
#     except:
#         print("Linking Saving excel failed for {}, retrying".format(pattern))
#         time.sleep(5)
if __name__ == '__main__':
    main('Streak | Create')