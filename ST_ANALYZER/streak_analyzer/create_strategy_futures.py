'''
Created on 20-Oct-2020

@author: rithomas
'''

import subprocess
#import win32com.client as win32
import time
import mouse
import os
import pygetwindow as gw
import pyautogui 

#shell = win32.Dispatch("WScript.Shell")

def write_symbol(sym):
    print('To type NSE: {}'.format(mouse.get_position()))
    pyautogui.write('NSE', interval=0.10)
    #time.sleep(5)

    
   
def create_strategy(symbol,lot,i):
    symbol_fut = symbol + '21JAN'
    symbol_nse = symbol + ' '
    ### To copy
    time.sleep(2)
     
    if i%5 == 5:
        mouse.move("88", "53")
        print('refresh position: {}'.format(mouse.get_position()))
        mouse.click('left')
        time.sleep(3)
            
        mouse.move("739", "84")
        print('click strategies tab position: {}'.format(mouse.get_position()))
        mouse.click('left')
        time.sleep(3)
      
        mouse.move("161", "309")
        print('click My strategies tab position: {}'.format(mouse.get_position()))
        mouse.click('left')
        time.sleep(3)
        exit(0)
        
    mouse.move("1143", "287")
    print('COPY position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(2)
    #Delete old symbol
    mouse.move("521", "458")
    print('Delete position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)
    #Add new symbol
    mouse.move("467", "407")
    print('clicking position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)
    pyautogui.write(symbol_fut, interval=0.10)
    #pyautogui.write(symbol_nse, interval=0.10)
    time.sleep(1)
    mouse.move("471", "451")
    print('clicking position2: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)
         
#     mouse.move("1257", "756")
#     print('lot back position2: {}'.format(mouse.get_position()))
#     mouse.click('left')
#     pyautogui.press('backspace', 40)
#     pyautogui.write(lot, interval=0.10)
#     time.sleep(3)
     
    #Scrolling down
    mouse.wheel(-4)
    time.sleep(1)
            
    mouse.move("534", "343")
    print('1st symbol backspace position: {}'.format(mouse.get_position()))
    mouse.click('left')
    pyautogui.press('backspace', 40)
    time.sleep(1)
            
    print('typing 1st symbol: {}'.format(mouse.get_position()))
    pyautogui.write(symbol_nse, interval=0.10)
    time.sleep(1)
           
    mouse.move("544", "385")
    print('click position3: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)
  
    mouse.wheel(-4)
    time.sleep(1)
           
#     mouse.move("734", "607")
#     print('backspace position3: {}'.format(mouse.get_position()))
#     mouse.click('left')
#     pyautogui.press('backspace', 40)
#     time.sleep(1)
#       
#           
#     print('typing position3: {}'.format(mouse.get_position()))
#     pyautogui.write(symbol_nse, interval=0.10)
#     time.sleep(1)
#           
#     mouse.move("706", "664")
#     print('click position4: {}'.format(mouse.get_position()))
#     mouse.click('left')
#     time.sleep(1)
#       
#     mouse.wheel(-3)
#     time.sleep(2)
           
    mouse.move("524", "452")
    print('writing strategy name: {}'.format(mouse.get_position()))
    mouse.click('left')
    strategy_name = '{i} {s}'.format(i=i,s=symbol)
    pyautogui.write(strategy_name, interval=0.10)
    time.sleep(1)
     
    mouse.move("452", "567")
    print('save and backtest position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(3)

    mouse.move("1290", "450")
    print('select script position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)


    mouse.move("1224", "454")
    print('download position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)

#######To live-deploy
    # mouse.move("1819", "461")
    # print('Deploy position: {}'.format(mouse.get_position()))
    # mouse.click('left')
    # time.sleep(1)
    #
    # mouse.move("1037", "389")
    # print('select dropdown: {}'.format(mouse.get_position()))
    # mouse.click('left')
    # time.sleep(1)
    #
    # mouse.move("1019", "576")
    # print('select week: {}'.format(mouse.get_position()))
    # mouse.click('left')
    # time.sleep(1)
    #
    # mouse.move("1246", "739")
    # print('click take live: {}'.format(mouse.get_position()))
    # mouse.click('left')
    # time.sleep(1)
#######################
    mouse.move("760", "99")
    print('click strategies tab position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(2)
    
    mouse.move("122", "314")
    print('click My strategies tab position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(2)
 
#############################
#     #mouse.wheel(-3)
#     #time.sleep(3)
#
#
#     #mouse.move("1732", "779")
#     #mouse.move("1741", "746")
#     mouse.move("1746", "775")
#     time.sleep(3)
#     print('edit sample: {}'.format(mouse.get_position()))
#     mouse.click('left')
#     time.sleep(3)
###################################
 
    mouse.move("121", "206")
    print('search sample: {}'.format(mouse.get_position()))
    mouse.click('left')
    pyautogui.write('MyStrategy2', interval=0.10)
    time.sleep(2)

    mouse.move("107", "232")
    print('search sample click position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(2)
     
    mouse.move("1106", "196")
    print('edit sample click position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(3)
    
    mouse.wheel(-10)
    time.sleep(1)
    
    mouse.move("410", "564")
    print('click re-deploy: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(1)
    
    mouse.move("781", "98")
    print('click strategies tab position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(3)
  
    mouse.move("172", "309")
    print('click My strategies tab position: {}'.format(mouse.get_position()))
    mouse.click('left')
    time.sleep(3)


def main(pattern):
    #try: 
        #window_foreground(pattern)
        #maximize_window(pattern)
        i=1
        file1 = open(r'C:\Users\admin\Desktop\Rijin\streak_automate\symbol_list.txt', 'r')
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