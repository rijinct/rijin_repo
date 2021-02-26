'''
Created on 02-Aug-2020

@author: rithomas
'''
import win32com.client as win32
outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.To = 'rijinct@gmail.com'
mail.Subject = 'Test'
mail.Body = 'Test'
mail.Send()