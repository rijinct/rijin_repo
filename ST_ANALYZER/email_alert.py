'''
Created on 26-Apr-2020

@author: rithomas
'''

import smtplib, ssl
#import win32com.client as win32

# def email_alert_sender_win32(msg):
#     outlook = win32.Dispatch('outlook.application')
#     mail = outlook.CreateItem(0)
#     mail.To = 'rijinct@gmail.com'
#     mail.Subject = msg
#     mail.Body = msg
#     mail.Send()

def email_alert_sender(message):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "thomasct2253@gmail.com"  # Enter your address
    receiver_email = "rijinct@gmail.com"  # Enter receiver address
    #password = input("Type your password and press enter: ")
    password = 'aeazjueefupjarav'
    print(port, smtp_server, sender_email, receiver_email, password, message)
    message = message.replace(':','-')
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)

def main():
    message = """Test Email"""
    email_alert_sender(message)
    #email_alert_sender_win32(message)
if __name__ == '__main__':
    main()    