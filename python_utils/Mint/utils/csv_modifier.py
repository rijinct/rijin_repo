
# import required modules
import pandas as pd
import csv
  
# assign header columns
file=r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\test\temp.csv'

headerList = ['col1', 'col2', 'col3', 'col4', 'col5','col6']
  
# open CSV file and assign header
with open(file, newline='\n') as f:
    r = csv.reader(f)
    data = [line for line in r]
with open(file, 'w', newline='\n') as f:
    w = csv.writer(f)
    w.writerow(headerList)
    w.writerow(data)
