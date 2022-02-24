import pandas as df
import csv

file_name = "/Users/rthomas/Downloads/year_appointment_1_year_appointment_arm_1_20211203-095057.csv"

file_df = df.read_csv(file_name)

#print(file_df.columns.values)


#for id in (file_df['1-ORIGINS Pregnancy Number']):
    #print(id)

file = open(file_name)
reader = csv.reader(file)
lines= len(list(reader))
print(lines)