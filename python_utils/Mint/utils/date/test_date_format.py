import pandas as pd

# Define string
date = '04/03/2021 11:23'

# Convert string to datetime format
date1 = pd.to_datetime(date)

# print to_datetime output
print(date1)

# print day, month and year separately from the to_datetime output
print("Day: ", date1.day)
print("Month", date1.month)
print("Year", date1.year)