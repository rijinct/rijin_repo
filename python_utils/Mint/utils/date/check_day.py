from datetime import datetime

print(datetime.today().weekday())
if (datetime.today().weekday() != 4)|(datetime.today().hour < 17):
     print("Today is not Friday too")