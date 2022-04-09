from cherrypicker import CherryPicker
import json
import pandas as pd

with open('response.json') as file:
    data = json.load(file)

picker = CherryPicker(data)
flat = picker['questionnaires'].flatten().get()
df = pd.DataFrame(flat)
df.to_csv('response.csv')