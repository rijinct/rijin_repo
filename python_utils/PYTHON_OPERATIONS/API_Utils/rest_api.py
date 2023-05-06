import requests
from requests.structures import CaseInsensitiveDict
from cherrypicker import CherryPicker
import pandas as pd


def initialize():
    global headers, quest_rep_url, quest_url
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers[
        "Authorization"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyVXVpZCI6IjJlYzRkZDgzLWNmYTktNGI0Ny1iZWJmLTUyYWRiYTk5ZDlhZiIsImV4cCI6MTY1MDM0OTM4OSwidmVyc2lvbiI6IjIifQ.9ZIKdjTCGvVdkXljLhh_7yl5KaTLE3-kMxLJs7UOds4"
    quest_rep_url = 'http://datastewardship.telethonkids.org.au:3000/questionnaires/51128efa-66d4-4bb0-a814-579b5741fbde/report'
    quest_url = 'http://datastewardship.telethonkids.org.au:3000/questionnaires'


def get_data(url):
    global j
    r = requests.get(url, headers=headers)
    # df = pd.read_csv(StringIO(r.text))
    return r.json()


def flatten_json(df, rep):
    if rep == 'quest':
        df_rep = df['_embedded']['questionnaires']
    else:
        df_rep = df['chapterReports']
    print(df_rep)
    picker = CherryPicker(df_rep)
    flat = picker.flatten().get()
    df_json = pd.DataFrame(flat)
    return df_json


if __name__ == "__main__":
    initialize()
    df_questionnaire_rep = flatten_json(get_data(quest_rep_url),'quest_rep')
    df_quest = flatten_json(get_data(quest_url),'quest')
    print(df_quest)
