import requests
import json
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {"owner": "mad", "retry": 5, "retry_delay": timedelta(minutes=5)}


def getData():
    response_API = requests.get("https://api.jikan.moe/v4/anime")
    print(response_API.status_code)
    data = response_API.text
    data_load_json = json.loads(data)
    return data_load_json


def formattingData(res):
    titles = []
    scores = []
    popularitys = []
    ranks = []
    ratings = []
    favorites_list = []
    for values in res["data"]:
        titles.append(values["title"])
        scores.append(values["score"])
        popularitys.append(values["popularity"])
        ranks.append(values["rank"])
        ratings.append(values["rating"])
        favorites_list.append(values["favorites"])
    df = {
        "title": titles,
        "score": scores,
        "popularity": popularitys,
        "ranking": ranks,
        "rating": ratings,
        "favorites": favorites_list,
    }
    return df


def stream_data():

    res = getData()
    res = formattingData(res)
    return json.dumps(res, indent=3)

def to_pandas():
    res = getData()
    res = formattingData(res)
    df = pd.DataFrame(res)
    return df
    

def most_anime_favorite():
    data=to_pandas()
    most_fav=data[["title","favorites"]].sort_values(by='favorites',ascending=False)
    return most_fav.to_dict()


with DAG(
    default_args=default_args,
    dag_id="dag_with_api_data_anime_V07",
    start_date=datetime(2024, 4, 10),
    schedule_interval="@once",
) as dag:
    task1 = PythonOperator(task_id="get_data", python_callable=stream_data)
    task2 = PythonOperator(task_id='data_df', python_callable=to_pandas) 
    task3 = PythonOperator(task_id='anime_fav', python_callable=most_anime_favorite) 

    task1 >> task2 >> task3
