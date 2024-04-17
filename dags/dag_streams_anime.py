import requests
import json


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


with DAG(
    default_args=default_args,
    dag_id="dag_with_api_data_anime_V01",
    start_date=datetime(2024, 4, 10),
    schedule_interval="@once",
) as dag:
    task1 = PythonOperator(task_id="get_data", python_callable=stream_data)

    task1
