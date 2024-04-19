import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from sqlalchemy import create_engine


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
    df = pd.DataFrame(res)
    return df

def insert_data():
    import psycopg2
    import time

    res = stream_data()
    # # Example: 'postgresql://username:password@localhost:5432/your_database'
    # code from https://medium.com/@askintamanli/fastest-methods-to-bulk-insert-a-pandas-dataframe-into-postgresql-2aa2ab6d2b24
    engine = create_engine(
        "postgresql://airflow:airflow@host.docker.internal/animejikan"
    )

    start_time = time.time()  # get start time before insert
    # Create a connection to your PostgreSQL database
    conn = psycopg2.connect(
        database="animedb",
        user="airflow",
        password="airflow",
        host="host.docker.internal",
        port="5432",
    )
    try:
        res.to_sql(name="jikan_anime", con=engine, if_exists="replace")
        conn.commit()
        end_time = time.time()  # get end time after insert
        total_time = end_time - start_time  # calculate the time
        logging.info(f"Insert time: {total_time} seconds")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")


with DAG(
    default_args=default_args,
    dag_id="dag_insert_anime_to_table_v10",
    start_date=datetime(2024, 4, 10),
    schedule_interval="@once",
) as dag:
    task1 = PythonOperator(task_id="get_data", python_callable=stream_data)
    task2 = PostgresOperator(
        task_id="create_postgres_anime_table",
        postgres_conn_id="anime_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS jikan_anime (
            anime_id SERIAL PRIMARY KEY,
            title VARCHAR NOT NULL,
            score int NOT NULL,
            popularity int NOT NULL,
            ranking int NOT NULL,
            ratings VARCHAR NOT NULL,
            favorites int NOT NULL);
        """,
    )

    task3 = PythonOperator(task_id="insert_Data", python_callable=insert_data)
    task1 >> task2 >> task3
