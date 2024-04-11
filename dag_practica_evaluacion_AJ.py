import pendulum
from airflow.decorators import task
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.decorators import task_group

'''
from airflow.models.connection import Connection
conn = Connection('postgres')
print(conn.host)
print(conn.extra_dejson)

from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('user_api')
print(conn.host)
print(conn.extra_dejson)

'''

with DAG('dag_practica_evaluacion', schedule='@daily', start_date=pendulum.datetime(2024, 4, 10), catchup=False) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='practica_evaluacion',
        sql='''
        CREATE TABLE IF NOT EXISTS table_tweets(
            tweet_id TEXT NOT NULL PRIMARY KEY,
            creation_date TEXT NOT NULL,
            retweet_num TEXT NOT NULL,
            reply_num TEXT NOT NULL,
            views_num TEXT NOT NULL

        );
        '''
    )
    
    @task_group()
    def group_api():
        @task
        def check_web():
            import requests
            web_page = "https://rapidapi.com/omarmhaimdat/api/twitter154/"
            response = requests.get(web_page)
            print('--------------------------')
            print(response)
            print('--------------------------')
            if response.status_code == 200:
                data = response.text
                print(f"INFO: La web esta disponible.")
            else:
                print(f"ERROR: La web parece caida.")

        @task
        def extract_tweets():
            import requests
            url = "https://twitter154.p.rapidapi.com/user/tweets"

            querystring = {"username":"omarmhaimdat",
                        "limit":"40",
                        "user_id":"96479162",
                        "include_replies":"false",
                        "include_pinned":"false"}

            headers = {
                "X-RapidAPI-Key": "d48253b2d3msh5c9c0fcd7d41e25p1d34dejsn24cbeb81a2c2",
                "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
            }

            response = requests.get(url, headers=headers, params=querystring)

            return response.json()['results']
    
        check_web()
        api_results = extract_tweets()

        return api_results
    

    @task
    def process_tweets(api_results, trigger_rule="all_success"):
        data_list_dict = []
        for idr, r in enumerate(api_results):
            temp_dict = {'tweet_id': r['tweet_id'],
                        'creation_date': r['creation_date'],
                        'retweet_count': r['retweet_count'],
                        'reply_count': r['reply_count'],
                        'views': r['views']
                        }
            data_list_dict.append(temp_dict)

        df_data = pd.DataFrame.from_records(data_list_dict)
        df_data.to_csv('/tmp/received_info.csv', index=None, header=False)

    @task
    def store_info():
        hook = PostgresHook(postgres_conn_id='practica_evaluacion')
        hook.copy_expert(
        sql="COPY table_tweets FROM stdin WITH DELIMITER as ','",
        filename='/tmp/received_info.csv'
        )

    @task
    def print_info():
        with open("/tmp/received_info.csv", "r") as f:
            print(f.read())

    api_results = group_api()

    create_table >> process_tweets(api_results) >> store_info() >> print_info()
