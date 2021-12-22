import datetime as dt
from datetime import timedelta
import datetime as dt
from os import path
import pandas as pd
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = '/<YOUR PATH >/geo_data.csv'


def csvToJson():
    df=pd.read_csv(DATA_PATH)
    for i,rw in df.iterrows():
        print({'first_name':rw['firstName'],'last_name': rw['lastName'], 'job': rw['Job']})
    df.to_json('/<YOUR PATH >/fromAirflow.json',orient='records')




default_args = {
    'owner': 'fermat01',
    'start_date': dt.datetime(2021, 12, 22),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('csvToJsonDag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      # '0 * * * *',
         ) as dag:

    print_starting = BashOperator(task_id='starting',
                               bash_command='echo "I am reading the CSV now....."')

    csvJson = PythonOperator(task_id='convertCSVtoJson',
                                 python_callable=csvToJson)


print_starting >> csvJson
