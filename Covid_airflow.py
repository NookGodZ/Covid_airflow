from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.dates import days_ago

import requests
import json
import pandas as pd

def Covid_API_week():
    url = "https://covid19.ddc.moph.go.th/api/Cases/today-cases-all"
    respone = requests.get(url)
    data = respone.json()
    df = pd.DataFrame(data)
    file = pd.read_excel('C:\\Users\\panta\\เดสก์ท็อป\\AAAAAAAAAAAAAAAAAAAAI\\output1.xlsx')
    file = pd.concat([file, df])
    file.to_excel('C:\\Users\\panta\\เดสก์ท็อป\\AAAAAAAAAAAAAAAAAAAAI\\output1.xlsx', engine='xlsxwriter')  

with DAG(
    "Covid19",
    start_date=days_ago(1),
    schedule_interval="None",
    tags=["workshop"]
) as dag:
    
    t1 = PythonOperator(
        task_id="Covid_API_week",
        python_callable=Covid_API_week,
        dag=dag
    )
    t1
