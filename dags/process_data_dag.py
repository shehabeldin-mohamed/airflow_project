import os
import pandas as pd
from datetime import datetime
from airflow import DAG, Dataset
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

INPUT_FILE = '/opt/airflow/data/tiktok_google_play_reviews.csv'
PROCESSED_FILE = '/opt/airflow/data/processed.csv'
processed_dataset = Dataset("file://" + PROCESSED_FILE)

def check_file_empty():
    if os.path.getsize(INPUT_FILE) == 0:
        return 'log_empty_file'
    return 'processing_tasks.replace_nulls'


def replace_nulls():
    df = pd.read_csv(INPUT_FILE)
    df.fillna('-', inplace=True)
    df.replace('null', '-', inplace=True)
    df.to_csv(PROCESSED_FILE, index=False)


def sort_data():
    df = pd.read_csv(PROCESSED_FILE)
    df['at'] = pd.to_datetime(df['at'])
    df.sort_values(by='at', inplace=True)
    df.to_csv(PROCESSED_FILE, index=False)


def clean_content():
    df = pd.read_csv(PROCESSED_FILE)
    df['content'] = df['content'].astype(str).str.replace(r'[^\w\s.,!?"\'-]', '', regex=True)
    df['content'] = df['content'].str.strip()
    df.to_csv(PROCESSED_FILE, index=False)


# DAG
with DAG(
        dag_id='1_process_data_dag',
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=['assignment']
) as dag:
    # 1.SENSOR
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=600
    )

    # 2.BRANCHING
    branch_task = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=check_file_empty
    )

    # 3.1 BASH SCRIPT: If file is empty
    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "The input file is empty. Nothing to process!"'
    )

    # 3.2 TASKGROUP: If file has data, run the Pandas cleaning steps
    with TaskGroup("processing_tasks") as processing_tasks:
        replace_nulls = PythonOperator(
            task_id='replace_nulls',
            python_callable=replace_nulls
        )

        sort_data = PythonOperator(
            task_id='sort_data',
            python_callable=sort_data
        )

        clean_content = PythonOperator(
            task_id='clean_content',
            python_callable=clean_content,
            outlets=[processed_dataset]
        )

        # Order of operations inside the TaskGroup
        replace_nulls >> sort_data >> clean_content

    # Sensor -> Branch -> [Bash (if empty), TaskGroup (if not empty)]
    wait_for_file >> branch_task >> [log_empty_file, processing_tasks]