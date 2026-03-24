import pandas as pd
from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


PROCESSED_FILE = '/opt/airflow/data/processed.csv'
processed_dataset = Dataset("file://" + PROCESSED_FILE)


def load_to_mongo():
    df = pd.read_csv(PROCESSED_FILE)
    records = df.to_dict(orient='records')
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.airflow_db
    collection = db.processed_data
    collection.delete_many({})
    collection.insert_many(records)
    print(f"Successfully inserted {len(records)} records into MongoDB!")


# DAG
with DAG(
        dag_id='2_load_to_mongo_dag',
        start_date=datetime(2026, 1, 1),
        schedule=[processed_dataset],  #Triggered by the Dataset
        catchup=False,
        tags=['assignment']
) as dag:
    load_data_task = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo
    )