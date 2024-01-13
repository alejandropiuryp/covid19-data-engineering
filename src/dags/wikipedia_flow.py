from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator
)
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.wikipedia_webscraping import extract_wikipedia_data
from scripts.wikipedia_transform import transform_data
from scripts.wikipedia_load_s3 import load_data


with DAG(
    dag_id='wikipedia_workflow',
    default_args={
        "owner": "Alejandro Piury",
    },
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    # Extraction
    extract_data_from_wikipedia = PythonOperator(
        task_id="extract_data",
        python_callable=extract_wikipedia_data,
        op_kwargs={"url":"https://en.wikipedia.org/wiki/Ranked_lists_of_Spanish_municipalities"},
        provide_context=True
    )
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )
    load_data_into_s3 = PythonOperator(
        task_id="load_data_into_S3",
        python_callable=load_data,
        provide_context=True
    )

    extract_data_from_wikipedia >> transform_data >> load_data_into_s3
# Preproccesiing

# Write

