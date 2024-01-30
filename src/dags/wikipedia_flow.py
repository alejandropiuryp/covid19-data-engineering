from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos import DbtDag
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.wikipedia_webscraping import extract_wikipedia_data
from scripts.wikipedia_transform import transform_data
from scripts.wikipedia_load_s3 import load_data
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup, DbtTestLocalOperator
from cosmos.operators import DbtDocsS3Operator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(profile_name="default",
                               target_name="dev",
                               profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="conn_id_snowflake",
                                                                                   profile_args={
                                                                                       "database": "ANALYTICS",
                                                                                       "schema": "MARTS"
                                                                                   }))

with DAG(
    dag_id='wikipedia_workflow',
    default_args={
        "owner": "Alejandro Piury",
    },
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval=None,
    catchup=False,
    template_searchpath="/opt/airflow/dags/include/sql"
) as dag:
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
    copy_csv_into_snowflake_table = SnowflakeOperator(
        task_id="copy_csv_into_snowflake_table",
        snowflake_conn_id="conn_id_snowflake",
        sql="insert_data.sql",
        params={
            "destination": 'RAW.WIKIPEDIA_DATA.SPANISH_MUNICIPALITY',
            "origin": '@RAW.WIKIPEDIA_DATA.SPANISH_MUNICIPALITY_STAGE',
            "format": 'CSV_FORMAT'
        }
    )
    dbt_dag = DbtTaskGroup(
        group_id="dbt_transform_data",
        project_config=ProjectConfig("/opt/airflow/dags/dbt/dbt_wikipedia"),
        profile_config=profile_config,
        operator_args={
            "install_deps": False,
        }
    )

    begin = EmptyOperator(task_id="Begin")
    end = EmptyOperator(task_id="End")

    begin >> extract_data_from_wikipedia >> transform_data >> load_data_into_s3 >> copy_csv_into_snowflake_table >> dbt_dag >> end

