import boto3
import os
import json
from datetime import datetime
import pandas as pd

def load_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids='transform_data')


    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    filename= f"wikipedia/{datetime.now().strftime('%Y-%m-%d')}/wikipedia_data.csv"

    s3_client.put_object(Bucket="rawlayer", Key=filename, Body=data)
    return "ok"