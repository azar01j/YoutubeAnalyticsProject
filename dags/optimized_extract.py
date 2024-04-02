import os
import json
from datetime import datetime
from io import StringIO
import pytz
import pandas as pd
import boto3
from googleapiclient.discovery import build
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import snowflake.connector.pandas_tools as sf

# Function to retrieve YouTube channel data using the YouTube API
def get_youtube_channel_data(channel_id, api_key):
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    request = youtube.channels().list(
        part="id,localizations,snippet,statistics,status,topicDetails",
        id=channel_id
    )
    response = request.execute()
    return response

# Function to flatten nested dictionaries into lists of keys and values
def flatten_dict(d):
    keys, values = [], []
    for key, value in d.items():
        if isinstance(value, dict):
            sub_keys, sub_values = flatten_dict(value)
            keys.extend(sub_keys)
            values.extend(sub_values)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                keys.append(f"{key}_{i}")
                values.append(v)
        else:
            keys.append(key)
            values.append(value)
    return keys, values

# Function to extract data from the YouTube API response
def extract_data(resp):
    values = []
    keys = []
    for item in resp.get("items", []):
        item_keys, item_values = flatten_dict(item)
        keys.extend(item_keys)
        values.append(item_values)
    return keys, values

# Function to upload data to S3
def upload_to_s3(bucket_name, key, data):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    s3 = session.client("s3")
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=data)
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Function to extract YouTube data and upload to S3
def extract_and_upload(bucket_name, channel_ids, api_key):
    for channel_id in channel_ids:
        resp = get_youtube_channel_data(channel_id, api_key)
        keys, values = extract_data(resp)
        df = pd.DataFrame([values], columns=keys)
        ottawa_timezone = pytz.timezone('America/Toronto')
        ottawa_time = datetime.now(ottawa_timezone)
        df['timestamp'] = ottawa_time
        key = f"{df['title'][0]}/{df['title'][0]}_data.csv"
        try:
            existing_data = pd.read_csv(f"s3://{bucket_name}/{key}")
            df = pd.concat([df, existing_data], ignore_index=True)
            df = df.drop('description', axis=1)
        except Exception:
            pass
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        upload_to_s3(bucket_name, key, csv_buffer.getvalue())

# Function to load data from S3 into Snowflake
def load_into_snowflake(bucket_name):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    s3 = session.client('s3')
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    for key in s3.list_objects(Bucket=bucket_name)["Contents"]:
        object_key = key['Key']
        tbl = '_'.join(((object_key.split('/'))[0].split(' ')))
        table_name = tbl + "_RAW"
        csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        df_ = df.head()
        sf.write_pandas(ctx, df_, table_name, auto_create_table=True)

# DAG definition
default_args = {
    "owner": "admin",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com"
}

with DAG("optimized_extract", start_date=datetime(2023, 12, 26),
         schedule_interval="*/5 * * * *", default_args=default_args, catchup=False) as dag:
    
    def extract():
        api_credentials = BaseHook.get_connection('youtube_api')
        aws_credentials = BaseHook.get_connection('aws_credentials')
        bucket_name = 'ytanalytics'
        channel_ids = ["UCX6OQ3DkcsbYNE6H8uQQuVA", "UC-lHJZR3Gqxm24_Vd_AJ5Yw", "UC1gSyUP5QOZBebhlCObZ-0A", "UCq-Fj5jknLsUf-MWSy4_brA"]
        extract_and_upload(bucket_name, channel_ids, api_credentials.password)

    def load():
        aws_credentials = BaseHook.get_connection('aws_credentials')
        load_into_snowflake("ytanalytics")

    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=extract
    )

    rates_to_raw = PythonOperator(
        task_id="rates_to_raw",
        python_callable=load
    )

    downloading_rates >> rates_to_raw
