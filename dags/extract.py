from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from googleapiclient.discovery import build
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import os
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from io import StringIO
import pytz
import json
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from snowflake.connector.pandas_tools import write_pandas
import snowflake
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

#from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator


# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/






    # Your code goes here.

#Defining the credentials and apis which are needed to extract
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

youtubers_list=["UC-lHJZR3Gqxm24_Vd_AJ5Yw","UC1gSyUP5QOZBebhlCObZ-0A",
                "UCq-Fj5jknLsUf-MWSy4_brA","UCJcCB-QYPIBcbKcBQOTwhiA",
                "UCbCmjCuTUZos6Inko4u57UQ","UCX6OQ3DkcsbYNE6H8uQQuVA","UCY6KjrDBN_tIRFT_QNqQbRQ"]


#using aws base hook and to access the credentials defined in the airflow UI  
aws_credentials = BaseHook.get_connection('aws_yt')
session = boto3.Session(
aws_access_key_id=aws_credentials.login,
aws_secret_access_key=aws_credentials.password)

#Aws Secret manager to store the snowflake creds and Google API key
secret_name = "snowflake_data"
region_name = "us-east-2"
client = session.client(service_name='secretsmanager',region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
secret_ = get_secret_value_response['SecretString']
secret=json.loads(secret_)

ctx= snowflake.connector.connect(
    user=secret["user"],
    password=secret["password"],
    account=secret["account"],
    warehouse=secret["warehouse"],
    database=secret["database"],
    schema=secret["schema"]
)

cs=ctx.cursor()

"""The main function accepts two arguments one is the channel ID and the other is the API key
and it returns the json response for the respective youtube channel
"""
def main(chnls,secret):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    apikey=secret
    api_service_name = "youtube"
    api_version = "v3"

    # Get credentials and create a
    youtube = build(
        api_service_name, api_version, developerKey=apikey,cache_discovery=False)

    request = youtube.channels().list(
        part="id,localizations,snippet,statistics,status,topicDetails",
        id=chnls
    )
    response = request.execute()

    return response


"""
This function, named loopdict, takes a dictionary (val) along with two lists (lst_vals2 and lst_keys2) as input parameters. 
Its purpose is to recursively iterate through the keys and values of the dictionary 
and extract the keys and corresponding values into the provided lists.
"""

def loopdict(val:dict,lst_vals2,lst_keys2):
    #print(val)
    if type(val)==dict:
        for key,value in val.items():            
            if type(value)==dict:
                loopdict(value,lst_vals2,lst_keys2)
            elif type(value)==list:
                for i in value:
                    lst_vals2.append(i)
                for i in range(len(value)):
                    lst_keys2.append("{}_{}".format(key,i))
            else:
                lst_keys2.append(key)
                lst_vals2.append(value)
    #print(lst_keys2,lst_vals2)
    return lst_keys2,lst_vals2

"""
This function, named extract, processes a response (resp) from an API, extracting relevant data into lists and a DataFrame. 
It iterates through the response, identifying specific keys and values, and utilizes another function (loopdict) to handle nested dictionary structures. 
The extracted data is then organized into a DataFrame, combined with existing data from an S3 bucket if available, and stored back into the bucket after processing. 
Additionally, it appends a timestamp to the DataFrame to track when the extraction occurred.
"""
def extract(bucket_name:str,resp,lst_values,lst_vals2,lst_keys2,session):
    for key,value in resp.items():
        if key=="items":
            for values in value:
                for key1,value1 in values.items():
                    if type(value1) == str:
                        lst_values.append(value1)
                    else:
                        col,val=loopdict(value1,lst_vals2,lst_keys2)
    new_df= pd.DataFrame([val],columns=col)
    ottawa_time = datetime.now(ottawa_timezone) 
    new_df['timestamp']=ottawa_time
    s3 = session.client('s3')
    new_df = new_df.loc[:, ~new_df.columns.duplicated()]
    fl_name='_'.join(((((new_df["title"][0]).replace('-',' ')).split('/'))[0].split(' ')))
    key_=fl_name+"/{}_data.csv".format(fl_name)

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key_)
        existing_data = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
        append_df=pd.concat([new_df,existing_data],ignore_index=True)
        append_df=append_df.drop('description', axis=1)
        csv_buffer = StringIO()
        append_df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name,Key=key_,Body=csv_buffer.getvalue())

    except:
        new_df = new_df.loc[:, ~new_df.columns.duplicated()]
        csv_buffer = StringIO()
        new_df=new_df.drop('description', axis=1)
        new_df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name,Key=key_,Body=csv_buffer.getvalue())

    return None


"""
The extract_data_google function fetches data from Google APIs for a list of YouTube channels, 
processes the response, and stores the extracted data in an AWS S3 bucket named 'ytanalytics'.

"""
def extract_data_google(session):
    secret_name = "googleAPI"
    region_name = "us-east-2"
    client = session.client(service_name='secretsmanager',region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret_ = get_secret_value_response['SecretString']
    secret=json.loads(secret_)["api_key"]
    for chnls in youtubers_list:
        lst_vals2=[]
        lst_keys2=[]
        lst_values=[]
        resp=main(chnls,secret)
        val=extract(bucket_name= 'ytanalytics',resp=resp,lst_values=lst_values,lst_vals2=lst_vals2,lst_keys2=lst_keys2,session=session)

"""
The load_ function fetches CSV data stored in an S3 bucket named 'ytanalytics', 
loads it into a Snowflake database table, dynamically creating or truncating the table based on the CSV file's contents.

"""

def load_(session):
    s3 = session.client('s3')
    for key in s3.list_objects(Bucket='ytanalytics')["Contents"]:
        object_key=key['Key']
        csv_obj = s3.get_object(Bucket="ytanalytics", Key=object_key)
        body = csv_obj['Body']
        tbl= '_'.join(((object_key.split('/'))[0].split(' ')))
        tabl_name_spl="\""+tbl+"_RAW"+"\""
        tabl_name=tbl+"_RAW"
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        #print(df_["title"])
        #print(df)
        #print(df.columns)
        #write_pandas(ctx,df,tabl_name,auto_create_table=True)
        #print(tabl_name)    
        try:
            cs.execute("truncate table ""{}""".format(tabl_name_spl))
            write_pandas(ctx,df,tabl_name)
        except:
            write_pandas(ctx,df,tabl_name,auto_create_table=True)


    



default_args = {
    "owner": "admin",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com"
}

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "airbnb", "schema": "ytanalytics"},
    )
)



with DAG("extract_cloud",start_date=datetime(2023,12,26),
         schedule_interval=timedelta(hours=1),default_args=default_args,catchup=False) as dag:

    downloading_rates = PythonOperator(
            task_id="downloading_rates",
            python_callable=extract_data_google,
            op_kwargs={'session':session},
    )

    rates_to_raw = PythonOperator(
            task_id="rates_to_raw",
            python_callable=load_,
            op_kwargs={'session':session},
    )

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/dbt_ytanalytics"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",))

    downloading_rates >> rates_to_raw >> transform_data



"""
trigger_job_run1 = DbtCloudRunJobOperator(
    task_id="trigger_job_run1",
    job_id=70403103916703,
    check_interval=10,
    \\timeout=300,
    )
"""