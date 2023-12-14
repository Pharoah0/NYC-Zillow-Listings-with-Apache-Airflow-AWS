from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Connects Airflow to S3.
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Allows transfer from S3 to Redshift via Airflow.
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import timedelta, datetime
import json
import requests


# Load our JSON config file with API details.
with open("/home/ubuntu/airflow/config_api.json", "r") as config_file:
    api_host_key = json.load(config_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "email": ["myemail@domain.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

# String to capture time & date of when a file was created.
now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

# Defining the S3 bucket for transformed data.
s3_bucket = "zillow-data-project-transformed"


def extract_zillow_data(**kwargs):
    url = kwargs["url"]
    headers = kwargs["headers"]
    querystring = kwargs["querystring"]
    dt_string = kwargs["date_string"]
    # return headers
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    # Specify the output file path within the Airflow server.
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"  # Applies the date & time to the file name, within a filepath.
    file_str = f"response_data_{dt_string}.parquet"  # Used for Lambda function.

    # Write the JSON response to a file
    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)  # Indent for pretty formatting.
    output_list = [
        output_file_path,
        file_str,
    ]  # Outout list [0] = JSON data, [1] = Parquet data.
    return output_list


with DAG(
    "zillow_analytics_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    extract_zillow_data_var = PythonOperator(
        task_id="task_extracting_zillow_data",
        python_callable=extract_zillow_data,
        op_kwargs={
            "url": "https://zillow56.p.rapidapi.com/search",
            "querystring": {"location": "New York, NY"},
            "headers": api_host_key,  # Our JSON file with API credentials.
            "date_string": dt_now_string,  # Captures time & date of when file was created.
        },
    )

    load_to_s3 = BashOperator(
        task_id="task_load_to_s3",
        bash_command='aws s3 mv {{ ti.xcom_pull("task_extracting_zillow_data")[0]}} s3://zillow-data-project-raw',  # Looks for the JSON data in the output_list variable.
    )

    # Checks transformed bucket if a new/unique file is there before moving it to Redshift.
    is_file_in_s3_available = S3KeySensor(
        task_id="task_is_file_in_s3_available",
        bucket_key='{{ti.xcom_pull("task_extracting_zillow_data")[1]}}',  # Looks for the Parquet data in the output_list variable.
        bucket_name=s3_bucket,
        aws_conn_id="aws_s3_conn",
        wildcard_match=False,  # Set this to True if you want to use wildcards in the prefix.
        timeout=60,  # Optional: Timeout for the sensor (in seconds).
        poke_interval=5,  # Optional: Time interval between S3 checks (in seconds).
    )

    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="tsk_transfer_s3_to_redshift",
        aws_conn_id="aws_s3_conn",
        redshift_conn_id="conn_id_redshift",
        s3_bucket=s3_bucket,
        s3_key='{{ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',
        schema="PUBLIC",
        table="zillowdata",
        copy_options=["csv IGNOREHEADER 1"],
    )

    (
        extract_zillow_data_var
        >> load_to_s3
        >> is_file_in_s3_available
        >> transfer_s3_to_redshift
    )
