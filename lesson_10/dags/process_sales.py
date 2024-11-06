import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime

from settings.constants import GCP_BUCKET, GCP_CONN_ID, LOCAL_ROOT_DIRECTORY, S3_BUCKET, AWS_S3_CONN_ID
from functions.job_functions import get_files_with_relative_paths

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Variable name should be "upload_date"
date_str = Variable.get("upload_date")
date = datetime.strptime(date_str, "%Y-%m-%d")
formatted_date = date.strftime("%Y/%m/%d")

files_to_upload = get_files_with_relative_paths(LOCAL_ROOT_DIRECTORY)

# Define the DAG GCP
with DAG(
        "upload_files_to_gcs",
        default_args=default_args,
        schedule_interval='@once',
        start_date=days_ago(1),
        catchup=False,
) as dag:
    # Task to upload files
    for file in files_to_upload:
        gcs_destination = f"src1/sales/v1/{formatted_date}/"
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f"upload_{os.path.basename(file)}",
            src=file,
            dst=gcs_destination,
            bucket=GCP_BUCKET,
            gcp_conn_id=GCP_CONN_ID,
        )

# Define the DAG S3
with DAG(
        'upload_files_to_s3',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        catchup=False,
) as dag:
    for file in files_to_upload:
        s3_uri = f"s3://{S3_BUCKET}/src1/sales/v1/{formatted_date}/{file.split('/')[-1]}"
        upload_file_to_s3 = LocalFilesystemToS3Operator(
            task_id=f"upload_{os.path.basename(file)}",
            filename=file,
            dest_key=s3_uri,
            aws_conn_id=AWS_S3_CONN_ID,
            replace=True,  # Replace existing file
        )
