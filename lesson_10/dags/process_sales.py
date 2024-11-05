import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime

from settings.constants import BUCKET, GCP_CONN_ID, LOCAL_ROOT_DIRECTORY
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

with DAG(
        "upload_files_to_gcs",
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(1),
) as dag:
    # Task to upload files
    for file_path in files_to_upload:
        gcs_destination = f"src1/sales/v1/{formatted_date}/"
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f"upload_{os.path.basename(file_path)}",
            src=file_path,
            dst=gcs_destination,
            bucket=BUCKET,
            gcp_conn_id=GCP_CONN_ID,
        )
