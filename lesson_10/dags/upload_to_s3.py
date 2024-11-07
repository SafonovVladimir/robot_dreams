import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from notifications import send_success_notification, send_no_files_notification, send_failure_alert
from settings import default_args, files_to_upload, formatted_date
from constants import BUCKET_NAME, AWS_S3_CONN_ID

# Define the DAG S3
with DAG(
        'upload_files_to_s3',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        catchup=False,
        on_success_callback=None,
        on_failure_callback=send_failure_alert,
) as dag:
    if files_to_upload:
        with TaskGroup("upload_files_group") as upload_group:
            for file in files_to_upload:
                s3_uri = f"s3://{BUCKET_NAME}/src1/sales/v1/{formatted_date}/{file.split('/')[-1]}"
                upload_file_to_s3 = LocalFilesystemToS3Operator(
                    task_id=f"upload_{os.path.basename(file)}",
                    filename=file,
                    dest_key=s3_uri,
                    aws_conn_id=AWS_S3_CONN_ID,
                    replace=True,  # Replace existing file
                )

        send_no_files_notification = PythonOperator(
            task_id="send_telegram_notification",
            provide_context=True,
            python_callable=send_success_notification,
        )

    else:
        no_files_task = EmptyOperator(
            task_id="no_files_found"
        )

        send_no_files_notification = PythonOperator(
            task_id="send_telegram_no_files_notification",
            provide_context=True,
            python_callable=send_no_files_notification,
        )
