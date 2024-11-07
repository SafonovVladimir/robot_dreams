import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from notifications import send_success_notification, send_no_files_notification, send_failure_alert
from settings import default_args, files_to_upload, formatted_date
from constants import BUCKET_NAME, GCP_CONN_ID

# Define the DAG GCP
with DAG(
        "upload_files_to_gcs",
        default_args=default_args,
        schedule_interval='@once',
        start_date=days_ago(1),
        catchup=False,
        on_success_callback=None,
        on_failure_callback=send_failure_alert,
) as dag:
    # Check files to upload and create task
    if files_to_upload:
        with TaskGroup("upload_files_group") as upload_group:
            for file in files_to_upload:
                gcs_destination = f"src1/sales/v1/{formatted_date}/"
                upload_task = LocalFilesystemToGCSOperator(
                    task_id=f"upload_{os.path.basename(file)}",
                    src=file,
                    dst=gcs_destination,
                    bucket=BUCKET_NAME,
                    gcp_conn_id=GCP_CONN_ID,
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

        no_files_task >> send_no_files_notification
