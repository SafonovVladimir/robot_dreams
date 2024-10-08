from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from functions.job_functions import extract_data_from_api, convert_to_avro
from functions.notifications import send_failure_alert, send_success_notification

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 9),
    "end_date": datetime(2022, 8, 12),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
        "process_sales",
        default_args=default_args,
        description="A simple sales processing DAG",
        schedule_interval="0 1 * * *",
        catchup=True,
        on_success_callback=None,
        on_failure_callback=send_failure_alert,
        max_active_runs=1,

) as dag:
    extract_data_task = PythonOperator(
        task_id="extract_data_from_api",
        provide_context=True,
        python_callable=extract_data_from_api,
    )

    convert_to_avro_task = PythonOperator(
        task_id="convert_to_avro",
        provide_context=True,
        python_callable=convert_to_avro,
    )

    send_notification_task = PythonOperator(
        task_id="send_telegram_notification",
        provide_context=True,
        python_callable=send_success_notification,
    )

    # Dependencies of tasks
    extract_data_task >> convert_to_avro_task >> send_notification_task
