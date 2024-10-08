import asyncio
import os

import requests
from aiogram import Bot
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    print("TELEGRAM_BOT_TOKEN environment variable must be set")

CHAT_ID = os.getenv("CHAT_ID")
if not CHAT_ID:
    print("CHAT_ID environment variable must be set")


# Define a function to send a Telegram message
async def send_telegram_message(message: str):
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    async with bot:
        await bot.send_message(chat_id=CHAT_ID, text=message)


def send_success_notification(execution_date):
    date_str = execution_date.strftime("%Y-%m-%d")
    message = (
        f"DAG 'process_sales' completed successfully. "
        f"All data for date {date_str} have been extracted and converted"
    )
    asyncio.run(send_telegram_message(message))


# Define the DAG
with DAG(
        "process_sales",
        default_args=default_args,
        description="A simple sales processing DAG",
        schedule_interval="0 1 * * *",
        catchup=True,  # Виконувати DAG для попередніх днів
        max_active_runs=1,  # Обмежити кількість активних запусків до 1
) as dag:
    # Define the function to trigger the first job (extract data from API)
    def extract_data_from_api(execution_date):
        date_str = execution_date.strftime("%Y-%m-%d")
        raw_dir = "raw/sales"

        response = requests.post(
            url="http://host.docker.internal:8081/",
            json={
                "date": date_str,
                "raw_dir": raw_dir
            },
        )

        if response.status_code == 201:
            return "Data extraction successful."
        else:
            raise ValueError(f"Failed to extract data for {date_str}: {response.text}")


    # Define the function to trigger the second job (convert to Avro)
    def convert_to_avro(execution_date):
        date_str = execution_date.strftime("%Y-%m-%d")
        raw_dir = "raw/sales"
        stg_dir = f"stg/sales/{date_str}"

        response = requests.post(
            url="http://host.docker.internal:8082/",
            json={
                "raw_dir": raw_dir,
                "stg_dir": stg_dir
            },
        )

        if response.status_code == 201:
            return "Data conversion to avro successful."
        else:
            raise ValueError(f"Failed to convert data for {date_str}: {response.text}")


    # Define tasks
    extract_data_task = PythonOperator(
        task_id="extract_data_from_api",
        provide_context=True,
        python_callable=extract_data_from_api,
        dag=dag,
    )

    convert_to_avro_task = PythonOperator(
        task_id="convert_to_avro",
        provide_context=True,
        python_callable=convert_to_avro,
        dag=dag,
    )

    send_notification_task = PythonOperator(
        task_id="send_telegram_notification",
        provide_context=True,
        python_callable=send_success_notification,
        dag=dag,
    )

    # Dependencies of tasks
    extract_data_task >> convert_to_avro_task >> send_notification_task
