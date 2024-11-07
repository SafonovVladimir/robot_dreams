import asyncio

from aiogram import Bot
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from constants import UPLOAD_DATE


# Fetch the bot token from Airflow's connection
def get_telegram_bot_token():
    conn = BaseHook.get_connection("telegram_bot")
    return conn.password


TELEGRAM_BOT_TOKEN = get_telegram_bot_token()
if not TELEGRAM_BOT_TOKEN:
    print("TELEGRAM_BOT_TOKEN must be set")

# Fetch CHAT_ID from Airflow variables
CHAT_ID = Variable.get("CHAT_ID")
if not CHAT_ID:
    print("CHAT_ID variable must be set")


# Define functions to send a Telegram message
async def send_telegram_message(message: str):
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    async with bot:
        await bot.send_message(chat_id=CHAT_ID, text=message)


def send_success_notification(**kwargs):
    dag_id = kwargs['dag'].dag_id
    message = (
        f"✅ DAG '{dag_id}' completed successfully! \n"
        f"All records for date {UPLOAD_DATE} have been uploaded! 😊"
    )
    asyncio.run(send_telegram_message(message))


def send_failure_alert(context, **kwargs):
    dag_id = kwargs['dag'].dag_id
    message = (
        f"❌ Task {dag_id} has failed! \n"
        f"🔑 Task Instance Key: {context['task_instance_key_str']} \n"
        f"🗓️ Execution Date: {context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')}"
    )
    asyncio.run(send_telegram_message(message))


def send_no_files_notification():
    message = (
        f"❌ For date {UPLOAD_DATE} no Files Found for Upload! 😞 \n"
        f"Please, check uploading date."
    )
    asyncio.run(send_telegram_message(message))
