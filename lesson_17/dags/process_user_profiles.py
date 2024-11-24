from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from constants import BUCKET_NAME, USER_PROFILES_PATH, USER_PROFILES_SILVER_DATASET_TABLES

with DAG(
        "process_user_profiles",
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
) as dag:
    load_to_silver = GCSToBigQueryOperator(
        task_id="load_user_profiles_to_silver",
        bucket=BUCKET_NAME,
        source_objects=[f"{USER_PROFILES_PATH}*.json"],
        destination_project_dataset_table=USER_PROFILES_SILVER_DATASET_TABLES,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
    )
