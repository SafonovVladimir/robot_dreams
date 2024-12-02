import os

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
from datetime import datetime

from airflow.utils.task_group import TaskGroup

from constants import BUCKET_NAME, CUSTOMERS_FOLDER_PATH, CUSTOMERS_DATASET_STAGING_TABLE, \
    MERGE_CUSTOMERS_SQL_PATH, TRANSFORM_CUSTOMERS_TO_SILVER_PATH
from functions import list_csv_customers_files, read_sql_file

with DAG(
        "process_customers",
        schedule_interval="@daily",
        start_date=datetime(2022, 9, 1),
        catchup=False,
) as dag:
    csv_files = list_csv_customers_files(BUCKET_NAME, CUSTOMERS_FOLDER_PATH)

    with TaskGroup("upload_customers_csv_files") as customers_group:
        previous_task = None

        for file in csv_files:
            load_to_bronze = GCSToBigQueryOperator(
                task_id=f"upload_{os.path.basename(file)}",
                bucket=BUCKET_NAME,
                source_objects=[file],
                destination_project_dataset_table=CUSTOMERS_DATASET_STAGING_TABLE,
                write_disposition="WRITE_TRUNCATE",
                skip_leading_rows=1,
                autodetect=False,
                schema_fields=[
                    {"name": "Id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "State", "type": "STRING", "mode": "NULLABLE"},
                ],
            )

            # Task to run the MERGE SQL query after the CSV file is uploaded
            merge_uniq_data = BigQueryInsertJobOperator(
                task_id=f"merge_{os.path.basename(file)}",
                configuration={
                    "query": {
                        "query": read_sql_file(MERGE_CUSTOMERS_SQL_PATH),
                        "useLegacySql": False,
                    }
                }
            )

            if previous_task:
                previous_task >> load_to_bronze

            load_to_bronze >> merge_uniq_data

            previous_task = merge_uniq_data

    # Transform bronze.customers -> silver.customers
    transform_to_silver = BigQueryExecuteQueryOperator(
        task_id="transform_customers_to_silver",
        sql=read_sql_file(TRANSFORM_CUSTOMERS_TO_SILVER_PATH),
        use_legacy_sql=False,
    )

    customers_group >> transform_to_silver
