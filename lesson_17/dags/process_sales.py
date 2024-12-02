import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

from airflow.utils.task_group import TaskGroup
from constants import BUCKET_NAME, SALES_FOLDER_PATH, SALES_DATASET_STAGING_TABLE, MERGE_SALES_SQL_PATH, \
    TRANSFORM_SALES_TO_SILVER_PATH
from functions import list_csv_sales_files, read_sql_file

with DAG(
        "process_sales",
        schedule_interval="@daily",
        start_date=datetime(2022, 9, 1),
        catchup=False,
) as dag:
    csv_files = list_csv_sales_files(BUCKET_NAME, SALES_FOLDER_PATH)

    with TaskGroup("upload_sales_csv_files") as sales_group:
        previous_task = None

        for file in csv_files:
            load_to_bronze = GCSToBigQueryOperator(
                task_id=f"upload_{os.path.basename(file)}",
                bucket=BUCKET_NAME,
                source_objects=[file],
                destination_project_dataset_table=SALES_DATASET_STAGING_TABLE,
                source_format="CSV",
                write_disposition="WRITE_TRUNCATE",
                skip_leading_rows=1,
                autodetect=False,
                schema_fields=[
                    {"name": "CustomerId", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
                ],
            )

            # Task to run the MERGE SQL query after the CSV file is uploaded
            merge_uniq_data = BigQueryInsertJobOperator(
                task_id=f"merge_{os.path.basename(file)}",
                configuration={
                    "query": {
                        "query": read_sql_file(MERGE_SALES_SQL_PATH),
                        "useLegacySql": False,
                    }
                }
            )

            if previous_task:
                previous_task >> load_to_bronze
            load_to_bronze >> merge_uniq_data
            previous_task = merge_uniq_data

    transform_to_silver = BigQueryExecuteQueryOperator(
        task_id="transform_sales_to_silver",
        sql=read_sql_file(TRANSFORM_SALES_TO_SILVER_PATH),
        use_legacy_sql=False,
    )

    sales_group >> transform_to_silver
