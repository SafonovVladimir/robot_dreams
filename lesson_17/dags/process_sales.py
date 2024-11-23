import os

from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

from airflow.utils.task_group import TaskGroup
from constants import BUCKET_NAME, SALES_FOLDER_PATH, PROJECT_DATASET_STAGING_TABLE


def list_csv_sales_files(bucket_name: str, folder_name: str) -> list:
    """
    List all CSV files in a robot_dreams_hw_17/data/sales bucket, including subfolders.
    """
    gcs_hook = GCSHook()
    files = gcs_hook.list(bucket_name, prefix=folder_name)
    csv_files = [file for file in files if file.endswith(".csv")]
    return csv_files


# SQL query to avoid duplicate data via MERGE
merge_sql = """
MERGE INTO `sep2024-volodymyr-safonov.bronze.sales` AS target
USING `sep2024-volodymyr-safonov.bronze.staging_sales` AS source
ON target.CustomerId = source.CustomerId
   AND target.PurchaseDate = source.PurchaseDate
   AND target.Product = source.Product
   AND target.Price = source.Price
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerId, PurchaseDate, Product, Price)
    VALUES (source.CustomerId, source.PurchaseDate, source.Product, source.Price)
"""

with DAG(
        "process_sales",
        schedule_interval="@daily",
        start_date=datetime(2022, 9, 1),
        catchup=False,
) as dag:
    csv_files = list_csv_sales_files(BUCKET_NAME, SALES_FOLDER_PATH)

    with TaskGroup("upload_csv_files_group") as upload_group:
        previous_task = None  # Track the previous task for dynamic chaining

        for file in csv_files:
            load_to_bronze = GCSToBigQueryOperator(
                task_id=f"upload_{os.path.basename(file)}",
                bucket=BUCKET_NAME,
                source_objects=[file],
                destination_project_dataset_table=PROJECT_DATASET_STAGING_TABLE,
                source_format="CSV",
                write_disposition="WRITE_TRUNCATE",
                skip_leading_rows=1,
                autodetect=False,
                schema_fields=[  # Define schema explicitly
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
                        "query": merge_sql,
                        "useLegacySql": False,
                    }
                }
            )

            # Set dependencies so merge runs right after the current file is uploaded
            if previous_task:
                previous_task >> load_to_bronze

            load_to_bronze >> merge_uniq_data

            previous_task = merge_uniq_data