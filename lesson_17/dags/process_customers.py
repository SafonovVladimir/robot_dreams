import os

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
from datetime import datetime

from airflow.utils.task_group import TaskGroup

from constants import BUCKET_NAME, CUSTOMERS_FOLDER_PATH, CUSTOMERS_DATASET_STAGING_TABLE
from functions import list_csv_customers_files


# SQL query to avoid duplicate data via MERGE
merge_customers_sql = """
MERGE INTO `sep2024-volodymyr-safonov.bronze.customers` AS target
USING `sep2024-volodymyr-safonov.bronze.staging_customers` AS source
ON target.Id = source.Id
WHEN MATCHED THEN
    UPDATE SET 
        FirstName = source.FirstName,
        LastName = source.LastName,
        Email = source.Email,
        RegistrationDate = source.RegistrationDate,
        State = source.State
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Id, FirstName, LastName, Email, RegistrationDate, State)
    VALUES (source.Id, source.FirstName, source.LastName, source.Email, source.RegistrationDate, source.State);
"""


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
                        "query": merge_customers_sql,
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
        sql="""
            CREATE OR REPLACE TABLE sep2024-volodymyr-safonov.silver.customers AS
            SELECT
                SAFE_CAST(Id AS INT64) AS client_id,
                SAFE_CAST(FirstName AS STRING) AS first_name,
                SAFE_CAST(LastName AS STRING) AS last_name,
                SAFE_CAST(Email AS STRING) AS email,
                SAFE.PARSE_DATE('%Y-%m-%d', RegistrationDate) AS registration_date,
                SAFE_CAST(State AS STRING) AS state
            FROM sep2024-volodymyr-safonov.bronze.customers
            WHERE
                Id IS NOT NULL
                AND Email IS NOT NULL
                AND RegistrationDate IS NOT NULL
        """,
        use_legacy_sql=False,
    )

    customers_group >> transform_to_silver
