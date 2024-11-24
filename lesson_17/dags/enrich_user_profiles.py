from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

with DAG(
        "enrich_user_profiles",
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
) as dag:
    enrich_profiles = BigQueryExecuteQueryOperator(
        task_id="enrich_profiles",
        sql="""
            CREATE OR REPLACE TABLE sep2024-volodymyr-safonov.gold.user_profiles_enriched AS
            SELECT
                c.client_id,
                COALESCE(c.first_name, SPLIT(u.full_name, " ")[OFFSET(0)]) AS first_name,
                COALESCE(c.last_name, SPLIT(u.full_name, " ")[OFFSET(1)]) AS last_name,
                c.email,
                c.registration_date,
                COALESCE(c.state, u.state) AS state,
                u.phone_number,
                u.birth_date
            FROM sep2024-volodymyr-safonov.silver.customers c
            LEFT JOIN sep2024-volodymyr-safonov.silver.user_profiles u
            ON c.email = u.email
        """,
        use_legacy_sql=False,
    )
