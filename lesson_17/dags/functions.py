import os

from airflow.providers.google.cloud.hooks.gcs import GCSHook


def list_csv_sales_files(bucket_name: str, folder_name: str) -> list:
    """
    List all CSV files in a robot_dreams_hw_17/data/sales bucket, including subfolders.
    """
    gcs_hook = GCSHook()
    files = gcs_hook.list(bucket_name, prefix=folder_name)
    sales_csv_files = [file for file in files if file.endswith(".csv")]
    return sales_csv_files


def list_csv_customers_files(bucket_name: str, folder_name: str) -> list:
    """
    List all CSV files in a robot_dreams_hw_17/data/customers bucket, including subfolders.
    """

    customers_file_name_list = []
    result_customers_files_list = []

    gcs_hook = GCSHook()
    files = gcs_hook.list(bucket_name, prefix=folder_name)

    customers_csv_files = [file for file in files if file.endswith(".csv")]

    for customers_file in customers_csv_files:
        if os.path.basename(customers_file) not in customers_file_name_list:
            customers_file_name_list.append(os.path.basename(customers_file))
            result_customers_files_list.append(customers_file)

    return result_customers_files_list


def read_sql_file(file_path):
    """Reads a SQL file and returns its content."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
