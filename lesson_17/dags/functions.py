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

    print("~" * 200)
    print(f"{customers_csv_files=}")
    print(f"{customers_file_name_list=}")
    print(f"{result_customers_files_list=}")
    print("~" * 200)
    return result_customers_files_list
