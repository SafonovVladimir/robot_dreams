import os
import shutil
from pathlib import Path
from typing import List, Dict, Any

from lesson_02.job1.dal.sales_api import get_sales


def remove_all_files_from_directory(directory: Path) -> None:
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))


def create_or_clean_is_exists_directory(json_content: bool, path: str, date: str) -> None:
    work_directory = Path(os.getcwd())
    suffix_path = Path(path)
    storage_directory = Path(work_directory, "file_storage", suffix_path, date)

    if json_content:
        if os.path.exists(storage_directory):
            remove_all_files_from_directory(storage_directory)
        else:
            os.makedirs(storage_directory)
        os.chdir(storage_directory)


def save_to_disk(date: str, json_content: List[Dict[str, Any]], path: str) -> None:
    if json_content:
        create_or_clean_is_exists_directory(True, path, date)
        file_number = 1

        for record in json_content:
            with open(f"sales_{date}_{file_number}.json", "w") as file:
                file.write(str(record))
            file_number += 1
        print(f"All records for {date} have been added to local storage.")

    else:
        create_or_clean_is_exists_directory(False, path, date)
        print("There are no sales records for this date!")


def get_data_and_safe_to_storage(date: str, raw_dir: str) -> None:
    content = get_sales(date)
    save_to_disk(date, content, raw_dir)
