import os
import shutil
from pathlib import Path
from typing import List, Dict, Any

from lesson_02.job1.dal.sales_api import get_sales

date = "2022-08-11"
content = get_sales(date)


def remove_all_files_from_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    if json_content:
        lesson_directory = Path(os.getcwd()).parents[1]
        suffix_path = Path(path)
        storage_directory = Path(lesson_directory, suffix_path, date)

        file_number = 1

        if os.path.exists(storage_directory):
            remove_all_files_from_directory(storage_directory)
        else:
            print(f"All records for {date} have been added to local storage.")
            os.makedirs(storage_directory)

        os.chdir(storage_directory)

        for record in json_content:
            with open(f"sales_{date}_{file_number}", "w") as file:
                file.write(str(record))
            file_number += 1
    else:
        print("There are no sales records for this date!")


save_to_disk(content, "file_storage/raw/sales")
