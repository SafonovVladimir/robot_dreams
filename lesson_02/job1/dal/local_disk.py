import os
import shutil
from pathlib import Path
from typing import List, Dict, Any

from lesson_02.job1.dal.sales_api import get_sales

date = "2022-08-09"
content = get_sales(date)

def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    if json_content is None:
        json_content = []

    lesson_directory = Path(os.getcwd()).parents[1]
    suffix_path = Path(path)
    storage_directory = Path(lesson_directory, suffix_path)

    if os.path.exists(storage_directory):
        for filename in os.listdir(storage_directory):
            file_path = os.path.join(storage_directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    else:
        print(f"Creating directory {storage_directory}")
        os.makedirs(storage_directory)


save_to_disk(content, "file_storage/raw/sales")