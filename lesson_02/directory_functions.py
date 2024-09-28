import os
import shutil
from pathlib import Path

from lesson_02.constants import PROJECT_DIRECTORY


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


def create_or_clean_is_exists_directory(content: bool, path: str, date: str) -> None:
    suffix_path = Path(path)
    storage_directory = Path(PROJECT_DIRECTORY, "file_storage", suffix_path, date)

    if content:
        if os.path.exists(storage_directory):
            remove_all_files_from_directory(storage_directory)
        else:
            os.makedirs(storage_directory)
        os.chdir(storage_directory)
