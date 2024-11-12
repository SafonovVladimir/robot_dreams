import os

from airflow.models import Variable


# Function to recursively get all files
def get_files_with_relative_paths(root_dir):
    files_list = []
    date_str = Variable.get("upload_date")
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(full_path, root_dir)
            if relative_path.split('/')[-2] == date_str:
                files_list.append(full_path)
    return files_list
