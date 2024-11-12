from datetime import datetime

from constants import LOCAL_ROOT_DIRECTORY, UPLOAD_DATE
from functions.job_functions import get_files_with_relative_paths

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Variable name should be "upload_date"
date = datetime.strptime(UPLOAD_DATE, "%Y-%m-%d")
formatted_date = date.strftime("%Y/%m/%d")

files_to_upload = get_files_with_relative_paths(LOCAL_ROOT_DIRECTORY)