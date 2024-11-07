from airflow.models import Variable
from datetime import datetime

from constants import LOCAL_ROOT_DIRECTORY
from functions.job_functions import get_files_with_relative_paths

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Variable name should be "upload_date"
date_str = Variable.get("upload_date")
date = datetime.strptime(date_str, "%Y-%m-%d")
formatted_date = date.strftime("%Y/%m/%d")

files_to_upload = get_files_with_relative_paths(LOCAL_ROOT_DIRECTORY)