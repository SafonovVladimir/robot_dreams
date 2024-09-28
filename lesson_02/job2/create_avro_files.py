import json
import os
from os import listdir
from os.path import isfile, join
from pathlib import Path

from fastavro import writer

from lesson_02.directory_functions import create_or_clean_is_exists_directory
from lesson_02.job2.avro_schema import schema
from lesson_02.constants import PROJECT_DIRECTORY


def get_json_files(directory: str, date: str) -> list:
    json_directory = Path(PROJECT_DIRECTORY, "file_storage", directory, date)
    files_list = []

    try:
        with os.scandir(json_directory) as it:
            if not any(it):
                raise OSError("Directory is empty")

            for json_file in listdir(json_directory):
                if isfile(join(json_directory, json_file)):
                    files_list.append((join(json_directory, json_file), json_file))
    except OSError as e:
        print("Error:", e)

    return files_list


def get_path_n_date_from_path(full_path):
    path = "/".join(full_path.split("/")[:-1])
    date = full_path.split("/")[-1]
    return path, date


def create_avro_files(raw_path, stg_full_path) -> str:
    date = get_path_n_date_from_path(stg_full_path)[1]
    stg_path = get_path_n_date_from_path(stg_full_path)[0]

    create_or_clean_is_exists_directory(True, stg_path, date)

    path_to_avro_files = Path(PROJECT_DIRECTORY, "file_storage", stg_path, date)

    try:
        for json_file in get_json_files(raw_path, date):
            with open(json_file[0], "r") as input_file:
                with open(
                        f"{Path(path_to_avro_files, json_file[1].split('.')[0])}"
                        f".avro", "wb"
                ) as avro_file:
                    writer(
                        avro_file, schema,
                        [
                            dict(
                                json.loads(input_file.read().replace("'", '"'))
                            )
                        ]
                    )
    except OSError as e:
        print("Error:", e)
