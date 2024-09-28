import os
import time
import requests

from lesson_02.settings.constants import PROJECT_DIRECTORY

BASE_DIR = PROJECT_DIRECTORY

if not BASE_DIR:
    print("BASE_DIR environment variable must be set")
    exit(1)

JOB1_PORT = 8081
JOB2_PORT = 8082

RAW_DIR = os.path.join(BASE_DIR, "file_storage", "raw", "sales")
STG_DIR = os.path.join(BASE_DIR, "file_storage", "stg", "sales", "2022-08-09")


def test_run_job1():
    print("Starting job1:")
    resp = requests.post(
        url=f'http://localhost:{JOB1_PORT}/',
        json={
            "date": "2022-08-09",
            "raw_dir": RAW_DIR
        }
    )
    assert resp.status_code == 201
    print("job1 completed!")


def test_run_job2():
    print("Starting job2:")
    resp = requests.post(
        url=f'http://localhost:{JOB2_PORT}/',
        json={
            "raw_dir": RAW_DIR,
            "stg_dir": STG_DIR
        }
    )
    assert resp.status_code == 201
    print("job2 completed!")


if __name__ == '__main__':
    test_run_job1()
    time.sleep(3)
    test_run_job2()
