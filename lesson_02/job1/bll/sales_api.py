from lesson_02.job1.dal.local_disk import save_to_disk
from lesson_02.job1.dal.sales_api import get_sales


def save_sales_to_local_disk(date: str, raw_dir: str) -> str:
    message = save_to_disk(date, get_sales(date), raw_dir)
    return message
