from lesson_02.job1.dal.local_disk import save_to_disk
from lesson_02.job1.dal.sales_api import get_sales


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    content = get_sales(date)
    save_to_disk(date, content, raw_dir)
