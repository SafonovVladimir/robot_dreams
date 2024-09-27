import os

import requests
from typing import List, Dict, Any, Union
from dotenv import load_dotenv

load_dotenv()
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.
    :param date: data retrieve the data from
    :return: list of records
    """

    data = []
    page = 1
    status_code = 200

    while status_code == 200:

        response = requests.get(
            url=API_URL,
            params={"date": date, "page": page},
            headers={"Authorization": AUTH_TOKEN},
        )
        status_code = response.status_code
        page += 1
        if status_code == 200:
            data.extend(response.json())
    return data
