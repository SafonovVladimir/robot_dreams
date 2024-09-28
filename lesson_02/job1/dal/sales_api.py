import os

import requests
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")

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
        try:
            response = requests.get(
                url=API_URL,
                params={"date": date, "page": page},
                headers={"Authorization": AUTH_TOKEN},
            )
            status_code = response.status_code
            page += 1
            if status_code == 200:
                data.extend(response.json())
        except requests.exceptions.HTTPError as error:
            raise SystemExit(error)

    return data
