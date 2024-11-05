import requests


# Define the function which trigger the first job (extract data from API)
def extract_data_from_api(execution_date):
    date_str = execution_date.strftime("%Y-%m-%d")
    raw_dir = "raw/sales"

    response = requests.post(
        url="http://host.docker.internal:8081/",
        json={
            "date": date_str,
            "raw_dir": raw_dir
        },
    )

    if response.status_code == 201:
        return "Data extraction successful."
    else:
        raise ValueError(f"Failed to extract data for {date_str}: {response.text}")


# Define the function which trigger the second job (convert to Avro)
def convert_to_avro(execution_date):
    date_str = execution_date.strftime("%Y-%m-%d")
    raw_dir = "raw/sales"
    stg_dir = f"stg/sales/{date_str}"

    response = requests.post(
        url="http://host.docker.internal:8082/",
        json={
            "raw_dir": raw_dir,
            "stg_dir": stg_dir
        },
    )

    if response.status_code == 201:
        return "Data conversion to avro successful."
    else:
        raise ValueError(f"Failed to convert data for {date_str}: {response.text}")
