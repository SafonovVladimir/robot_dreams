from typing import List, Dict, Any

from lesson_02.functions.directory_functions import create_or_clean_is_exists_directory


def save_to_disk(date: str, json_content: List[Dict[str, Any]], path: str) -> str:
    if json_content:
        create_or_clean_is_exists_directory(True, path, date)
        file_number = 1

        for record in json_content:
            with open(f"sales_{date}_{file_number}.json", "w") as file:
                file.write(str(record))
            file_number += 1
        return f"All records for {date} have been added to local storage."

    else:
        create_or_clean_is_exists_directory(False, path, date)
        return "There are no sales records for this date!"
