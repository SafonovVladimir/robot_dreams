from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.job2.create_avro_files import create_avro_files

app = Flask(__name__)


@app.route("/", methods=["POST"])
def main() -> flask_typing.ResponseReturnValue:
    """
    Proposed POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales",
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09",
    }
    """

    input_data: dict = request.json
    raw_dir = input_data.get("raw_dir")
    stg_dir = input_data.get("stg_dir")

    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400
    if not stg_dir:
        return {
            "message": "stg_dir parameter missed",
        }, 400

    create_avro_files(raw_dir, stg_dir)

    return {
        "message": "Data save successfully to local storage",
    }, 201


if __name__ == "__main__":
    app.run(host="localhost", port=8082)
