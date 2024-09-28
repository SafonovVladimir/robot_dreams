"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.job1.bll.sales_api import save_sales_to_local_disk

app = Flask(__name__)


@app.route("/", methods=["POST"])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "date: "2022-08-09",
      "raw_dir": "path_to_my_dir/raw/sales"
    }
    """
    input_data: dict = request.json
    date = input_data.get("date")
    raw_dir = input_data.get("raw_dir")

    if not date:
        return {
            "message": "date parameter missed",
        }, 400
    if not raw_dir:
        return {
            "message": "raw_dir (path to file storage) parameter missed",
        }, 400

    message = save_sales_to_local_disk(date=date, raw_dir=raw_dir)

    return {
        "message": f"{message}",
    }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
