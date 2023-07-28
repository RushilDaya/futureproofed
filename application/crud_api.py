# Description: This file contains the CRUD API for the measurement table
#              It is used to get, update, post and delete measurements

from flask import Flask, request
import sqlite3
from application import DB_NAME
from application.data_models.object_models import get_measurement_from_db

app = Flask(__name__)


@app.route("/measurement/", methods=["GET"])
def getMeasurement():
    args = request.args.to_dict()
    # check that the request contains all the required arguments / needs better error handling
    assert set(args.keys()) == set(["seic_code", "nrg_bal_code", "country_code", "year"])

    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()
    # get the measurement from the database
    measurement = get_measurement_from_db(
        args["seic_code"], args["nrg_bal_code"], args["country_code"], args["year"], db_cursor
    )

    response = {}
    if measurement is None:
        response["status"] = "404"
    else:
        response["status"] = "200"
        response["res"] = measurement.pretty()
    return response


@app.route("/measurement/", methods=["PUT"])
def updateMeasurement():
    return "Hello, World!"


@app.route("/measurement/", methods=["POST"])
def postMeasurement():
    return "Hello, World!"


@app.route("/measurement/", methods=["DELETE"])
def deleteMeasurement():
    return "Hello, World!"


if __name__ == "__main__":
    app.run()
