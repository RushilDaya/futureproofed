# Description: This file contains the CRUD API for the measurement table
#              It is used to get, update, post and delete measurements

# missing safety features for loading invalid data (units/types etc) should add

from flask import Flask, request
import sqlite3
from application import DB_NAME
from application.data_models.object_models import (
    remove_from_db,
    get_measurement_from_db,
    load_record,
    Measurement,
)

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
    body = request.json

    # check for existing measurement
    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()
    existing_measurement = get_measurement_from_db(
        body["seic_code"], body["nrg_bal_code"], body["country_code"], body["year"], db_cursor
    )
    if existing_measurement is None:
        response = {"status": "404"}
        return response

    incoming_measurement = Measurement(
        seic_code=body["seic_code"],
        nrg_bal_code=body["nrg_bal_code"],
        country_code=body["country_code"],
        year=body["year"],
        measurement_value=float(body["measurement_value"]),
        measurement_unit=body["measurement_unit"],
        standardised_measurement_value=body.get("standardised_measurement_value"),
        standardised_measurement_unit=body.get("standardised_measurement_unit"),
    )

    # load the record into the database / standard load method does a replace
    # if the record exists so this is already in line with the PUT
    load_record(incoming_measurement, db_cursor, db_conn)
    response = {"status": "200"}
    return response

@app.route("/measurement/", methods=["POST"])
def postMeasurement():
    body = request.json

    # check for existing measurement
    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()
    existing_measurement = get_measurement_from_db(
        body["seic_code"], body["nrg_bal_code"], body["country_code"], body["year"], db_cursor
    )
    if existing_measurement is not None:
        response = {"status": "409"}
        return response

    incoming_measurement = Measurement(
        seic_code=body["seic_code"],
        nrg_bal_code=body["nrg_bal_code"],
        country_code=body["country_code"],
        year=body["year"],
        measurement_value=float(body["measurement_value"]),
        measurement_unit=body["measurement_unit"],
        standardised_measurement_value=body.get("standardised_measurement_value"),
        standardised_measurement_unit=body.get("standardised_measurement_unit"),
    )

    # load the record into the database
    load_record(incoming_measurement, db_cursor, db_conn)
    response = {"status": "200"}
    return response


@app.route("/measurement/", methods=["DELETE"])
def deleteMeasurement():
    args = request.args.to_dict()
    # check that the request contains all the required arguments / needs better error handling
    assert set(args.keys()) == set(["seic_code", "nrg_bal_code", "country_code", "year"])

    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()
    measurement = get_measurement_from_db(
        args["seic_code"], args["nrg_bal_code"], args["country_code"], args["year"], db_cursor
    )
    if measurement is None:
        response = {"status": "404"}
        return response
    remove_from_db(measurement, db_cursor, db_conn)

    response = {"status": "200"}
    return response


if __name__ == "__main__":
    app.run()
