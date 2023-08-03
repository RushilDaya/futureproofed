from fastapi import FastAPI, Response, status
import sqlite3
from enum import Enum
import json
from typing import List
from application.data_models.object_models import get_measurement_from_db
from application import (
    DB_NAME,
    ENERGY_BALANCE_DATA_JSON,
    COUNTRY_DATA_JSON,
    SEIC_DATA_JSON,
    YEAR_DATA_JSON,
)

app = FastAPI()


def load_keys(file_path: str) -> List:
    # required for Enum to work in fastapi
    obj = json.load(open(file_path))
    keys = list(obj.keys())
    return {str(i): k for i, k in enumerate(keys)}

SEIC_CODES = Enum("SEIC_CODE", load_keys(SEIC_DATA_JSON))
NRG_BAL_CODES = Enum("NRG_BAL_CODE", load_keys(ENERGY_BALANCE_DATA_JSON))
COUNTRY_CODES = Enum("COUNTRY_CODE", load_keys(COUNTRY_DATA_JSON))
YEARS = Enum("YEAR", load_keys(YEAR_DATA_JSON))


@app.get("/measurement", status_code=status.HTTP_200_OK)
def getMeasurement(
    seic_code: SEIC_CODES,
    nrg_bal_code: NRG_BAL_CODES,
    country_code: COUNTRY_CODES,
    year: YEARS,
    response: Response,
):
    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()

    measurement = get_measurement_from_db(
        seic_code.value, nrg_bal_code.value, country_code.value, year.value, db_cursor
    )
    if measurement == None:
        response.status_code = status.HTTP_404_NOT_FOUND
    return measurement.pretty()
