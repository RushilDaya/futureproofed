from fastapi import FastAPI, Response, status
from pydantic import BaseModel
import sqlite3
from enum import Enum
import json
from typing import List, Optional
from application.data_models.object_models import (
    get_measurement_from_db,
    Measurement,
    load_record,
    remove_from_db
)
from application import (
    db_instance,
    ENERGY_BALANCE_DATA_JSON,
    COUNTRY_DATA_JSON,
    SEIC_DATA_JSON,
    YEAR_DATA_JSON,
)

#TODO: the type of YEAR is not being done correctly

app = FastAPI()


def load_keys(file_path: str) -> List:
    # required for Enum to work in fastapi
    obj = json.load(open(file_path))
    keys = list(obj.keys())
    return {str(i): k for i, k in enumerate(keys)}


SEIC_CODES_ENUM = Enum("SEIC_CODE", load_keys(SEIC_DATA_JSON))
NRG_BAL_CODES_ENUM = Enum("NRG_BAL_CODE", load_keys(ENERGY_BALANCE_DATA_JSON))
COUNTRY_CODES_ENUM = Enum("COUNTRY_CODE", load_keys(COUNTRY_DATA_JSON))
YEARS_ENUM = Enum("YEAR", load_keys(YEAR_DATA_JSON))

class MeasurementBody(BaseModel):
    # TODO: should be combined with Measurement object
    seic_code: SEIC_CODES_ENUM
    nrg_bal_code: NRG_BAL_CODES_ENUM
    country_code: COUNTRY_CODES_ENUM
    year: YEARS_ENUM
    measurement_value: float
    measurement_unit: str
    standardised_measurement_value: Optional[float] = None
    standardised_measurement_unit: Optional[str] = None

@app.get("/measurement", status_code=status.HTTP_200_OK)
def getMeasurement(
    seic_code: SEIC_CODES_ENUM,
    nrg_bal_code: NRG_BAL_CODES_ENUM,
    country_code: COUNTRY_CODES_ENUM,
    year: YEARS_ENUM,
    response: Response,
):
    measurement = get_measurement_from_db(
        seic_code.value, nrg_bal_code.value, country_code.value, int(year.value), db_instance
    )
    if measurement is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    return measurement.pretty()

@app.put("/measurement", status_code=status.HTTP_200_OK)
def updateMeasurement(body: MeasurementBody, response: Response):
    existing_measurement = get_measurement_from_db(
        body.seic_code.value, 
        body.nrg_bal_code.value, 
        body.country_code.value, 
        int(body.year.value), 
        db_instance
    )
    if existing_measurement is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    
    incoming_measurement = Measurement(
        seic_code=body.seic_code.value,
        nrg_bal_code=body.nrg_bal_code.value,
        country_code=body.country_code.value,
        year=int(body.year.value),
        measurement_value=body.measurement_value,
        measurement_unit=body.measurement_unit,
        standardised_measurement_value=body.standardised_measurement_value,
        standardised_measurement_unit=body.standardised_measurement_unit
    )
    load_record(incoming_measurement, db_instance)
    return

@app.post("/measurement", status_code=status.HTTP_200_OK)
def createMeasurement(body: MeasurementBody, response: Response):
    existing_measurement = get_measurement_from_db(
        body.seic_code.value, 
        body.nrg_bal_code.value, 
        body.country_code.value, 
        int(body.year.value), 
        db_instance
    )
    if existing_measurement is not  None:
        response.status_code = status.HTTP_409_CONFLICT
        return
    
    incoming_measurement = Measurement(
        seic_code=body.seic_code.value,
        nrg_bal_code=body.nrg_bal_code.value,
        country_code=body.country_code.value,
        year=int(body.year.value),
        measurement_value=body.measurement_value,
        measurement_unit=body.measurement_unit,
        standardised_measurement_value=body.standardised_measurement_value,
        standardised_measurement_unit=body.standardised_measurement_unit
    )
    load_record(incoming_measurement, db_instance)
    return

@app.delete("/measurement", status_code=status.HTTP_200_OK)
def deleteMeasurement(
    seic_code: SEIC_CODES_ENUM,
    nrg_bal_code: NRG_BAL_CODES_ENUM,
    country_code: COUNTRY_CODES_ENUM,
    year: YEARS_ENUM,
    response: Response,
):
    measurement = get_measurement_from_db(
        seic_code.value,
        nrg_bal_code.value,
        country_code.value,
        int(year.value),
        db_instance
    )
    if measurement == None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    
    remove_from_db(measurement, db_instance)
    return