from typing import Optional, List
from dataclasses import dataclass
from application  import DatabaseObject

@dataclass
class Measurement:
    seic_code: str
    nrg_bal_code: str
    country_code: str
    year: int
    measurement_value: float
    measurement_unit: str
    standardised_measurement_value: Optional[float]
    standardised_measurement_unit: Optional[str]

    def pretty(self) -> dict:
        return {
            "seic_code": self.seic_code,
            "nrg_bal_code": self.nrg_bal_code,
            "country_code": self.country_code,
            "year": self.year,
            "measurement_value": self.measurement_value,
            "measurement_unit": self.measurement_unit,
            "standardised_measurement_value": self.standardised_measurement_value,
            "standardised_measurement_unit": self.standardised_measurement_unit,
        }


# this is a configuration object:
# it maps the csv columns in the source to the object attributes
# should be moved out of the code
# this is not very robust as it is quite "far" from the csv file ingestion point
csv_ob_map = {
    "seic_code": 4,
    "nrg_bal_code": 3,
    "country_code": 6,
    "year": 7,
    "measurement_value": 8,
    "measurement_unit": 5,
}

# order of columns in the database measurements table to map to the object
# not very elegant, improvement would be to use a proper ORM (sqlalchemy) for this instead
db_ob_map = {
    "seic_code": 0,
    "nrg_bal_code": 1,
    "country_code": 2,
    "year": 3,
    "measurement_value": 4,
    "measurement_unit": 5,
    "standardised_measurement_value": 6,
    "standardised_measurement_unit": 7,
}


def create_measurement_object_from_csv_row(raw_measurement: List[str]) -> Measurement:
    # should make this a configuration instead of hardcoding
    return Measurement(
        seic_code=raw_measurement[csv_ob_map["seic_code"]],
        nrg_bal_code=raw_measurement[csv_ob_map["nrg_bal_code"]],
        country_code=raw_measurement[csv_ob_map["country_code"]],
        year=raw_measurement[csv_ob_map["year"]],
        measurement_value=float(raw_measurement[csv_ob_map["measurement_value"]]),
        measurement_unit=raw_measurement[csv_ob_map["measurement_unit"]],
        standardised_measurement_value=None,
        standardised_measurement_unit=None,
    )


def get_measurement_from_db(
    seic_code: str, nrg_bal_code: str, country_code: str, year: int, db: DatabaseObject
) -> Optional[Measurement]:
    db.db_cursor.execute(
        """
              SELECT * FROM measurement where
                seic_code = ? and
                nrg_bal_code = ? and
                country_code = ? and
                year = ?
        """,
        (
            seic_code,
            nrg_bal_code,
            country_code,
            year,
        ),
    )
    records = db.db_cursor.fetchall()
    if len(records) > 1:
        raise ValueError("More than one record found for the same PK")
    if len(records) == 0:
        return None

    record = records[0]
    return Measurement(
        seic_code=record[db_ob_map["seic_code"]],
        nrg_bal_code=record[db_ob_map["nrg_bal_code"]],
        country_code=record[db_ob_map["country_code"]],
        year=record[db_ob_map["year"]],
        measurement_value=record[db_ob_map["measurement_value"]],
        measurement_unit=record[db_ob_map["measurement_unit"]],
        standardised_measurement_value=record[db_ob_map["standardised_measurement_value"]],
        standardised_measurement_unit=record[db_ob_map["standardised_measurement_unit"]],
    )


def load_record(measurement: Measurement, db: DatabaseObject) -> bool:
    # this load function essentially does the following logic
    # does an insert or replace thus the measurement provided is the source of truth
    # if the measurement is already in the database, it will be replaced as it is assumed
    # to be the same / or less complete than the incoming measurement

    db.db_cursor.execute(
        """
            INSERT OR REPLACE INTO 
            measurement (seic_code, 
                         nrg_bal_code, 
                         country_code, year, 
                         measurement_value, 
                         measurement_unit, 
                         standardised_measurement_value, 
                         standardised_measurement_unit)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            measurement.seic_code,
            measurement.nrg_bal_code,
            measurement.country_code,
            measurement.year,
            measurement.measurement_value,
            measurement.measurement_unit,
            measurement.standardised_measurement_value,
            measurement.standardised_measurement_unit,
        ),
    )
    db.db_conn.commit()
    return True


def remove_from_db(measurement: Measurement, db: DatabaseObject) -> None:
    db.db_cursor.execute(
        """
              DELETE FROM measurement where
                seic_code = ? and
                nrg_bal_code = ? and
                country_code = ? and
                year = ?
        """,
        (
            measurement.seic_code,
            measurement.nrg_bal_code,
            measurement.country_code,
            measurement.year,
        ),
    )
    db.db_conn.commit()
    return None
