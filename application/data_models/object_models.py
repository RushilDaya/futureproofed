from typing import Optional, List
from dataclasses import dataclass

@dataclass
class Measurement:
    seic_code: str
    nrg_bal_code: str
    country_code: str
    year: int
    measurement_value: float
    measurent_unit: str
    standardised_measurement_value: Optional[float]
    standardised_measurement_unit: Optional[str]

# this is a configuration object:
# it maps the csv columns in the source to the object attributes
# should be moved out of the code
# this is not very robust as it is quite "far" from the csv file ingestion point
csv_ob_map = {
    "seic_code":4,
    "nrg_bal_code":3,
    "country_code":6,
    "year":7,
    "measurement_value":8,
    "measurement_unit":5
}

# order of columns in the database measurements table to map to the object
# not very elegant, improvement would be to use a proper ORM (sqlalchemy) for this instead
db_ob_map = {
    "seic_code":0,
    "nrg_bal_code":1,
    "country_code":2,
    "year":3,
    "measurement_value":4,
    "measurement_unit":5,
    "standardised_measurement_value":6,
    "standardised_measurement_unit":7
}

def create_measurement_object_from_csv_row(raw_measurement: List[str]) -> Measurement:
    # should make this a configuration instead of hardcoding
    return Measurement(
        seic_code=raw_measurement[csv_ob_map["seic_code"]],
        nrg_bal_code=raw_measurement[csv_ob_map["nrg_bal_code"]],
        country_code=raw_measurement[csv_ob_map["country_code"]],
        year=raw_measurement[csv_ob_map["year"]],
        measurement_value=float(raw_measurement[csv_ob_map["measurement_value"]]),
        measurent_unit=raw_measurement[csv_ob_map["measurement_unit"]],
        standardised_measurement_value=None,
        standardised_measurement_unit=None,
    )

def get_measurement_from_db(seic_code, nrg_bal_code, country_code, year, db_cursor) -> Optional[Measurement]:
    db_cursor.execute(
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
    records = db_cursor.fetchall()
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
        measurent_unit=record[db_ob_map["measurement_unit"]],
        standardised_measurement_value=record[db_ob_map["standardised_measurement_value"]],
        standardised_measurement_unit=record[db_ob_map["standardised_measurement_unit"]],
    )
