import csv
from typing import List, Any
import sqlite3
from click import progressbar

from application import DB_NAME, MEASUREMENT_DATA_CSV, STANDARD_UNIT
from application.data_models.object_models import create_measurement_object_from_csv_row, Measurement


# generator to supply data values (simulate a streaming data source)
# this approach is used to avoid loading the entire file into memory
# and is simple to understand
# However, it is slow - not a big issue for this use case but can be improved


def data_generator(source_file) -> List:
    with open(source_file, "r") as infile:
        csv_reader = csv.reader(infile)
        next(csv_reader)  # skip header
        for row in csv_reader:
            yield row


def transform_fill_standardised_values(measurement: Measurement) -> Measurement:
    if measurement.measurent_unit == STANDARD_UNIT:
        measurement.standardised_measurement_value = measurement.measurement_value
        measurement.standardised_measurement_unit = measurement.measurent_unit
    return measurement


def transform_merge_with_existing_measurement(measurement: Measurement, db_cursor) -> Measurement:
    # a single loaded measurement may not consistute a complete measurement
    # as in the raw file a measurement can be split into multiple rows based on the unit

    db_cursor.execute(
        """
              SELECT * FROM measurement where
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
    records = db_cursor.fetchall()
    if len(records) > 1:
        raise ValueError("More than one record found for the same measurement")
    if len(records) == 0:
        return measurement

    existing_measurement = records[0]  # refactor this  to be more clear

    # the merge logic here  should be re-looked at -> think there are some
    #  edge cases that are not covered

    if measurement.standardised_measurement_unit is None:
        # take the existing standard measurement
        measurement.standardised_measurement_value = existing_measurement[6]
        measurement.standardised_measurement_unit = existing_measurement[7]

    if measurement.measurent_unit == STANDARD_UNIT:
        # if the the measurement we are currently loading has the standard unit
        # it means that it is possibly not the original measurement
        if existing_measurement[5] != STANDARD_UNIT:
            measurement.measurement_value = existing_measurement[4]
            measurement.measurent_unit = existing_measurement[5]

    return measurement


def load_record(measurement: Measurement, db_cursor: Any, db_conn: Any) -> bool:
    # load the record into the database
    db_cursor.execute(
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

    db_cursor.execute(
        """
              INSERT INTO measurement (seic_code, nrg_bal_code, country_code, year, measurement_value, measurement_unit, standardised_measurement_value, standardised_measurement_unit)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              """,
        (
            measurement.seic_code,
            measurement.nrg_bal_code,
            measurement.country_code,
            measurement.year,
            measurement.measurement_value,
            measurement.measurent_unit,
            measurement.standardised_measurement_value,
            measurement.standardised_measurement_unit,
        ),
    )
    db_conn.commit()
    return True


def etl_row(raw_measurement: List[str], db_cursor: Any, db_conn: Any) -> bool:
    measurement = create_measurement_object_from_csv_row(raw_measurement)
    measurement = transform_fill_standardised_values(measurement)
    measurement = transform_merge_with_existing_measurement(measurement, db_cursor)
    load_record(measurement, db_cursor, db_conn)


if __name__ == "__main__":
    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()

    # ingesting one row at a time is not a very fast approach
    # however, it is simple to understand the transformations being applied
    # and sufficient for this use case -> can be optimized if needed

    # hard coding the length of the data source file here: purely for
    # understanding the progress - not the most elegant solution

    with progressbar(
        data_generator(MEASUREMENT_DATA_CSV), label="ingesting rows", length=27899
    ) as bar:
        for record in bar:
            etl_row(record, db_cursor, db_conn)
