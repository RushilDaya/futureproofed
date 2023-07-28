import csv
from typing import List, Any
import sqlite3
from click import progressbar

from application import DB_NAME, MEASUREMENT_DATA_CSV
from application.data_models.object_models import (
    create_measurement_object_from_csv_row,
    load_record
)
from application.etl_utils.transformers import (
    fill_standardised_values,
    merge_with_existing_measurement,
)

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


def etl_row(raw_measurement: List[str], db_cursor: Any, db_conn: Any) -> bool:
    measurement = create_measurement_object_from_csv_row(raw_measurement)
    measurement = fill_standardised_values(measurement)
    measurement = merge_with_existing_measurement(measurement, db_cursor)
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
