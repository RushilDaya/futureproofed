from application.data_models.object_models import Measurement, get_measurement_from_db
from application import DatabaseObject
from application import STANDARD_UNIT


def fill_standardised_values(measurement: Measurement) -> Measurement:
    if measurement.measurement_unit == STANDARD_UNIT:
        measurement.standardised_measurement_value = measurement.measurement_value
        measurement.standardised_measurement_unit = measurement.measurement_unit
    return measurement


def merge_with_existing_measurement(measurement: Measurement, db: DatabaseObject) -> Measurement:
    # a single loaded measurement may not consistute a complete measurement
    # as in the raw file a measurement can be split into multiple rows based on the unit

    existing_record = get_measurement_from_db(
        measurement.seic_code,
        measurement.nrg_bal_code,
        measurement.country_code,
        measurement.year,
        db,
    )

    if existing_record is None:
        # nothing to merge
        return measurement

    if measurement.standardised_measurement_unit is None:
        # the incoming record doesn't have a standard unit
        # we try to take it form the existing record
        measurement.standardised_measurement_value = existing_record.standardised_measurement_value
        measurement.standardised_measurement_unit = existing_record.standardised_measurement_unit

    if measurement.measurement_unit == STANDARD_UNIT:
        # if the the measurement we are currently loading has the standard unit
        # it means that it is possibly not the original measurement
        if existing_record.measurement_unit != STANDARD_UNIT:
            measurement.measurement_value = existing_record.measurement_value
            measurement.measurement_unit = existing_record.measurement_unit

    return measurement
