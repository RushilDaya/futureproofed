import pandas as pd
from typing import List

from application import DatabaseObject, MEASUREMENT_DATA_CSV, db_instance
from application.data_models.object_models import load_record, Measurement

# define all the configuration used in the ingestion
# of the csv file into the database
CONFIGURATION = {
    "source_file_name": MEASUREMENT_DATA_CSV,
    "name_mapping": {
        "nrg_bal": "nrg_bal_code",
        "siec": "seic_code",
        "unit": "measurement_unit",
        "geo": "country_code",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "measurement_value",
    },
    "group_key": ["seic_code", "nrg_bal_code", "country_code", "year"],
    "standard_unit": "TJ",
}


def load_from_csv(source_file_name: str) -> pd.DataFrame:
    df = pd.read_csv(source_file_name)
    return df


def transform_keep_columns(df: pd.DataFrame, keep_columns: List[str]) -> pd.DataFrame:
    df = df[keep_columns]
    return df


def transform_rename_columns(df: pd.DataFrame, name_mapping: dict) -> pd.DataFrame:
    df = df.rename(columns=name_mapping)
    return df


def transform_add_standardised_value_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["standardised_measuremen_value"] = None
    df["standardised_measurement_unit"] = None
    return df


def transform_fill_standardised_values(df: pd.DataFrame, unit: str) -> pd.DataFrame:
    # set the standardised value and unit from the measurement value and unit
    # if the measurement unit is equal to the unit passed in
    # otherwise set to None
    df.loc[df["measurement_unit"] == unit, "standardised_measurement_value"] = df[
        "measurement_value"
    ]
    df.loc[df["measurement_unit"] == unit, "standardised_measurement_unit"] = df["measurement_unit"]
    return df


def transform_set_measurements_to_null(
    df: pd.DataFrame, key: List[str], standard_unit: str
) -> pd.DataFrame:
    def _transform_group(input: pd.DataFrame) -> pd.DataFrame:
        # if there are multiple rows then set the measurement value and measurement unit to None
        # for the row which has a measurement unit equal to the standard unit
        if len(input) > 1:
            input.loc[input["measurement_unit"] == standard_unit, "measurement_value"] = None
            input.loc[input["measurement_unit"] == standard_unit, "measurement_unit"] = None
        return input

    df = df.groupby(key).apply(lambda x: _transform_group(x)).reset_index(drop=True)
    return df


def transform_merge_rows(df: pd.DataFrame, key: List[str]) -> pd.DataFrame:
    df = (
        df.groupby(key)
        .agg(
            {
                "measurement_unit": "first",
                "measurement_value": "first",
                "standardised_measurement_unit": "first",
                "standardised_measurement_value": "first",
            }
        )
        .reset_index()
    )
    return df


def load_records_batch(df: pd.DataFrame, db: DatabaseObject) -> None:
    for _, row in df.iterrows():
        # create measurement from row
        measurement = Measurement(
            seic_code=row["seic_code"],
            nrg_bal_code=row["nrg_bal_code"],
            country_code=row["country_code"],
            year=row["year"],
            measurement_value=row["measurement_value"],
            measurement_unit=row["measurement_unit"],
            standardised_measurement_value=row["standardised_measurement_value"],
            standardised_measurement_unit=row["standardised_measurement_unit"],
        )
        load_record(measurement, db, commit=False)
    db.db_conn.commit()


if __name__ == "__main__":
    keep_columns = list(CONFIGURATION["name_mapping"].keys())

    df = load_from_csv(MEASUREMENT_DATA_CSV)

    df = transform_keep_columns(df, keep_columns)
    df = transform_rename_columns(df, CONFIGURATION["name_mapping"])
    df = transform_add_standardised_value_columns(df)
    df = transform_fill_standardised_values(df, unit=CONFIGURATION["standard_unit"])
    df = transform_set_measurements_to_null(
        df, key=CONFIGURATION["group_key"], standard_unit=CONFIGURATION["standard_unit"]
    )
    df = transform_merge_rows(df, key=CONFIGURATION["group_key"])

    load_records_batch(df, db_instance)
