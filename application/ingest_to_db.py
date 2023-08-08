import csv
import pandas as pd
from typing import List
from click import progressbar

from application import DatabaseObject, MEASUREMENT_DATA_CSV, db_instance
from application.data_models.object_models import (
    create_measurement_object_from_csv_row,
    load_record,
    Measurement
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


def etl_row(raw_measurement: List[str], db: DatabaseObject) -> bool:
    measurement = create_measurement_object_from_csv_row(raw_measurement)
    measurement = fill_standardised_values(measurement)
    measurement = merge_with_existing_measurement(measurement, db)
    load_record(measurement, db)

def load_from_csv(source_file_name:str) -> pd.DataFrame:
    df = pd.read_csv(source_file_name)
    return df

def keep_columns_batch(df: pd.DataFrame,keep_columns: List[str]) -> pd.DataFrame:
    df = df[keep_columns]
    return df

def rename_columns(df: pd.DataFrame, name_mapping: dict) -> pd.DataFrame:
    df = df.rename(columns=name_mapping)
    return df

def add_standardised_value_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['standardised_measuremen_value'] = None
    df['standardised_measurement_unit'] = None
    return df 

def fill_standardised_values_batch(df: pd.DataFrame, unit:str) -> pd.DataFrame:
    # set the standardised value and unit from the measurement value and unit
    # if the measurement unit is equal to the unit passed in
    # otherwise set to None
    df.loc[df['measurement_unit'] == unit, 'standardised_measurement_value'] = df['measurement_value']
    df.loc[df['measurement_unit'] == unit, 'standardised_measurement_unit'] = df['measurement_unit']
    return df

def merge_rows_batch(df: pd.DataFrame, key: List[str], standard_unit:str) -> pd.DataFrame:

    def transform_group(input: pd.DataFrame) -> pd.DataFrame:
        # if there are multiple rows then set the measurement value and measurement unit to None
        # for the row which has a measurement unit equal to the standard unit
        if len(input) > 1:
            input.loc[input['measurement_unit'] == standard_unit, 'measurement_value'] = None
            input.loc[input['measurement_unit'] == standard_unit, 'measurement_unit'] = None
        return input
    
    df = df.groupby(key).apply(
        lambda x: transform_group(x)
    ).reset_index(drop=True)
    df = df.groupby(key).agg({
        'measurement_unit' : 'first',
        'measurement_value' : 'first',
        'standardised_measurement_unit' : 'first',
        'standardised_measurement_value' : 'first',
    }).reset_index()
    return df

def load_records_batch(df: pd.DataFrame, db: DatabaseObject) -> None:
    for _, row in df.iterrows():
        #create measurement from row
        measurement = Measurement(
            seic_code=row['seic_code'],
            nrg_bal_code=row['nrg_bal_code'],
            country_code=row['country_code'],
            year=row['year'],
            measurement_value=row['measurement_value'],
            measurement_unit=row['measurement_unit'],
            standardised_measurement_value=row['standardised_measurement_value'],
            standardised_measurement_unit=row['standardised_measurement_unit']
        )
        load_record(measurement, db_instance, commit=False)
    db_instance.db_conn.commit()

if __name__ == "__main__":

    # ingesting one row at a time is not a very fast approach
    # however, it is simple to understand the transformations being applied
    # and sufficient for this use case -> can be optimized if needed

    # hard coding the length of the data source file here: purely for
    # understanding the progress - not the most elegant solution

    name_mapping = {
        'nrg_bal':'nrg_bal_code',
        'siec': 'seic_code',
        'unit': 'measurement_unit',
        'geo':'country_code',
        'TIME_PERIOD':'year',
        'OBS_VALUE':'measurement_value'    
    }

    keep_columns = list(name_mapping.keys())

    df = load_from_csv(MEASUREMENT_DATA_CSV)
    print(df.shape)
    df = keep_columns_batch(df,keep_columns)
    print(df.shape)
    df = rename_columns(df, name_mapping)
    print(df.shape)
    df = add_standardised_value_columns(df)
    print(df.shape)
    df = fill_standardised_values_batch(df, unit='TJ')
    print(df.shape)
    df = merge_rows_batch(df, key=['nrg_bal_code','seic_code','country_code','year'], standard_unit='TJ')
    print(df.shape)
    load_records_batch(df, db_instance)