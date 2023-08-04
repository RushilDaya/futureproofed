DB_NAME = 'datastore.db' # name of the sqlite database file

COUNTRY_DATA_JSON = 'source_data/country.json'
ENERGY_BALANCE_DATA_JSON = 'source_data/energy_balance.json'
SEIC_DATA_JSON = 'source_data/seic.json'
UNIT_DATA_JSON = 'source_data/unit.json'
YEAR_DATA_JSON = 'source_data/year.json'
MEASUREMENT_DATA_CSV = 'source_data/nrg_d_hhq_linear.csv'

STANDARD_UNIT = 'TJ' # based on knowledge of the measurement data -> TJ is the standardised unit for comparison

import sqlite3

class DatabaseObject():
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.db_conn = sqlite3.connect(self.db_name, check_same_thread=False)
        self.db_cursor = self.db_conn.cursor()

db_instance = DatabaseObject(DB_NAME)