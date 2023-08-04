# this script initializes / recreates a sqlite database
# and populates it with the initial metadata
import json
from application import (
    db_instance,
    COUNTRY_DATA_JSON,
    ENERGY_BALANCE_DATA_JSON,
    SEIC_DATA_JSON,
    UNIT_DATA_JSON,
    YEAR_DATA_JSON,
)
import application.data_models.database_models as dbm

# drop the tables allowing for a clean resetting of the database

tables_to_create = {
    "country": {
        "model": dbm.country,
        "data": json.load(open(COUNTRY_DATA_JSON))
    },
    "energy_balance": {
        "model": dbm.energy_balance,
        "data": json.load(open(ENERGY_BALANCE_DATA_JSON))
    },
    "seic": {
        "model": dbm.seic, 
        "data": json.load(open(SEIC_DATA_JSON))
    },
    "year": {
        "model": dbm.year, 
        "data": json.load(open(YEAR_DATA_JSON))
    },
    "unit": {
        "model": dbm.unit,
        "data": json.load(open(UNIT_DATA_JSON))
    },
    "measurement": {
        "model": dbm.measurement
    }
}

# clean reset of the database - easier to manage in this case
for table_name in tables_to_create.keys():
    db_instance.db_cursor.execute(
        f""" DROP TABLE IF EXISTS {table_name} """
    )
db_instance.db_conn.commit()
print("Dropped Existing Tables")

# create/ recreate the tables based on the defined models
for table_name, config in tables_to_create.items():
    db_instance.db_cursor.execute(config["model"])
db_instance.db_conn.commit()
print("Recreated Tables Successfully")


# populate all tables besides measurement table
for country_code, description in tables_to_create["country"]["data"].items():
    db_instance.db_conn.execute(
        """
              INSERT INTO country (country_code, description)
              VALUES (?, ?)
              """,
        (country_code, description),
    )
db_instance.db_conn.commit()

for nrg_bal_code, description in tables_to_create["energy_balance"]["data"].items():
    db_instance.db_conn.execute(
        """
              INSERT INTO energy_balance (nrg_bal_code, description)
              VALUES (?, ?)
              """,
        (nrg_bal_code, description),
    )
db_instance.db_conn.commit()

for seic_code, description in tables_to_create["seic"]["data"].items():
    db_instance.db_cursor.execute(
        """
              INSERT INTO seic (seic_code, description)
              VALUES (?, ?)
              """,
        (seic_code, description),
    )
db_instance.db_conn.commit()

for key, year in tables_to_create["year"]["data"].items():
    db_instance.db_cursor.execute(
        """
              INSERT INTO year (year)
              VALUES (?)
              """,
        (year,),
    )
db_instance.db_conn.commit()

for unit_code, description in tables_to_create["unit"]["data"].items():
    db_instance.db_cursor.execute(
        """
              INSERT INTO unit (unit_code, description)
              VALUES (?, ?)
              """,
        (unit_code, description),
    )
db_instance.db_conn.commit()

print("Initialised Tables Successfully")
