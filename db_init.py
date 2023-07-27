# this script initializes / recreates a sqlite database
# and populates it with the initial metadata

import sqlite3
import json

# drop the tables allowing for a clean resetting of the database
conn = sqlite3.connect('datastore.db') 
c = conn.cursor()

# turn this into a function
c.execute('''
          DROP TABLE IF EXISTS country
          ''')
c.execute('''
          DROP TABLE IF EXISTS seic
          ''')
c.execute('''
          DROP TABLE IF EXISTS energy_balance
          ''')
c.execute('''
          DROP TABLE IF EXISTS year
          ''')
c.execute('''
          DROP TABLE IF EXISTS unit
          ''')
c.execute('''
          DROP TABLE IF EXISTS measurement
          ''')
conn.commit()

print('Dropped Existing Tables')

# CREATE TABLES
# turn these into functions which load the schema from a file
c.execute('''
          CREATE TABLE country (
          country_code varchar(2) NOT NULL,
          description varchar(255) NOT NULL,
          PRIMARY KEY (country_code)
          )
          ''')
c.execute('''
          CREATE TABLE energy_balance (
          nrg_bal_code varchar(14) NOT NULL,
          description varchar(255) NOT NULL,
          PRIMARY KEY (nrg_bal_code)
          )
          ''')
c.execute('''
          CREATE TABLE seic (
          seic_code varchar(32) NOT NULL,
          description varchar(255) NOT NULL,
          PRIMARY KEY (seic_code)
          )
          ''')
c.execute('''
          CREATE TABLE year (
          year INTEGER  NOT NULL,
          PRIMARY KEY (year)
          )
          ''')
c.execute('''
          CREATE TABLE unit (
          unit_code varchar(32) NOT NULL,
          description varchar(255) NOT NULL,
          PRIMARY KEY (unit_code)
          )
          ''')
c.execute('''
          CREATE TABLE measurement (
          seic_code varchar(32) NOT NULL,
          nrg_bal_code varchar(14) NOT NULL,
          country_code varchar(2) NOT NULL,
          year INTEGER NOT NULL,
          measurement_value NUMERIC NOT NULL,
          measurement_unit varchar(32) NOT NULL,
          standardised_measurement_value NUMERIC,
          standardised_measurement_unit varchar(32),

          FOREIGN KEY (seic_code) REFERENCES seic(seic_code),
          FOREIGN KEY (nrg_bal_code) REFERENCES energy_balance(nrg_bal_code),
          FOREIGN KEY (country_code) REFERENCES country(country_code),
          FOREIGN KEY (year) REFERENCES year(year),
          FOREIGN KEY (measurement_unit) REFERENCES unit(unit_code),
          FOREIGN KEY (standardised_measurement_unit) REFERENCES unit(unit_code)

          PRIMARY KEY (seic_code, nrg_bal_code, country_code, year)
          )
          ''')
conn.commit()

print('Recreated Tables Successfully')

# POPULATE TABLES
# ideally shift this into a function

# populate country table
countries_source = json.load(open('source_data/country.json'))
for  country_code, description in countries_source.items():
    c.execute('''
              INSERT INTO country (country_code, description)
              VALUES (?, ?)
              ''', (country_code, description)
    )
conn.commit()

# populate energy_balance table
energy_balance_source = json.load(open('source_data/energy_balance.json'))
for  nrg_bal_code, description in energy_balance_source.items():
    c.execute('''
              INSERT INTO energy_balance (nrg_bal_code, description)
              VALUES (?, ?)
              ''', (nrg_bal_code, description)
    )
conn.commit()

# populate seic table
seic_source = json.load(open('source_data/seic.json'))
for  seic_code, description in seic_source.items():
    c.execute('''
              INSERT INTO seic (seic_code, description)
              VALUES (?, ?)
              ''', (seic_code, description)
    )
conn.commit()

# populate year table
years_source = json.load(open('source_data/year.json'))
for  year in years_source:
    c.execute('''
              INSERT INTO year (year)
              VALUES (?)
              ''', (year,)
    )
conn.commit()

# populate unit table
units_source = json.load(open('source_data/unit.json'))
for  unit_code, description in units_source.items():
    c.execute('''
              INSERT INTO unit (unit_code, description)
              VALUES (?, ?)
              ''', (unit_code, description)
    )
conn.commit()

print('Initialised Tables Successfully')