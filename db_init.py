# this script initializes / recreates a sqlite database
# and populates it with the initial data

import sqlite3

# drop the tables allowing for a clean resetting of the database
conn = sqlite3.connect('datastore.db') 
c = conn.cursor()

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

# CREATE TABLES

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

# POPULATE TABLES
# ideally shift this to a separate script but for now it's fine

# populate energy_balance table