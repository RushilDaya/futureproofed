# HOW TO RUN
## install
requires python 3.8+
```
pip install -e .
```
## Run
The functionality of the package is divided into a number of python files which can be executed. They should all be run from the root folder of this package
### 1. initialise database
Once installed a sqlite database with required schemas needs to be initialised in the package root folder. to do this run
```
python application/db_init.py
```
running the initialisation subsequent times can be used to reset the database
### 2. ingest measurements
After initialising a database, the source dataset (located in ./source_data) can be ingested into the database by running
```
python application/ingest_to_db.py
```
### 3. crud api
A Flask API can be started by running the following command
```
python application/crud_api.py
```
the created api will be available at http://127.0.0.1:5000


### 4. visualisation
Run the following command to generate graphs in plotly. The visualisation script should automatically open a browser window with the visualisations
```
python application/visualise.py
```
## Data Model 

## Flask API usage

## Next Steps
- Enrich dataset with external data to fill gaps
- speed up data ingestion step. Currently ingesting rows sequentially -> this can be optimized
- add additional CRUD enpoints for non-measurement tables + less constrained GETS for measurements
- data integrity/validity checks in the API
- additional visualisations 

