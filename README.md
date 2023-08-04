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
A fastapi API can be started by running the following command
```
 python3 -m uvicorn  main:app --reload
```
the created api will be available at http://127.0.0.1:8000
swagger documentation available at http://127.0.0.1:8000/docs
redoc documentations available at http://127.0.0.1:8000/redoc


### 4. visualisation
Run the following command to generate graphs in plotly. The visualisation script should automatically open a browser window with the visualisations
```
python application/visualise.py
```
## Data Model
An initial data exploration was done, the code/comments for this can be found in:
```
./exploration_modelling/data_exploration.py
```

Based on this exploration and documentation of the source data available the following model was decided on: 
```
./exploration_modelling/data_model_diagram.png
```

The model reflects the structure of the source dataset, however the following deviations exist:

- no modelling of the columns DATAFLOW,LAST UPDATE,freq or OBS_FLAG
    - these columns either have all rows as NULL or all rows contain the same value - these there is no meaningful information for this use case
- in the source csv multiple rows can exist for the same combination of nrg_bal,siec,geo and TIME_PERIOD. allowing multiple measurements with different units. when looking into the data it was seen that when these cases exist the measurements are actually equivelent / but in different units. thus in the data model we use nrg_bal, siec, geo and TIME_PERIOD equivalents as a PRIMARY KEY and introduce the concept of a standardised unit to measurements -> every measurement can be expressed  in at most its native unit and a standardised unit for comparision  purposes.
    - note: we cannot automatically convert between units in all cases as the transform between certain unit pairs seems non-trivial thus we need to store the standardised units as distinct values even though it seems like a duplication of data

## Flask API usage
when the fastapi server is running a single endpoint is made available namely:
```
http://127.0.0.1:8000/measurement  (METHODS:GET,PUT,POST,DELETE)
```
#### GET
example  request:
```
http://127.0.0.1:8000/measurement?country_code=AL&seic_code=E7000&nrg_bal_code=FC_OTH_HH_E&year=2010
```
example response:
```json
{
    "res": {
        "country_code": "AL",
        "measurement_unit": "GWH",
        "measurement_value": 2997.45,
        "nrg_bal_code": "FC_OTH_HH_E",
        "seic_code": "E7000",
        "standardised_measurement_unit": "TJ",
        "standardised_measurement_value": 10790.82,
        "year": 2010
    },
    "status": "200"
}

```
#### PUT
```
http://127.0.0.1:8000/measurement
```
with json body
```json
{
        "country_code": "AL",
        "measurement_unit": "GWH",
        "measurement_value": 29970.45,
        "nrg_bal_code": "FC_OTH_HH_E",
        "seic_code": "E7000",
        "standardised_measurement_unit": "TJ",
        "standardised_measurement_value": 107900.82,
        "year": 2010
}
```
#### POST
```
http://127.0.0.1:8000/measurement
```
with json body
```json
{
        "country_code": "EL",
        "measurement_unit": "GWH",
        "measurement_value": 29977.45,
        "nrg_bal_code": "FC_OTH_HH_E",
        "seic_code": "E7000",
        "standardised_measurement_unit": "TJ",
        "standardised_measurement_value": 1079000.82,
        "year": 2011
}
```
#### DELETE
example request
```
http://127.0.0.1:8000/measurement?country_code=AL&seic_code=E7000&nrg_bal_code=FC_OTH_HH_E&year=2010
```




## Next Steps
- Enrich dataset with external data to fill gaps
- speed up data ingestion step. Currently ingesting rows sequentially -> this can be optimized
- add additional CRUD enpoints for non-measurement tables + less constrained GETS for measurements
- data integrity/validity checks in the API (done)
- additional visualisations 

