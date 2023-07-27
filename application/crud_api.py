# crud api for the application allowing
# the user get post put and delete single measurements
# in the database

from flask import Flask, request
import sqlite3
from application import DB_NAME
app = Flask(__name__)

primary_key = ['seic_code', 'nrg_bal_code', 'country_code', 'year']

@app.route('/measurement/', methods=['GET'])
def getMeasurement():
    args = request.args.to_dict()
    assert set(args.keys()) == set(primary_key) # put in proper error handling
    # get the measurement from the database

    db_conn = sqlite3.connect(DB_NAME)
    db_cursor = db_conn.cursor()
    db_cursor.execute("""
              SELECT * FROM measurement where
                      seic_code = ? and
                      nrg_bal_code = ? and
                      country_code = ? and
                      year = ?
                      """,
                      (
                            args['seic_code'],
                            args['nrg_bal_code'],
                            args['country_code'],
                            args['year']
                      )
    )
    records = db_cursor.fetchall()

    response = {}
    if len(records) == 0:
        response['status'] = '404'
    if len(records) > 1:
        response['status'] = '500'
    if  len(records) == 1:
        response['status'] = '200'
        response['res'] = {
            'seic_code': records[0][0],
            'nrg_bal_code': records[0][1],
            'country_code': records[0][2],
            'year': records[0][3],
            'measurement_value': records[0][4],
            'measurement_unit': records[0][5],
            'standardised_measurement_value': records[0][6],
            'standardised_measurement_unit': records[0][7]
        }
    return response




    


    return 

@app.route('/measurement/', methods=['PUT'])
def updateMeasurement():
    return 'Hello, World!'

@app.route('/measurement/', methods=['POST'])
def postMeasurement():
    return 'Hello, World!'

@app.route('/measurement/', methods=['DELETE'])
def deleteMeasurement():
    return 'Hello, World!'


if __name__ == "__main__":
    app.run()
