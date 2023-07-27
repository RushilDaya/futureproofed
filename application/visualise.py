from plotly.subplots import make_subplots
import plotly.express as px

import pandas as pd
import sqlite3
import json
from application import DB_NAME, STANDARD_UNIT, COUNTRY_DATA_JSON, ENERGY_BALANCE_DATA_JSON

# Connect to database
conn = sqlite3.connect(DB_NAME)
df = pd.read_sql_query("SELECT * FROM measurement", conn)
conn.close()

# remove measures for which we don't have standardised units available
# as we  cannot compare them
print(df.shape)

df = df[df.standardised_measurement_unit == STANDARD_UNIT]
print(df.shape)

# group by the year, nrg_bal_code and country_code then aggregate the standardised_measurement_value
# by summing it -> this removes the concept of siec codes ( we dont care about it in this plot)
df = df.groupby(["year", "nrg_bal_code", "country_code"])
df = df.agg({"standardised_measurement_value": "sum"})
df = df.reset_index()
print(df.shape)

# remove the already combined values (eu totals) as here
# we want to focus  on individual countries
df = df[df.country_code != "EU27_2020"]
print(df.shape)
df = df[df.country_code != "EA20"]
print(df.shape)

# use user friendly labels
countries = json.load(open(COUNTRY_DATA_JSON))
df["country_code"] = df["country_code"].map(countries)

energy_balance = json.load(open(ENERGY_BALANCE_DATA_JSON))
df["nrg_bal_code"] = df["nrg_bal_code"].map(energy_balance)

fig = px.bar(
    df,
    title="energy balance totals for EU countries",
    x="country_code",
    y="standardised_measurement_value",
    color="nrg_bal_code",
    barmode="stack",
    facet_row="year",
    facet_row_spacing=0.01,
    height=10000
)
fig.update_xaxes(showticklabels=True)
fig.update_yaxes(title=f"Total energy usage in {STANDARD_UNIT}")	
fig.show()
