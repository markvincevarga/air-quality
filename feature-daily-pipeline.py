# Grade ‘E’ tasks
# 1. Write a backfill feature pipeline that downloads historical weather data (ideally >1 year of data), loads a csv file with
# historical air quality data (downloaded from https://aqicn.org) and registers them as 2 Feature Groups with Hopsworks.
# 2. Schedule a daily feature pipeline notebook that downloads yesterday’s weather data and air quality data, and also the
# weather prediction for the next 7-10 days and update the Feature Groups in Hopsworks. Use GH Actions or Modal.
# 3. Write a training pipeline that (1) selects the features for use in a feature view, (2) reads training data with the Feature
# View, trains a regression or classifier model to predict air quality (pm25). Register the model with Hopsworks.
# 4. Write a batch inference pipeline that creates a dashboard. The program should download your model from Hopsworks
# and plot a dashboard that predicts the air quality for the next 7-10 days for your chosen air quality sensor.
# 5. Monitor the accuracy of your predictions by plotting a hindcast graph showing your predictions vs outcomes
# (measured air quality).
# Grade ‘C’ tasks
# 6. Update your Model by adding a new feature, lagged air quality for the previous 1 day, 2 days, and 3 days. Measure and
# explain the performance improvement or regression for these features.
# Grade ‘A’ tasks
# 7. Provide predictions for all air quality sensors in a Swedish city

# %%
import os

# import hopsworks
from dotenv import load_dotenv

load_dotenv()

import requests

CITY_KEY = "@13986"
AQICN_URL = (
    f"https://api.waqi.info/feed/{CITY_KEY}/?token={os.environ['AQICN_ORG_API_TOKEN']}"
)

resp = requests.get(AQICN_URL)
resp.json()

# %%
resp.json()["data"]["city"]

# project = hopsworks.login(engine="python")

# %%
