# %%
import hops
import datetime
import pandas as pd
from xgboost import XGBRegressor
import json
import os

from dotenv import load_dotenv

load_dotenv()
# %%
# Load places
with open("places.json") as plf:
    places = json.load(plf)

places

# %%
project = hops.Project(name="ostergotland_air_quality")
mr = project.model_registry

retrieved_model = mr.get_model(
    name="air_quality_xgboost_model",
    version=2,
)

fv = retrieved_model.get_feature_view()
# Download the saved model artifacts to a local directory
saved_model_dir = retrieved_model.download()
# Loading the XGBoost regressor model and label encoder from the saved model directory
# retrieved_xgboost_model = joblib.load(saved_model_dir + "/xgboost_regressor.pkl")
retrieved_xgboost_model = XGBRegressor()
retrieved_xgboost_model.load_model(saved_model_dir + "/model.json")
# Displaying the retrieved XGBoost regressor model
retrieved_xgboost_model



# %%
today = datetime.date.today().strftime("%Y-%m-%d")

fs = project.feature_store
weather_fg = project.get_feature_groups(
    [
        (
            "weather",
            2,
        )
    ]
)[0]

# %%
batch_data = weather_fg.filter(weather_fg.date >= today).read()
batch_data['predicted_pm25'] = retrieved_xgboost_model.predict( batch_data.drop(columns=["date", "id"]).add_prefix("weather_"))
batch_data.head()

# %%
# Save forecasts to separate feature group
batch_data = batch_data[["date", "id", "predicted_pm25"]]
batch_data["forecast_on"] = datetime.date.today()

forecasts_fg = fs.get_or_create_feature_group(
    name="air_quality_forecasts",
    description="Forecasted Air Quality for performance measurements",
    version=1,
    primary_key=["id"],
    event_time="forecast_on",
)
forecasts_fg.insert(batch_data)
