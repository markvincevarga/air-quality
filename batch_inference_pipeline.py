# %%
import hops
import datetime
from xgboost import XGBRegressor
import json
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

retrieved_model = mr.get_model(name="air_quality_xgboost_model", version=7)

fv = retrieved_model.get_feature_view()
# Download the saved model artifacts to a local directory
saved_model_dir = retrieved_model.download()
# Loading the XGBoost regressor model and label encoder from the saved model directory
retrieved_xgboost_model = XGBRegressor()
retrieved_xgboost_model.load_model(saved_model_dir + "/model.json")
# Displaying the retrieved XGBoost regressor model
retrieved_xgboost_model


# %%
today = datetime.date.today().strftime("%Y-%m-%d")

# Get and clean feature view

fs = project.feature_store
weather_fg, lagged_air_quality_fg = project.get_feature_groups(
    [
        (
            "weather",
            2,
        ),
        (
            "air_quality_lagged",
            3,
        ),
    ]
)

selected_features = weather_fg.select_all().join(
    lagged_air_quality_fg.select_all(), on="id", prefix="lagged_aq_"
)

feature_view = project.feature_store.get_or_create_feature_view(
    name="weather_plus_lagged_air_quality_inf_fv",
    description="",
    version=3,
    inference_helper_columns=["id"],
    training_helper_columns=["id"],
    query=selected_features,
)
# %%
# Get batch weather and lagged air quality data for tomorrow onwards
tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
batch_data = feature_view.get_batch_data(start_time=tomorrow)
batch_data.drop(columns=["lagged_aq_date"], inplace=True)
batch_data.rename(columns={"lagged_aq_id": "id"}, inplace=True)
# Set lagged data from day after tomorrow to NaN
# Since we don't actually have that data, the join just adds the latest available
for lag_col in batch_data.columns:
    if lag_col.startswith("lagged_aq_"):
        batch_data.loc[batch_data["date"] > (datetime.date.today() + datetime.timedelta(days=1)), lag_col] = None
batch_data["predicted_pm25"] = None
batch_data["predicted_pm25"] = batch_data["predicted_pm25"].astype('float32')
batch_data.info()
# %%
for day in batch_data["date"].unique():  
    print("Processing day:", day)
    mask_day = batch_data["date"] == day
    predicted = retrieved_xgboost_model.predict(
        batch_data[mask_day].drop(columns=["date", "id", "predicted_pm25"]).rename(
            columns={
                col: "weather_" + col
                for col in batch_data[mask_day].columns
                if not col.startswith("lagged_aq_")
            },
        )
    )
    predicted = predicted.astype(float)
    batch_data.loc[mask_day,"predicted_pm25"] = predicted
    # Take the weather forecast for tomorrow
    if day == batch_data["date"].max():
        continue
    next_day = day + datetime.timedelta(days=1)
    mask_next = batch_data["date"] == next_day
    # pm25 lagged_1d for tomorrow = predicted_pm25 for today
    batch_data.loc[mask_next, "lagged_aq_pm25_lagged_1d"] = batch_data.loc[mask_day, "predicted_pm25"].values
    batch_data.loc[mask_next, "lagged_aq_pm25_lagged_2d"] = batch_data.loc[mask_day, "lagged_aq_pm25_lagged_1d"].values
    batch_data.loc[mask_next, "lagged_aq_pm25_lagged_3d"] = batch_data.loc[mask_day, "lagged_aq_pm25_lagged_2d"].values
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
