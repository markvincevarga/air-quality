# %%
import datetime
from pathlib import Path

import requests
import pandas as pd
import os
import openmeteo_requests
import requests_cache
from retry_requests import retry
import hopsworks

from dotenv import load_dotenv

load_dotenv()

# %%
sensors = {}
for file in Path("data/air-quality").glob("*.csv"):
    sensor_id = file.stem
    print(f"Processing {sensor_id}")
    aqicn_url = f"https://api.waqi.info/feed/@{sensor_id}/?token={os.environ['AQICN_ORG_API_TOKEN']}"
    sensor_data = requests.get(aqicn_url, timeout=10).json()["data"]["city"]
    sensors[sensor_id] = sensor_data  # to fetch weather data later

    aq_df = pd.read_csv(file)
    aq_df.dropna(inplace=True)
    aq_df["name"] = sensor_data["name"]
    aq_df["url"] = f"https://api.waqi.info/feed/{sensor_id}"

sensors
# %%

# %%

project = hopsworks.login(engine="python")
fs = project.get_feature_store()

# %%
import great_expectations as ge

aq_expectation_suite = ge.core.ExpectationSuite(
    expectation_suite_name="aq_expectation_suite"
)

aq_expectation_suite.add_expectation(
    ge.core.ExpectationConfiguration(
        expectation_type="expect_column_min_to_be_between",
        kwargs={
            "column": "pm25",
            "min_value": -0.1,
            "max_value": 500.0,
            "strict_min": True,
        },
    )
)

# %%
air_quality_fg = fs.get_or_create_feature_group(
    name="air_quality",
    description="Air Quality characteristics of each day",
    version=1,
    primary_key=["country", "city", "street"],
    event_time="date",
    expectation_suite=aq_expectation_suite,
)
air_quality_fg.insert(aq_df)
air_quality_fg.update_feature_description("date", "Date of measurement of air quality")
air_quality_fg.update_feature_description(
    "pm25",
    "Particles less than 2.5 micrometers in diameter (fine particles) pose health risk",
)
# air_quality_fg.update_feature_description("country", "Country where the air quality was measured (sometimes a city in acqcn.org)")
# air_quality_fg.update_feature_description("city", "City where the air quality was measured")
# air_quality_fg.update_feature_description("street", "Street in the city where the air quality was measured")

# %%


def get_historical_weather(start_date, end_date, latitude, longitude):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_mean",
            "precipitation_sum",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)
    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    daily = response.Daily()
    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left",
        ),
        "temperature_2m_mean": daily.Variables(0).ValuesAsNumpy(),
        "precipitation_sum": daily.Variables(1).ValuesAsNumpy(),
        "wind_speed_10m_max": daily.Variables(2).ValuesAsNumpy(),
        "wind_direction_10m_dominant": daily.Variables(3).ValuesAsNumpy(),
    }
    daily_dataframe = pd.DataFrame(data=daily_data)
    daily_dataframe = daily_dataframe.dropna()
    return daily_dataframe


# %%
weather_fg = fs.get_or_create_feature_group(
    name="weather",
    description="Weather characteristics of each day",
    version=1,
    primary_key=["city"],
    event_time="date",
    # expectation_suite=
)

earliest_aq_date = pd.Series.min(aq_df["date"])
earliest_aq_date = earliest_aq_date.strftime("%Y-%m-%d")
sensor_id = "9999"
lat, long = aq_df[sensor_id]["city"]["geo"]
weather_df = get_historical_weather(
    earliest_aq_date, str(datetime.date.today()), lat, long
)
weather_fg.insert(weather_df, wait=True)
