# %%
import datetime
import os
import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from pathlib import Path
from retry_requests import retry
from typing import TypedDict
from dotenv import load_dotenv

import hopsworks

load_dotenv()

# %%
class Place(TypedDict):
    street: str
    city: str
    country: str
    id: str
    latitude: float | None
    longitude: float | None


places = {
    "A65707": Place(street="Lilla Åby", city="Slaka", country="Sweden", id="A65707"),
    "A58909": Place(street="Tröskaregatan", city="Slaka", country="Sweden", id="A58909"),
    "A77446": Place(street="Ånestad", city="Johannelund", country="Sweden", id="A77446"),
    "@13990": Place(
        street="Hamngatan 10", city="Linköping", country="Sweden", id="@13990"
    ),
    "A533086": Place(street="Björnsbacken", city="Berg", country="Sweden", id="A533086"),
    "@13985": Place(
        street="Kungsgatan 32", city="Norrköping", country="Sweden", id="@13985"
    ),
    "@13986": Place(
        street="Trädgårdsgatan 21", city="Norrköping", country="Sweden", id="@13986"
    ),
    "A556792": Place(
        street="Enebymovägen", city="Norrköping", country="Sweden", id="A556792"
    ),
}

for place_id in places:
    # Get lat,long from live feed data for sensor
    aqicn_url = f"https://api.waqi.info/feed/{place_id}/?token={os.environ['AQICN_ORG_API_TOKEN']}"
    resp = requests.get(aqicn_url, timeout=10)
    resp.raise_for_status()
    sensor_data = resp.json()["data"]["city"]
    latitude, longitude = sensor_data["geo"]
    places[place_id]["latitude"] = latitude
    places[place_id]["longitude"]= longitude

places

# %%
def process_aq(df: pd.DataFrame, place: Place):
    """
    Process air quality dataframe depending on the type (A or @).
    
    The parameter will be modified in place to:
        pd.DataFrame: Processed dataframe with columns [id, date, pm25]
    """
    if place["id"].startswith("@"):
        pass
    elif place["id"].startswith("A"):
        df.rename(columns={"median": "pm25"}, inplace=True)
    else:
        raise ValueError(f"Unknown place id format: {place['id']}")
    df["pm25"] = df["pm25"].astype("float32")
    df.drop(df.columns.difference(["date", "pm25"]), axis=1, inplace=True)    
    df.dropna(inplace=True)
    df["id"] = place["id"]
    

aq_df = pd.DataFrame()
for place_id in places:
    file_path =Path(f"data/air-quality/{place_id}.csv")
    if not Path(f"data/air-quality/{place_id}.csv").is_file():
        raise FileNotFoundError(f"File data/air-quality/{place_id}.csv not found")
    
    print(f"Processing {place_id}")
    df = pd.read_csv(file_path, comment="#", skipinitialspace=True, parse_dates=["date"])
    process_aq(df, places[place_id])
    aq_df = pd.concat([aq_df, df])

aq_df.head()

# %%

aq_df.info()
pd.set_option("display.max_columns", 7)
aq_df.head()

# %%
project = hopsworks.login(engine="python")
fs = project.get_feature_store()
fs

# %%
air_quality_fg = fs.get_or_create_feature_group(
    name="air_quality",
    description="Air Quality characteristics of each day",
    version=1,
    primary_key=["id"],
    event_time="date",
    # expectation_suite=aq_expectation_suite,
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
def get_historical_weather(start_date, end_date, latitude, longitude, id):
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
        "id": id,
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


weather_fg = fs.get_or_create_feature_group(
    name="weather",
    description="Weather characteristics of each day",
    version=1,
    primary_key=["id"],
    event_time="date",
    # expectation_suite=weather_expectation_suite.to_json_dict(),
)

earliest_aq_date = pd.Series.min(aq_df["date"])
earliest_aq_date = earliest_aq_date.strftime("%Y-%m-%d")
# TODO: this is stupid, fix hard coded value.
sensor_id = "13986"
lat, long = places[sensor_id]["geo"]
weather_df = get_historical_weather(
    earliest_aq_date, str(datetime.date.today()), lat, long, sensor_id
)
weather_fg.insert(weather_df, wait=True)
