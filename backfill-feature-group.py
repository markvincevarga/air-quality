# %%
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
    "A65707": Place(
        street="Lilla Åby",
        city="Slaka",
        country="Sweden",
        id="A65707",
        latitude=58.364,
        longitude=15.55,
    ),
    "A58909": Place(
        street="Tröskaregatan",
        city="Slaka",
        country="Sweden",
        id="A58909",
        latitude=58.386,
        longitude=15.56,
    ),
    "A77446": Place(
        street="Ånestad",
        city="Johannelund",
        country="Sweden",
        id="A77446",
        latitude=58.392,
        longitude=15.656,
    ),
    "@13990": Place(
        street="Hamngatan 10",
        city="Linköping",
        country="Sweden",
        id="@13990",
        latitude=58.412392308267435,
        longitude=15.630079303463972,
    ),
    "A533086": Place(
        street="Björnsbacken",
        city="Berg",
        country="Sweden",
        id="A533086",
        latitude=58.49270341067,
        longitude=15.5322432518,
    ),
    "@13985": Place(
        street="Kungsgatan 32",
        city="Norrköping",
        country="Sweden",
        id="@13985",
        latitude=58.59151641212257,
        longitude=16.177874909141224,
    ),
    "@13986": Place(
        street="Trädgårdsgatan 21",
        city="Norrköping",
        country="Sweden",
        id="@13986",
        latitude=58.59203940483364,
        longitude=16.18933291215303,
    ),
    "A556792": Place(
        street="Enebymovägen",
        city="Norrköping",
        country="Sweden",
        id="A556792",
        latitude=58.6,
        longitude=16.154,
    ),
}

# Only run if geo data is missing
if len(list(p["latitude"] for p in places.values())) != len(places):
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
        df["date"] = df["date"].dt.date
    else:
        raise ValueError(f"Unknown place id format: {place['id']}")
    df["pm25"] = df["pm25"].astype("float32")
    df.drop(df.columns.difference(["date", "pm25"]), axis=1, inplace=True)
    df.dropna(inplace=True)
    df["id"] = place["id"]


aq_df = pd.DataFrame()
for place_id in places:
    file_path = Path(f"data/air-quality/{place_id}.csv")
    if not Path(f"data/air-quality/{place_id}.csv").is_file():
        raise FileNotFoundError(f"File data/air-quality/{place_id}.csv not found")

    print(f"Processing {place_id}")
    df = pd.read_csv(
        file_path, comment="#", skipinitialspace=True, parse_dates=["date"]
    )
    process_aq(df, places[place_id])
    aq_df = pd.concat([aq_df, df])

aq_df.head()

# %%

# aq_df.info()
# pd.set_option("display.max_columns", 7)
aq_df.count()

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
def get_historical_weather(aq_df: pd.DataFrame, places: dict[str, Place]) -> pd.DataFrame:
    """Get historical weather for all places in the places list

    The aq_df has the air quality readings for each place. Checks for each place
    what date range it has air quality data for, and requests weather data
    in that range.

    Returns:
        pd.DataFrame with columns [id, date, weather_records...]
        where id is the place id
        weather_records is all weather data points available from the api"""
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": [place["latitude"] for place in places.values()],
        "longitude": [place["longitude"] for place in places.values()],
        "start_date": [
            aq_df[aq_df["id"] == place["id"]]["date"].min().strftime("%Y-%m-%d")
            for place in places.values()
        ],
        "end_date": [
            aq_df[aq_df["id"] == place["id"]]["date"].max().strftime("%Y-%m-%d")
            for place in places.values()
        ],
        "daily": [
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant",
            "precipitation_sum",
            "precipitation_hours",
            "rain_sum",
            "snowfall_sum",
            "et0_fao_evapotranspiration",
            "shortwave_radiation_sum",
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "sunset",
            "sunrise",
            "daylight_duration",
            "sunshine_duration",
            "temperature_2m_mean",
            "apparent_temperature_mean",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)
    weather_df = pd.DataFrame()
    
    daily_vars = params["daily"]
    for response, place_id in zip(responses, places.keys()):
        daily = response.Daily()
        daily_data = {
            "id": place_id,
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left",
            ),
        }
        
        for i, var_name in enumerate(daily_vars):
            if var_name in ["sunset", "sunrise"]:
                daily_data[var_name] = daily.Variables(i).ValuesInt64AsNumpy()
            else:
                daily_data[var_name] = daily.Variables(i).ValuesAsNumpy()
        weather_df = pd.concat([weather_df, pd.DataFrame(data=daily_data)])
    weather_df.dropna(inplace=True)
    weather_df["date"] = weather_df["date"].dt.date
    return weather_df

weather_df = get_historical_weather(aq_df, places)
weather_df.head()

# %%
weather_fg = fs.get_or_create_feature_group(
    name="weather",
    description="Weather characteristics of each day",
    version=2,
    primary_key=["id"],
    event_time="date",
    # expectation_suite=weather_expectation_suite.to_json_dict(),
)
weather_fg.insert(weather_df, wait=True)
# %%
