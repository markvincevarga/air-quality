# %%
import os
import pandas as pd
import json
import requests
import datetime
from pathlib import Path
import helper
from dotenv import load_dotenv

import hops
import weather

load_dotenv()


# %%
with open("places.json") as plf:
    places = json.load(plf)

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
        places[place_id]["longitude"] = longitude

places


# %%
def process_aq(df: pd.DataFrame, place: dict[str, dict]) -> None:
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
# Save air quality data with lagged values
lagged_aq_df = aq_df
lagged_aq_df["date"] = pd.to_datetime(lagged_aq_df["date"]).dt.date
today = lagged_aq_df["date"].max()
# Add fake rows for tomorrow, since we already know the lagged data for tomorrow
fake_aq_tomorrow = lagged_aq_df[lagged_aq_df["date"] == today].copy()
fake_aq_tomorrow["date"] = fake_aq_tomorrow["date"] + datetime.timedelta(days=1)
# fake_aq_tomorrow["date"] = pd.to_datetime(fake_aq_tomorrow["date"])
fake_aq_tomorrow["pm25"] = None
lagged_aq_df = pd.concat([lagged_aq_df, fake_aq_tomorrow])
# Add lagged data
lagged_aq_df = helper.add_lagged_data(lagged_aq_df, "pm25", by_days=1)
lagged_aq_df = helper.add_lagged_data(lagged_aq_df, "pm25", by_days=2)
lagged_aq_df = helper.add_lagged_data(lagged_aq_df, "pm25", by_days=3)
lagged_aq_df.drop(columns=["pm25"], inplace=True)
lagged_aq_df.dropna(inplace=True)
lagged_aq_df.tail(15)
# %%
project = hops.Project(name="ostergotland_air_quality")
fs = project.feature_store
fs
# %%
air_quality_fg = fs.get_or_create_feature_group(
    name="air_quality",
    description="Air Quality characteristics of each day",
    version=2,
    primary_key=["id"],
    event_time="date",
)
air_quality_fg.insert(aq_df)
air_quality_fg.update_feature_description("date", "Date of measurement of air quality")
air_quality_fg.update_feature_description(
    "pm25",
    "Particles less than 2.5 micrometers in diameter (fine particles) pose health risk",
)

# %%
lagged_aq_fg = fs.get_or_create_feature_group(
    name="air_quality_lagged",
    description="Air Quality characteristics with lagged pm25 values",
    version=3,
    primary_key=["id"],
    event_time="date",
)
lagged_aq_fg.insert(lagged_aq_df)
# %%
weather_df = weather.get_historical(aq_df, places)
weather_df.head()

# %%
weather_fg = fs.get_or_create_feature_group(
    name="weather",
    description="Weather characteristics of each day",
    version=2,
    primary_key=["id"],
    event_time="date",
)
weather_fg.insert(weather_df, wait=True)
# %%
