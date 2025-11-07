# %%
import os
import pandas as pd
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

import hopsworks
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
project = hopsworks.login(engine="python", project="ostergotland_air_quality")
fs = project.get_feature_store()
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
