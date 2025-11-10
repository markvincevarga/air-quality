# Taks 2. Schedule a daily feature pipeline notebook that downloads yesterday’s weather data and air quality data, and also the
# weather prediction for the next 7-10 days and update the Feature Groups in Hopsworks. Use GH Actions or Modal.

# %%
import os
import json

import hops
import pandas as pd
import requests
import weather
from dotenv import load_dotenv

load_dotenv()
# %%
# Load places
with open("places.json") as plf:
    places = json.load(plf)

places

# %%
# Get today's air quality for all places
aq_df = pd.DataFrame(columns=["id", "date", "pm25"])
for place in places:
    aqicn_url = (
        f"https://api.waqi.info/feed/{place}/?token={os.environ['AQICN_ORG_API_TOKEN']}"
    )
    resp = requests.get(aqicn_url)
    resp.raise_for_status()
    data = resp.json()["data"]

    new_row = pd.DataFrame(
        [
            {
                "id": place,
                "date": pd.to_datetime(data["time"]["iso"]).date(),
                "pm25": float(data["iaqi"]["pm25"]["v"]),
            }
        ]
    )
    aq_df = pd.concat([aq_df, new_row], ignore_index=True)

aq_df["pm25"] = aq_df["pm25"].astype("float32")

aq_df.head(10)

# %%
weather_df = weather.get_forecast(10, places)
weather_df.head(10)

# %%
# Retrieve feature groups
project = hops.Project(name="ostergotland_air_quality")
air_quality_fg, weather_fg = project.get_feature_groups(
    [("air_quality", 2), ("weather", 2)]
)
air_quality_fg.insert(aq_df)
weather_fg.insert(weather_df)

# %%
