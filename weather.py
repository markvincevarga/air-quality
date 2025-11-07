import pandas as pd
import requests_cache
from datetime import date
import openmeteo_requests
from retry_requests import retry

OPENMETEO_DAILY_VARIABLES = [
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
]


def get_forecast(forecast_days: int, places: dict[str, dict]) -> pd.DataFrame:
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": [place["latitude"] for place in places.values()],
        "longitude": [place["longitude"] for place in places.values()],
        "forecast_days": forecast_days,
        "daily": OPENMETEO_DAILY_VARIABLES,
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


def get_historical(aq_df: pd.DataFrame, places: dict[str, dict]) -> pd.DataFrame:
    """Get historical weather for all places and date ranges in the aq_df"""
    return get_historical_in_daterange(
        [
            aq_df[aq_df["id"] == place["id"]]["date"].min().strftime("%Y-%m-%d")
            for place in places.values()
        ],
        [
            aq_df[aq_df["id"] == place["id"]]["date"].max().strftime("%Y-%m-%d")
            for place in places.values()
        ],
        places,
    )


def get_historical_in_daterange(
    starts: date, ends: date, places: dict[str, dict]
) -> pd.DataFrame:
    """Get historical weather for all places in the places list

    The aq_df has the air quality readings for each place. Checks for each place
    what date range it has air quality data for, and requests weather data
    in that range.

    Arguments:
        starts: list of start date for each place (in dict key order)
        ends: list of end date for each place (in dict key order)
        places: dict of place id to place data (latitude, longitude)

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
        "start_date": starts,
        "end_date": ends,
        "daily": OPENMETEO_DAILY_VARIABLES,
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
