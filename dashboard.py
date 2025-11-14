import json

import streamlit as st
import datetime
import plotly.graph_objects as go
import pandas as pd
import hops
from dotenv import load_dotenv

load_dotenv()  # API key to Hopsworks


@st.cache_data
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load air quality records and predictions from Hopsworks"""
    project = hops.Project(name="ostergotland_air_quality")
    air_quality_fg, forecast_fg = project.get_feature_groups(
        [("air_quality", 2), ("air_quality_forecasts", 3)]
    )
    # Only load historical data for the last 14 days
    last_14_days_filter = air_quality_fg.date >= (
        datetime.date.today() - datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    next_14_days_filter = forecast_fg.date <= (
        datetime.date.today() + datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    aq_df = air_quality_fg.filter(last_14_days_filter).read().sort_values(by="date")
    forecast_df = (
        forecast_fg.filter(last_14_days_filter and next_14_days_filter)
        .read()
        .sort_values(by="date")
    )
    return (
        aq_df,
        forecast_df,
    )


def add_forecast_diff(df_forecast: pd.DataFrame) -> pd.DataFrame:
    """Add column "forecast_days_before" to indicate
    how many days before the reading the forecast was made"""
    df_forecast["forecast_days_before"] = (
        df_forecast["forecast_on"] - df_forecast["date"]
    ).map(lambda x: abs(x.days))
    df_forecast["forecast_days_before"] = df_forecast["forecast_days_before"]
    return df_forecast


def create_plot(
    place: dict,
    df_air_quality: pd.DataFrame,
    df_forecast: pd.DataFrame,
    forecast_days: tuple[int] = None,
) -> None:
    """Create and display air quality visualization in Streamlit"""
    if forecast_days is None:
        forecast_days = (1, 4, 9)
    fig = go.Figure()
    # Add historical data
    fig.add_trace(
        go.Scatter(
            x=df_air_quality["date"],
            y=df_air_quality["pm25"],
            mode="lines",
            name="PM2.5",
        )
    )
    # Add forecast data
    for days in forecast_days:
        forecast_days_in_advance = df_forecast[
            df_forecast["forecast_days_before"] == days
        ]
        fig.add_trace(
            go.Scatter(
                x=forecast_days_in_advance["date"],
                y=forecast_days_in_advance["predicted_pm25"],
                mode="lines",
                name=f"PM2.5 forecast ({days} days before)",
                line=dict(dash="dash"),
            )
        )

    fig.update_layout(
        title=f"Air Quality in {place['city']} on ({place['street']})",
        xaxis_title="Date",
        yaxis_title="PM2.5 Level",
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")


with open("places.json") as f:
    places = json.load(f)


st.title("Östergotland Air Quality Dashboard")
data_load_state = st.text("Loading data...")
historical_data, forecast_data = load_data()
forecast_data = add_forecast_diff(forecast_data)
data_load_state.text("")

for place in places.values():
    selected_place = place["id"]
    hist_for_place = historical_data[historical_data["id"] == selected_place]
    forecast_for_place = forecast_data[forecast_data["id"] == selected_place]
    create_plot(place, hist_for_place, forecast_for_place)
