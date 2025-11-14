import json

import streamlit as st
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
        [("air_quality", 2), ("air_quality_forecasts", 1)]
    )

    return air_quality_fg.read().sort_values(by="date"), forecast_fg.read().sort_values(
        by="date"
    )


def create_plot(df_air_quality: pd.DataFrame, df_forecast: pd.DataFrame):
    """Create and display air quality visualization in Streamlit"""
    fig = go.Figure()

    # Add historical data
    fig.add_trace(
        go.Scatter(
            x=df_air_quality["date"],
            y=df_air_quality["pm25"],
            mode="lines",
            name="Historical PM2.5",
        )
    )

    # Add forecast data
    fig.add_trace(
        go.Scatter(
            x=df_forecast["date"],
            y=df_forecast["predicted_pm25"],
            mode="lines",
            name="Forecast PM2.5",
            line=dict(dash="dash"),
        )
    )

    fig.update_layout(
        title="Air Quality: Historical and Forecast",
        xaxis_title="Date",
        yaxis_title="PM2.5 Level",
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)


with open("places.json") as f:
    places = json.load(f)


st.title("Ostergotland Air Quality Dashboard")
selected_place = "A77446"
data_load_state = st.text("Loading data...")
historical_data, forecast_data = load_data()
data_load_state.text("Done! (using st.cache_data)")

hist_for_place, forecast_for_place = (
    historical_data[historical_data["id"] == selected_place],
    forecast_data[forecast_data["id"] == selected_place],
)
create_plot(hist_for_place, forecast_for_place)
st.write(
    f"Displaying air quality data for place ID: {selected_place} ({places[selected_place]['city']}, {places[selected_place]['street']})"
)
