# Air Quality

A student project for the [KTH](kth.se) course *ID2223/FID3020 HT25 Scalable Machine Learning and Deep Learning* created by **Númi Steinn Baldursson** and **Márk Vince Varga**, aka group Teki 🐢.

The goal of the project is to build a data ingest and machine learning pipeline to predict air quality in a county of Sweden, in our case in Östergotland.

- We monitor a number of sensors, listed in `places.json`. These are all the sensors in Östergotland with decent amount of historical data available at the time of the inception of the project.
- `backfill-feature-group.py` is used to backfill historical data for both air quality and weather
- `feature-daily-pipeline.py` is executed daily as a GitHub Action to grab daily data for air quality and weather forecast
- `training_pipeline.py` is ran on demand to train the model with all available data
- `batch_inference_pipeline.py` is executed daily as a GitHub Action to provide air quality predictions for all sensors
- `dashboard.py` produces historical, forecast and hindcast graphs for all monitored sensors, displayed on [air-quality-visundur.streamlit.app](https://air-quality-visundur.streamlit.app/)

## A note on performed steps

- We provide predictions for all sensors in our county of Östergotland
- We implement a hindcast graph to monitor prediction accuracy on the dashboard
- We add lagged air quality of the 1,2,3 previous days to our data, experiencing an improvement in accuracy seen on the following table:

|                            | R^2    | MSE    |
| ---                        | ---    | ---    |
| Without lagged air quality | -0.666 | 750.00 |
| With lagged air quality    | -0.438 | 349.01 |

This may mean that the previous few days' air quality is an important factor in predicting the next day's air quality. This is indeed what we experience charting feature importance.

![air quality model feature importance](https://github.com/user-attachments/assets/ea33a840-e6ce-423f-99bb-9388086f0f80)

## Set up environment variables

Run

```shell
cp .env.example .env
```

and use the comments to set up the environment variables.

## Installing Dependencies

The project is managed by uv. Use the following command to install dependencies locally:

```sh
uv sync
```

## Getting the backfill data

Download the desired historical air quality data to be backfilled and place it in the data/ directory. Make sure to adjust the dictionary in the code of the places to have the desired ids.

1. Find the sensor's page ([example](https://aqicn.org/station/sweden/linkoping-hamngatan-10/))
2. Click PM2.5 "Download this data (CSV format)"
3. Place the file in the `data/air-quality` directory
4. Rename the file to the station ID as seen in the API endpoint. This is a string, for example `@13990`.

Finally, run the following: `uv run backfill-feature-group.py`
