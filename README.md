# Air Quality

## Installing Dependencies

The project is managed by uv, but `hopsworks` must be installed separately in the venv. Use the following commands:

```sh
uv sync
uv pip install "hopsworks[python]"
```

## Getting the backfill data

Download the desired historical air quality data to be backfilled and place it in the data/ directory. Make sure to adjust the dictionary in the code of the places to have the desired ids.

1. Find the sensor's page ([example](https://aqicn.org/station/sweden/linkoping-hamngatan-10/))
2. Click PM2.5 "Download this data (CSV format)"
3. Place the file in the `data/air-quality` directory
4. Rename the file to the station ID as seen in the API endpoint. This is a string, for example `@13990`.

Finally, run the following: `uv run backfill-feature-group.py`
