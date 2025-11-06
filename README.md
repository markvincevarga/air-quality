# Air Quality

## Installing Dependencies

The project is managed by uv, but `hopsworks` must be installed separately in the venv. Use the following commands:

```sh
uv sync
uv pip install "hopsworks[python]"
```

## Getting the backfill data

## Air Quality Data

Download the desired data to be backfilled and place it in the data/ directory.

1. Search for the sensor's name (Usually city, street, number) on [https://aqicn.org/historical/](https://aqicn.org/historical/)
2. Select the sensor from the list
3. Click "Download this data (CSV format)
4. Place the file in the `data/air-quality` directory
5. Rename the file to the station ID. This is a number, for example `13990`.
