import hopsworks


def get_feature_groups():
    project = hopsworks.login(engine="python", project="ostergotland_air_quality")
    fs = project.get_feature_store()
    air_quality_fg = fs.get_feature_group(
        name="air_quality",
        version=2,
    )
    weather_fg = fs.get_feature_group(
        name="weather",
        version=2,
    )
    return air_quality_fg, weather_fg, fs
