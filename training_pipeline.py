# %%
# Write a training pipeline that
# (1) selects the features for use in a feature view,
# (2) reads training data with the FeatureView, trains a regression or classifier model to predict air quality (pm25). Register the model with Hopsworks.

import os
import sys
import json
import hops
from datetime import datetime, timedelta
from plot import plot_air_quality_forecast
from xgboost import XGBRegressor
from xgboost import plot_importance
from sklearn.metrics import mean_squared_error, r2_score

from dotenv import load_dotenv

load_dotenv()


def is_interactive():
    return hasattr(sys, "ps1")


# %%
project = hops.Project(name="ostergotland_air_quality")
air_quality_fg, weather_fg, lagged_air_quality_fg = project.get_feature_groups(
    [("air_quality", 2), ("weather", 2), ("air_quality_lagged", 3)]
)
selected_features = (
    air_quality_fg.select(["id", "pm25", "date"])
    .join(weather_fg.select_all(), on="id", prefix="weather_")
    .join(lagged_air_quality_fg.select_all(), on="id", prefix="lagged_aq_")
)

feature_view = project.feature_store.get_or_create_feature_view(
    name="air_quality_fv",
    description="weather features with air quality as the target",
    version=6,
    inference_helper_columns=["id"],
    training_helper_columns=["id"],
    labels=["pm25"],
    query=selected_features,
)

# %%
test_start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")

X_train, X_test, y_train, y_test = feature_view.train_test_split(
    test_start=test_start, primary_key=True
)
X_train.head()

# %%
# Create features by removing unnecessary columns
columns_to_drop = [
    "date",
    "weather_date",
    "lagged_aq_date",
    "weather_id",
    "ostergotland_air_quality_air_quality_2_id",
    "lagged_aq_id",
]
X_features = X_train.drop(columns=columns_to_drop).rename(
    columns={
        col: col.replace("lagged_aq_", "")
        for col in X_train.columns
        if col.startswith("lagged_aq_")
    },
)
X_test_features = X_test.drop(columns=columns_to_drop).rename(
    columns={
        col: col.replace("lagged_aq_", "")
        for col in X_test.columns
        if col.startswith("lagged_aq_")
    },
)
print(y_train.head())
X_features.head()
X_features.info()

# %%
xgb_regressor = XGBRegressor()
xgb_regressor.fit(X_features, y_train)
y_pred = xgb_regressor.predict(X_test_features)
mse = mean_squared_error(y_test.iloc[:, 0], y_pred)
print("MSE:", mse)
r2 = r2_score(y_test.iloc[:, 0], y_pred)
print("R squared:", r2)
df = y_test
df["predicted_pm25"] = y_pred
df["date"] = X_test["date"]
df = df.sort_values(by=["date"])
df.head(5)

# %%
# Load places
with open("places.json") as plf:
    places = json.load(plf)

places

# %%
model_dir = "model"
os.makedirs("model", exist_ok=True)
img_dir = "model/images"
os.makedirs(img_dir, exist_ok=True)

df["id"] = X_test["weather_id"]
for place in places.values():
    plt = plot_air_quality_forecast(
        df,
        place,
        img_dir + f"/pm25_forecast_{place['city']}_{place['street']}.png",
        hindcast=True,
    )
    if is_interactive():
        plt.show()


# %%
#  Saving the XGBoost regressor object as a json file in the model directory
model_file = model_dir + "/model.json"

xgb_regressor.save_model(model_file)
res_dict = {
    "MSE": str(mse),
    "R squared": str(r2),
}
mr = project.model_registry
# Creating a Python model in the model registry named 'air_quality_xgboost_model'
aq_model = mr.python.create_model(
    name="air_quality_xgboost_model",
    metrics=res_dict,
    feature_view=feature_view,
    description="Air Quality (PM2.5) predictor",
)
# Saving the model artifacts to the 'air_quality_model' directory in the model registry
aq_model.save(model_file)

# %%
# Plotting feature importances using the plot_importance function from XGBoost
plot_importance(xgb_regressor)
feature_importance_path = img_dir + "/feature_importance.png"
plt.savefig(feature_importance_path)
if is_interactive():
    plt.show()
