# %%
# Write a training pipeline that
# (1) selects the features for use in a feature view,
# (2) reads training data with the FeatureView, trains a regression or classifier model to predict air quality (pm25). Register the model with Hopsworks.

import os
import json
import hops
from datetime import datetime
from plot import plot_air_quality_forecast
from xgboost import XGBRegressor
from xgboost import plot_importance
from sklearn.metrics import mean_squared_error, r2_score

# %%
project = hops.Project(name="ostergotland_air_quality")
air_quality_fg, weather_fg = project.get_feature_groups(
    [("air_quality", 2), ("weather", 2)]
)
selected_features = air_quality_fg.select(["pm25", "date"]).join(
    weather_fg.select_features(), on=["id"]
)

feature_view = project.feature_store.get_or_create_feature_view(
    name="air_quality_fv",
    description="weather features with air quality as the target",
    version=1,
    labels=["pm25"],
    query=selected_features,
)

# %%

test_start = datetime.today().strftime("%Y-%m-%d")

X_train, X_test, y_train, y_test = feature_view.train_test_split(test_start=test_start)
X_features = X_train.drop(columns=["date"])
X_test_features = X_test.drop(columns=["date"])
X_train
y_train

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
for place in places.values():
    plt = plot_air_quality_forecast(
        df,
        place,
        img_dir + f"/pm25_forecast_{place['city']}_{place['street']}.png",
        hindcast=True,
    )
    plt.show()


# %%
# TODO: This is just a copy/paste, have not tested with hops library yet
#  Saving the XGBoost regressor object as a json file in the model directory
# xgb_regressor.save_model(model_dir + "/model.json")
# res_dict = {
#     "MSE": str(mse),
#     "R squared": str(r2),
# }
# mr = project.get_model_registry()

# # Creating a Python model in the model registry named 'air_quality_xgboost_model'

# aq_model = mr.python.create_model(
#     name="air_quality_xgboost_model",
#     metrics=res_dict,
#     feature_view=feature_view,
#     description="Air Quality (PM2.5) predictor",
# )

# # Saving the model artifacts to the 'air_quality_model' directory in the model registry
# aq_model.save(model_dir)
