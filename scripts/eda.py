import pandas as pd
import hopsworks
from datetime import datetime
import os

# 🔹 Authenticate with Hopsworks using your API key from environment
api_key = os.getenv("HOPSWORKS_API_KEY")
project = hopsworks.login(api_key_value=api_key)
fs = project.get_feature_store()  # ✅ Only one call now

# 🔹 Load the latest pollution data
df = pd.read_csv("data/pollution_data.csv")

# 🔹 Preprocess / feature engineering
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-5)  # Avoid division by zero

print("✅ Data cleaned and processed.")

# 🔹 Create or get the feature group
pollution_fg = fs.get_or_create_feature_group(
    name="pollution_features",
    version=1,
    primary_key=["city"],
    description="Processed pollution data with extra features",
    online_enabled=False
)

# 🔹 Insert processed data into Hopsworks Feature Store
pollution_fg.insert(df)
print("✅ Data uploaded to Hopsworks Feature Store successfully!")
