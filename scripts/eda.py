import pandas as pd
import hopsworks
from datetime import datetime
import os

# ✅ Get the Hopsworks API key from environment (set in GitHub Secrets)
api_key = os.getenv("HOPSWORKS_API_KEY")

# ✅ Login non-interactively (no terminal input)
project = hopsworks.login(api_key_value=api_key, project="pollution_cicd", hostname="c.app.hopsworks.ai")

fs = project.get_feature_store()

# 🔹 Load latest data
df = pd.read_csv("data/pollution_data.csv")

# 🔹 Process features
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-5)

print("✅ Data cleaned and processed.")

# 🔹 Create or get feature group
pollution_fg = fs.get_or_create_feature_group(
    name="pollution_features",
    version=1,
    primary_key=["city"],
    description="Processed pollution data with extra features",
    online_enabled=False
)

# 🔹 Insert into feature store
pollution_fg.insert(df)
print("✅ Data uploaded to Hopsworks Feature Store successfully!")
