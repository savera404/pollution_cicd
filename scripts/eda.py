import pandas as pd
import hopsworks
from datetime import datetime

# Load the data
df = pd.read_csv("data/pollution_data.csv")

# Example preprocessing
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-5)  # Avoid division by zero

print("✅ Data cleaned and processed.")

# Connect to Hopsworks
project = hopsworks.login()
fs = project.get_feature_store()

# Create or get the feature group
pollution_fg = fs.get_or_create_feature_group(
    name="pollution_features",
    version=1,
    primary_key=["city"],
    description="Processed pollution data with extra features",
    online_enabled=False
)

# Insert data
pollution_fg.insert(df)
print("✅ Data uploaded to Hopsworks Feature Store successfully!")
