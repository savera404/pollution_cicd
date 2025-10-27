import pandas as pd
import hopsworks
from datetime import datetime
import os

# Get API key from environment
api_key = os.getenv("HOPSWORKS_API_KEY")

if not api_key:
    raise ValueError("❌ HOPSWORKS_API_KEY not found in environment variables!")

print("✅ API key loaded successfully")

# ✅ Non-interactive login for CI/CD - Fix the URL (remove duplicate https://)
project = hopsworks.login(
    project="pollution_cicd",
    api_key_value=api_key,
    host="c.app.hopsworks.ai"  # Fixed: removed https:// prefix
)

fs = project.get_feature_store()

# Load the data
df = pd.read_csv("data/pollution_data.csv")

print(f"✅ Loaded {len(df)} rows from pollution_data.csv")

# Example preprocessing
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-5)  # Avoid division by zero

print("✅ Data cleaned and processed.")

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
