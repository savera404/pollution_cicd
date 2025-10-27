import pandas as pd
import hopsworks
from datetime import datetime
import os

# Get API key from environment
api_key = os.getenv("HOPSWORKS_API_KEY")

if not api_key:
    raise ValueError("❌ HOPSWORKS_API_KEY not found in environment variables!")

print("✅ API key loaded successfully")

# ✅ Non-interactive login for CI/CD
project = hopsworks.login(
    project="pollution_cicd",
    api_key_value=api_key,
    host="c.app.hopsworks.ai"
)

fs = project.get_feature_store()

# Load the data
df = pd.read_csv("data/pollution_data.csv")
print(f"✅ Loaded {len(df)} rows from pollution_data.csv")
print(f"Columns: {df.columns.tolist()}")

# Preprocessing
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Create event_time as Unix timestamp (required for Hopsworks)
df["event_time"] = (df["timestamp"].astype(int) // 10**9).astype(int)

# Add engineered features
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-5)  # Avoid division by zero
df["total_pollutants"] = df["pm2_5"] + df["pm10"] + df["co"] + df["no2"] + df["o3"] + df["so2"] + df["nh3"]

# Drop the original timestamp string (keep event_time)
df = df.drop(columns=["timestamp"])

print("✅ Data cleaned and processed.")
print(f"Processed columns: {df.columns.tolist()}")
print(f"Shape: {df.shape}")
print(f"\nFirst row:\n{df.head(1).to_dict('records')}")

# Create or get the feature group
pollution_fg = fs.get_or_create_feature_group(
    name="pollution_features",
    version=1,
    primary_key=["city", "event_time"],
    event_time="event_time",
    description="Processed pollution data with extra features",
    online_enabled=False
)

print(f"\n✅ Feature group created/retrieved")

# Insert data with proper error handling
try:
    job = pollution_fg.insert(df, write_options={"wait_for_job": True})
    print(f"✅ Successfully inserted {len(df)} rows to Hopsworks Feature Store!")
    print(f"Job details: {job}")
except Exception as e:
    print(f"❌ Error inserting data: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# Verify data was inserted
try:
    print("\n🔍 Verifying insertion...")
    fg_data = pollution_fg.read()
    print(f"✅ Verification: Feature group now contains {len(fg_data)} rows")
    print(f"Latest entries:\n{fg_data.tail()}")
except Exception as e:
    print(f"⚠️ Could not verify data: {str(e)}")
