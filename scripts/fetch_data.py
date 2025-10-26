#in this code I will be fetching data from openweathermap api, the data i will be collecting is pollution data
import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

# ✅ Load .env from root folder
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# ✅ API key from .env
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# ✅ Coordinates for major cities
CITY_COORDS = {
    "Karachi": {"lat": 24.8607, "lon": 67.0011},
    "Lahore": {"lat": 31.5820, "lon": 74.3297},
    "Islamabad": {"lat": 33.6844, "lon": 73.0479},
}

def fetch_air_quality(city):
    """Fetch air quality data for a given city from OpenWeather API."""
    coords = CITY_COORDS[city]
    url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={coords['lat']}&lon={coords['lon']}&appid={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ Failed for {city}: {response.status_code}, {response.text}")
        return None

    data = response.json()
    if "list" not in data:
        print(f"⚠️ No data in response for {city}: {data}")
        return None

    components = data["list"][0]["components"]
    aqi = data["list"][0]["main"]["aqi"]

    return {
        "city": city,
        "pm2_5": components.get("pm2_5"),
        "pm10": components.get("pm10"),
        "co": components.get("co"),
        "no2": components.get("no2"),
        "o3": components.get("o3"),
        "so2": components.get("so2"),
        "nh3": components.get("nh3"),
        "aqi": aqi,
        "timestamp": datetime.utcnow(),
    }

def main():
    """Fetch data and save to CSV file."""
    records = []
    for city in CITY_COORDS.keys():
        record = fetch_air_quality(city)
        if record:
            records.append(record)

    if not records:
        print("❌ No data fetched.")
        return

    df = pd.DataFrame(records)
    print(df)

    # ✅ Save or append to CSV
    os.makedirs("data", exist_ok=True)
    csv_path = os.path.join("data", "pollution_data.csv")

    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        updated_df.to_csv(csv_path, index=False)
        print("✅ Data appended to existing CSV file.")
    else:
        df.to_csv(csv_path, index=False)
        print("✅ New CSV file created and data saved.")

if __name__ == "__main__":
    main()
