# etl/extract.py
import requests
import json
import os
import csv
from datetime import datetime

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
BASE_DIR = "/mnt/d/projects/open-meteo_weather_etl_pipeline"
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
LOG_FILE = os.path.join(BASE_DIR, "logs", "etl_error_log.csv")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Ensure CSV log file header
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "raw_file", "clean_file", "status", "error_message"])

def log_etl_status(raw_file, clean_file, status, error_message=""):
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            raw_file or "",
            clean_file or "",
            status,
            error_message
        ])

# ---------------------------------------------------------------------
# WEATHER EXTRACTION LOGIC
# ---------------------------------------------------------------------
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
PUNJAB_CITIES = {
    "Lahore": (31.5497, 74.3436),
    "Faisalabad": (31.4180, 73.0791),
    "Rawalpindi": (33.6844, 73.0479),
    "Multan": (30.1989, 71.4687),
    "Sialkot": (32.5000, 74.5333),
    "Gujranwala": (32.1877, 74.1945),
    "Bahawalpur": (29.3957, 71.6833),
    "Rahim Yar Khan": (28.4202, 70.2952),
    "Dera Ghazi Khan": (30.0459, 70.6403),
    "Sahiwal": (30.6612, 73.1086),
    "Okara": (30.8081, 73.4458),
    "Kasur": (31.1164, 74.4500),
    "Sheikhupura": (31.7131, 73.9783),
    "Jhang": (31.2780, 72.3118),
    "Gujrat": (32.5731, 74.0780)
}
HOURLY_PARAMS = ["temperature_2m", "relative_humidity_2m", "weather_code", "wind_speed_10m"]
FORECAST_HOURS = 1
API_TIMEOUT = 15

def interpret_weather_code(code):
    code = int(code)
    if code == 0: return "Clear sky"
    elif code == 1: return "Mostly clear"
    elif code == 2: return "Partly cloudy"
    elif code == 3: return "Overcast"
    elif code in (45, 48): return "Fog or Rime fog"
    elif 51 <= code <= 55: return "Drizzle (Light to Dense)"
    elif 56 <= code <= 57: return "Freezing Drizzle"
    elif 61 <= code <= 65: return "Rain (Slight to Heavy)"
    elif 66 <= code <= 67: return "Freezing Rain"
    elif 80 <= code <= 82: return "Rain showers (Slight to Violent)"
    elif 85 <= code <= 86: return "Snow showers (Slight to Heavy)"
    elif code == 95: return "Thunderstorm (Slight or Moderate)"
    elif 96 <= code <= 99: return "Thunderstorm with hail"
    else: return f"Conditions Varies (WMO Code {code})"

def extract_data():
    """Fetch weather for Punjab cities and save a JSON file."""
    all_cities_data = []
    raw_file_path = None
    try:
        print(f"🌍 Starting Weather Fetch for {len(PUNJAB_CITIES)} Punjab cities...")

        for city, (lat, lon) in PUNJAB_CITIES.items():
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": ",".join(HOURLY_PARAMS),
                "temperature_unit": "celsius",
                "wind_speed_unit": "kmh",
                "forecast_hours": FORECAST_HOURS,
                "timezone": "Asia/Karachi"
            }
            try:
                resp = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=API_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()

                hourly = data.get("hourly", {})
                times = hourly.get("time", [])
                temps = hourly.get("temperature_2m", [])
                humids = hourly.get("relative_humidity_2m", [])
                codes = hourly.get("weather_code", [])
                winds = hourly.get("wind_speed_10m", [])

                city_data = {"city": city, "current_hourly_forecast_PKT": []}
                for i in range(len(times)):
                    city_data["current_hourly_forecast_PKT"].append({
                        "time_PKT": times[i],
                        "temperature_c": temps[i],
                        "humidity_percent": humids[i],
                        "conditions": interpret_weather_code(codes[i]),
                        "wind_speed_kmh": winds[i]
                    })
                all_cities_data.append(city_data)
                print(f"  ✅ {city} OK")

            except Exception as e:
                print(f"  ❌ {city} failed: {e}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file_path = os.path.join(RAW_DIR, f"punjab_weather_raw_{timestamp}.json")
        with open(raw_file_path, "w", encoding="utf-8") as f:
            json.dump(all_cities_data, f, ensure_ascii=False, indent=4)

        log_etl_status(raw_file_path, "", "SUCCESS", "")
        print(f"✅ All data saved to {raw_file_path}")
        return raw_file_path

    except Exception as e:
        log_etl_status("", "", "FAILED", str(e))
        raise

if __name__ == "__main__":
    extract_data()
