# etl/transform.py
import json
import csv
import os
from datetime import datetime
from etl.extract import log_etl_status, LOG_FILE

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
BASE_DIR = "/mnt/d/projects/open-meteo_weather_etl_pipeline"
DATA_DIR = os.path.join(BASE_DIR, "data")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

DEDUP_FIELDS = ['date', 'time_ampm']

def load_existing_keys(csv_path):
    keys = set()
    if os.path.exists(csv_path):
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                key = tuple(row.get(k) for k in DEDUP_FIELDS)
                if all(key): keys.add(key)
    return keys

def append_rows(csv_path, rows, headers):
    file_exists = os.path.exists(csv_path)
    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

def transform_data(raw_file):
    """Transforms JSON into cleaned per-city CSVs."""
    clean_dir = CLEAN_DIR
    try:
        with open(raw_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"🔄 Loaded raw data from {raw_file}")

        headers = ['city', 'date', 'time_ampm', 'temperature_c', 'humidity_percent', 'conditions', 'wind_speed_kmh']
        total = 0
        for city_forecast in data:
            city = city_forecast.get("city")
            rows = []
            csv_file = os.path.join(clean_dir, f"{city.replace(' ', '_').lower()}_weather.csv")
            existing = load_existing_keys(csv_file)

            for rec in city_forecast.get("current_hourly_forecast_PKT", []):
                try:
                    dt = datetime.strptime(rec["time_PKT"], "%Y-%m-%dT%H:%M")
                    date, time_ampm = dt.strftime("%Y-%m-%d"), dt.strftime("%I:%M %p")
                except Exception:
                    continue
                key = (date, time_ampm)
                if key in existing:
                    continue
                rows.append({
                    "city": city,
                    "date": date,
                    "time_ampm": time_ampm,
                    "temperature_c": rec.get("temperature_c"),
                    "humidity_percent": rec.get("humidity_percent"),
                    "conditions": rec.get("conditions"),
                    "wind_speed_kmh": rec.get("wind_speed_kmh")
                })

            if rows:
                append_rows(csv_file, rows, headers)
                total += len(rows)
                print(f"  ✅ {city}: {len(rows)} new rows appended.")

        print(f"✅ Transformation complete, {total} rows added.")
        log_etl_status(raw_file, clean_dir, "SUCCESS", "")
        return clean_dir

    except Exception as e:
        log_etl_status(raw_file, "", "FAILED", str(e))
        raise
