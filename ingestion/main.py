import os
import requests
import json
from datetime import datetime

API_URL = "https://api.data.gov.in/resource/cef25fe2-9231-4128-8aec-2c948fedd43f"

RAW_DATA_DIR = os.path.join("storage", "raw_data")

def fetch_monthly_data(year, month):
    params = {
    'api-key': '579b464db66ec23bdd000001c41966aa877d48cd5fa657dfe64c66d5',
    'format': 'json',
    'limit': 100,
    'filters[year]': str(year),
    'filters[month]': str(month)
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def save_raw_data(data, year, month):
    dir_path = os.path.join(RAW_DATA_DIR, str(year), str(month).zfill(2))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, "data.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)  # <-- FIXED

    print(f"Saved to path: {file_path}")


def main():
    # Example: Ingest data for Mar 2024
    for year in [2024]:
        for month in range(3, 4):
            try:
                print(f"Fetching data for {year}-{month:02d}")
                data = fetch_monthly_data(year, month)
                save_raw_data(data, year, month)
            except Exception as e:
                print(f"Error for {year}-{month:02d}: {e}")

if __name__ == "__main__":
    main()
