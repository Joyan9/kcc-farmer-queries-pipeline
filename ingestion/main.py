import os
import requests
from datetime import datetime
import argparse
from dotenv import load_dotenv
from typing import List, Tuple
import logging

# ----------------- Logging Setup -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------- Config & Constants -----------------
load_dotenv()
API_KEY = os.getenv("KCC_API_KEY")

API_URL = "https://api.data.gov.in/resource/cef25fe2-9231-4128-8aec-2c948fedd43f"
RAW_DATA_DIR = os.path.join("storage", "raw_data")
EARLIEST_YEAR = 2008  # earliest year available on API (as int)
EARLIEST_MONTH = 12   # earliest month available on API (as int)

# ----------------- Functions -----------------
def fetch_monthly_data(year: int, month: int) -> str:
    """Fetch CSV data for a specific year and month from the API."""
    if not API_KEY:
        logger.error("KCC_API_KEY not set in environment variables.")
        raise ValueError("KCC_API_KEY not set in environment variables.")
    params = {
        'api-key': API_KEY,
        'format': 'csv',
        'filters[year]': str(year),
        'filters[month]': str(month)
    }
    response = requests.get(API_URL, params=params)
    if response.status_code != 200:
        logger.error(f"API error for {year}-{month:02d}: {response.text}")
    response.raise_for_status()
    return response.text  # CSV data as string

def save_raw_data(data: str, year: int, month: int) -> None:
    """Save fetched data as a CSV file in the appropriate directory."""
    dir_path = os.path.join(RAW_DATA_DIR, str(year), str(month).zfill(2))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, "data.csv")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(data)
    logger.info(f"Saved to path: {file_path}")

def get_month_year_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """
    Generates (year, month) tuples from start to end date inclusive.
    """
    months = []
    year, month = start_year, start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="KCC Data Ingestion Pipeline")
    parser.add_argument("--start-year", type=int, help="Start Year (e.g., 2020)")
    parser.add_argument("--start-month", type=int, help="Start Month (1-12)")
    parser.add_argument("--end-year", type=int, help="End Year (e.g., 2024)")
    parser.add_argument("--end-month", type=int, help="End Month (1-12)")
    return parser.parse_args()

def main():
    args = parse_args()

    # Determine the default end date (last complete month)
    now = datetime.now()
    last_year = now.year if now.month > 1 else now.year - 1
    last_month = now.month - 1 if now.month > 1 else 12

    start_year = args.start_year if args.start_year else EARLIEST_YEAR
    start_month = args.start_month if args.start_month else EARLIEST_MONTH
    end_year = args.end_year if args.end_year else last_year
    end_month = args.end_month if args.end_month else last_month

    logger.info(f"Ingesting from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")

    for year, month in get_month_year_range(start_year, start_month, end_year, end_month):
        try:
            logger.info(f"Fetching data for {year}-{month:02d}")
            data = fetch_monthly_data(year, month)
            save_raw_data(data, year, month)
        except Exception as e:
            logger.error(f"Error for {year}-{month:02d}: {e}")

if __name__ == "__main__":
    main()
