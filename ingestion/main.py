import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.destinations import filesystem
import os
import argparse
from typing import Optional, List, Tuple
from datetime import datetime
from dotenv import load_dotenv
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
RAW_DATA_DIR = os.path.join("storage")
EARLIEST_YEAR = 2009  # earliest year available on API (as int)
EARLIEST_MONTH = 1    # earliest month available on API (as int)

def get_kcc_source(year: int, 
                  month: int, 
                  max_offset: Optional[int] = 50000 
                  ):
    """
    Create a parametrized KCC data source.
    
    Args:
        year: The year to fetch data for
        month: The month to fetch data for
        max_offset: Optional maximum offset for pagination
    
    Returns:
        A dlt source that can be used in a pipeline
    """
    # Configure authentication
    auth = APIKeyAuth(
        name="api-key", 
        api_key=API_KEY, 
        location="query"
    )
    
    # Set up paginator with optional max offset
    paginator_config = {
        "limit": 100,
        "total_path": None,
        "offset_param": 'offset',
        "limit_param": 'limit',
    }
    
    # Add maximum_offset if provided
    if max_offset:
        paginator_config["maximum_offset"] = max_offset
    
    paginator = OffsetPaginator(**paginator_config)
    
    # Resource name with year-month format
    resource_name = f"kcc_data_{year}_{month:02d}"
    
    # Configure the REST API source
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.data.gov.in/",
            "auth": auth,
            "paginator": paginator
        },
        
        "resources": [
            {
                "name": resource_name,
                "endpoint": {
                    "path": "resource/cef25fe2-9231-4128-8aec-2c948fedd43f",
                    "params": {
                        'format': 'json',
                        'filters[year]': str(year),
                        'filters[month]': str(month)
                    }
                }
            }
        ]
    }
    
    # Create and return the source
    return rest_api_source(config)

def get_last_month() -> Tuple[int, int]:
    """Get the year and month for last month."""
    now = datetime.now()
    
    # Calculate last month
    if now.month == 1:  # January
        return now.year - 1, 12  # December of previous year
    else:
        return now.year, now.month - 1

def get_month_year_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """Generates (year, month) tuples from start to end date inclusive."""
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
    parser.add_argument("--backfill", action="store_true", 
                        help="Run ingestion in backfill mode (loads all historical data)")
    parser.add_argument("--max-offset", type=int, default=60000,
                        help="Maximum pagination offset")
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="kcc",
        destination=filesystem(bucket_url=RAW_DATA_DIR, 
                               layout="{table_name}/{placeholder}.{ext}", 
                               extra_placeholders={"placeholder": "data"}),
        dataset_name="raw_data",
        #progress='log'
    )
    
    if args.backfill:
        # Backfill mode - load all data from earliest date to last month
        logger.info("Running in BACKFILL mode")
        
        # Get last month as end date for backfill
        end_year, end_month = get_last_month()
        
        # Generate all month-year combinations to process
        months_to_process = get_month_year_range(
            EARLIEST_YEAR, EARLIEST_MONTH, 
            end_year, end_month
        )
        
        logger.info(f"Will process {len(months_to_process)} month(s) from {EARLIEST_YEAR}-{EARLIEST_MONTH:02d} to {end_year}-{end_month:02d}")
        
        # Process all months
        for year, month in months_to_process:
            logger.info(f"Processing {year}-{month:02d}")
            source = get_kcc_source(year=year, month=month, max_offset=args.max_offset)
            load_info = pipeline.run(source, loader_file_format="parquet")
            logger.info(f"Loaded data for {year}-{month:02d}: {load_info}")
    else:
        # Normal mode - load only last month's data
        year, month = get_last_month()
        logger.info(f"Running for last month: {year}-{month:02d}")
        
        source = get_kcc_source(year=year, month=month, max_offset=args.max_offset)
        load_info = pipeline.run(source, loader_file_format="parquet")
        logger.info(f"Loaded data for {year}-{month:02d}: {load_info}")
    
if __name__ == "__main__":
    main()