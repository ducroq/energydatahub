"""
Backfill missing ENTSO-E price data into existing energy_price_forecast files.

Scans all timestamped price files, identifies those missing 'entsoe' (NL) and/or
'entsoe_de' (DE) datasets, fetches historical prices from the ENTSO-E API,
and merges them back into the encrypted files.

Usage:
    python scripts/backfill_entsoe.py --dry-run     # Report only, no changes
    python scripts/backfill_entsoe.py               # Patch files
"""

import asyncio
import argparse
import base64
import glob
import logging
import os
import sys
from configparser import ConfigParser
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.entsoe import EntsoeCollector
from utils.helpers import load_data_file, save_data_file
from utils.secure_data_handler import SecureDataHandler

AMSTERDAM_TZ = ZoneInfo('Europe/Amsterdam')
RATE_LIMIT_DELAY = 3  # seconds between API calls


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('backfill_entsoe.log')
        ]
    )


def load_keys(project_dir: str) -> tuple:
    """Load encryption and API keys from secrets.ini."""
    config = ConfigParser()
    secrets_path = os.path.join(project_dir, 'secrets.ini')
    if not os.path.exists(secrets_path):
        raise FileNotFoundError(f"secrets.ini not found at {secrets_path}")
    config.read(secrets_path)

    encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
    hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
    entsoe_api_key = config.get('api_keys', 'entsoe')

    return encryption_key, hmac_key, entsoe_api_key


def parse_file_date(filename: str) -> datetime:
    """Extract date from filename like '251003_161652_energy_price_forecast.json'."""
    basename = os.path.basename(filename)
    date_str = basename[:6]  # yymmdd
    year = 2000 + int(date_str[:2])
    month = int(date_str[2:4])
    day = int(date_str[4:6])
    return datetime(year, month, day, tzinfo=AMSTERDAM_TZ)


def scan_degraded_files(data_dir: str, handler: SecureDataHandler) -> list:
    """Find all price files missing entsoe and/or entsoe_de datasets."""
    pattern = os.path.join(data_dir, '*_energy_price_forecast.json')
    files = sorted(glob.glob(pattern))

    degraded = []
    for filepath in files:
        # Skip the non-timestamped "current" copy
        basename = os.path.basename(filepath)
        if not basename[0].isdigit():
            continue

        try:
            data = load_data_file(filepath, handler=handler)
        except Exception as e:
            logging.warning(f"Could not read {basename}: {e}")
            continue

        missing = []
        if 'entsoe' not in data or data.get('entsoe') is None:
            missing.append('entsoe')
        if 'entsoe_de' not in data or data.get('entsoe_de') is None:
            missing.append('entsoe_de')

        if missing:
            file_date = parse_file_date(basename)
            degraded.append({
                'path': filepath,
                'basename': basename,
                'date': file_date,
                'missing': missing,
                'data': data,
            })

    return degraded


async def fetch_entsoe_prices(
    collector: EntsoeCollector,
    date: datetime,
    country_code: str
):
    """Fetch day-ahead prices for a single day."""
    start = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=2) - timedelta(seconds=1)
    try:
        dataset = await collector.collect(start, end, country_code=country_code)
        return dataset
    except Exception as e:
        logging.error(f"API error for {country_code} on {date.date()}: {e}")
        return None


async def backfill(dry_run: bool = False):
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_dir, 'data')
    docs_dir = os.path.join(project_dir, 'docs')

    encryption_key, hmac_key, entsoe_api_key = load_keys(project_dir)
    handler = SecureDataHandler(encryption_key, hmac_key)
    collector = EntsoeCollector(api_key=entsoe_api_key)

    country_map = {
        'entsoe': 'NL',
        'entsoe_de': 'DE_LU',
    }

    logging.info("Scanning for degraded price files...")
    degraded = scan_degraded_files(data_dir, handler)

    if not degraded:
        logging.info("No degraded files found. All price files have ENTSO-E data.")
        return

    # Deduplicate dates (multiple files per day possible)
    dates_missing = {}
    for entry in degraded:
        date_key = entry['date'].date()
        if date_key not in dates_missing:
            dates_missing[date_key] = set()
        dates_missing[date_key].update(entry['missing'])

    logging.info(f"Found {len(degraded)} degraded files across {len(dates_missing)} unique dates")
    for date_key in sorted(dates_missing):
        logging.info(f"  {date_key}: missing {', '.join(sorted(dates_missing[date_key]))}")

    if dry_run:
        logging.info("Dry run — no files will be modified.")
        return

    # Fetch missing data per unique date
    fetched = {}  # (date, dataset_key) -> EnhancedDataSet
    for date_key in sorted(dates_missing):
        for dataset_key in sorted(dates_missing[date_key]):
            country = country_map[dataset_key]
            date_dt = datetime(date_key.year, date_key.month, date_key.day, tzinfo=AMSTERDAM_TZ)

            logging.info(f"Fetching {dataset_key} ({country}) for {date_key}...")
            dataset = await fetch_entsoe_prices(collector, date_dt, country)

            if dataset is not None and len(dataset.data) > 0:
                fetched[(date_key, dataset_key)] = dataset
                logging.info(f"  Got {len(dataset.data)} data points")
            else:
                logging.warning(f"  No data available from ENTSO-E for {dataset_key} on {date_key}")

            await asyncio.sleep(RATE_LIMIT_DELAY)

    # Patch files
    patched_count = 0
    skipped_count = 0
    error_count = 0

    for entry in degraded:
        date_key = entry['date'].date()
        data = entry['data']
        any_patched = False

        for dataset_key in entry['missing']:
            result = fetched.get((date_key, dataset_key))
            if result is None:
                continue
            data[dataset_key] = result.to_dict()
            any_patched = True

        if not any_patched:
            skipped_count += 1
            continue

        try:
            save_data_file(data=data, file_path=entry['path'], handler=handler, encrypt=True)
            patched_count += 1
            logging.info(f"Patched: {entry['basename']}")

            # Mirror to docs/ if the file exists there
            docs_path = os.path.join(docs_dir, entry['basename'])
            if os.path.exists(docs_path):
                save_data_file(data=data, file_path=docs_path, handler=handler, encrypt=True)
                logging.info(f"  Also patched docs/{entry['basename']}")
        except Exception as e:
            error_count += 1
            logging.error(f"Failed to save {entry['basename']}: {e}")

    logging.info(f"\nBackfill complete:")
    logging.info(f"  Patched: {patched_count} files")
    logging.info(f"  Skipped (no API data): {skipped_count} files")
    logging.info(f"  Errors: {error_count} files")


def main():
    parser = argparse.ArgumentParser(description='Backfill missing ENTSO-E price data')
    parser.add_argument('--dry-run', action='store_true', help='Report degraded files without patching')
    args = parser.parse_args()

    setup_logging()
    asyncio.run(backfill(dry_run=args.dry_run))


if __name__ == '__main__':
    main()
