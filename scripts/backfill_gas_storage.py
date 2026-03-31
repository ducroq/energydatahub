"""
Backfill broken gas storage timestamps in historical data files.

A bug in pd.concat(ignore_index=True) caused GIE storage data to lose its
gasDayStart index, resulting in integer keys ('0', '1', '2') instead of ISO
timestamps. The data values are intact; only the keys need reconstruction
using metadata start_time + daily offset.

Usage:
    python scripts/backfill_gas_storage.py --dry-run     # Report only, no changes
    python scripts/backfill_gas_storage.py               # Patch files
"""

import argparse
import base64
import glob
import logging
import os
import re
import shutil
import sys
from configparser import ConfigParser
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import load_data_file, save_data_file
from utils.secure_data_handler import SecureDataHandler


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('backfill_gas_storage.log')
        ]
    )


def load_keys(project_dir: str) -> tuple:
    """Load encryption keys from secrets.ini."""
    config = ConfigParser()
    secrets_path = os.path.join(project_dir, 'secrets.ini')
    if not os.path.exists(secrets_path):
        raise FileNotFoundError(f"secrets.ini not found at {secrets_path}")
    config.read(secrets_path)

    encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
    hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))

    return encryption_key, hmac_key


def has_integer_keys(data: dict) -> bool:
    """Check if the data dict uses integer-string keys ('0', '1', ...)."""
    keys = list(data.keys())
    return bool(keys) and all(re.match(r'^\d+$', str(k)) for k in keys)


def reconstruct_timestamps(data_dict: dict) -> dict:
    """
    Replace integer keys with ISO timestamps reconstructed from metadata.

    Each file covers a date range (metadata start_time to end_time), with
    one data point per day, ordered sequentially.
    """
    metadata = data_dict.get('metadata', {})
    start_str = metadata.get('start_time')
    if not start_str:
        raise ValueError("No start_time in metadata")

    start_time = datetime.fromisoformat(start_str)
    data = data_dict['data']

    new_data = {}
    for key in sorted(data.keys(), key=lambda k: int(k)):
        ts = start_time + timedelta(days=int(key))
        new_data[ts.isoformat()] = data[key]

    data_dict['data'] = new_data
    return data_dict


def validate_reconstruction(data_dict: dict) -> tuple:
    """
    Validate that reconstructed timestamps align with metadata end_time.

    Returns (is_valid, message).
    """
    metadata = data_dict.get('metadata', {})
    end_str = metadata.get('end_time')
    if not end_str:
        return True, "No end_time in metadata to validate against"

    end_time = datetime.fromisoformat(end_str)
    keys = sorted(data_dict['data'].keys())
    if not keys:
        return False, "No data points after reconstruction"

    last_ts = datetime.fromisoformat(keys[-1])
    diff = abs((end_time - last_ts).total_seconds()) / 3600

    if diff > 24:
        return False, (
            f"Last timestamp {keys[-1]} differs from end_time {end_str} "
            f"by {diff:.0f}h (expected <= 24h)"
        )

    return True, f"OK: {len(keys)} points, last={keys[-1]}"


def scan_degraded_files(data_dir: str, handler: SecureDataHandler) -> tuple:
    """Find gas storage files with integer keys. Returns (degraded, total)."""
    pattern = os.path.join(data_dir, '[0-9]*_gas_storage.json')
    files = sorted(glob.glob(pattern))

    degraded = []
    good = 0

    for filepath in files:
        basename = os.path.basename(filepath)
        try:
            data = load_data_file(filepath, handler=handler)
        except Exception as e:
            logging.warning(f"Could not read {basename}: {e}")
            continue

        file_data = data.get('data', data)
        if has_integer_keys(file_data):
            degraded.append({
                'path': filepath,
                'basename': basename,
                'data': data,
            })
        else:
            good += 1

    return degraded, good + len(degraded)


def backfill(dry_run: bool = False):
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_dir, 'data')
    docs_dir = os.path.join(project_dir, 'docs')

    encryption_key, hmac_key = load_keys(project_dir)
    handler = SecureDataHandler(encryption_key, hmac_key)

    logging.info("Scanning for gas storage files with broken timestamps...")
    degraded, total = scan_degraded_files(data_dir, handler)

    if not degraded:
        logging.info(f"No degraded files found ({total} files checked). All timestamps OK.")
        return

    logging.info(f"Found {len(degraded)} degraded files out of {total} total")
    for entry in degraded:
        meta = entry['data'].get('metadata', {})
        n_points = len(entry['data'].get('data', {}))
        logging.info(
            f"  {entry['basename']}: {n_points} points, "
            f"range {meta.get('start_time', '?')} to {meta.get('end_time', '?')}"
        )

    if dry_run:
        logging.info("Dry run -- no files will be modified.")
        return

    patched_count = 0
    error_count = 0
    latest_file = None

    for entry in degraded:
        basename = entry['basename']
        try:
            data = reconstruct_timestamps(entry['data'])

            is_valid, msg = validate_reconstruction(data)
            if not is_valid:
                logging.warning(f"Skipping {basename}: {msg}")
                error_count += 1
                continue

            save_data_file(data=data, file_path=entry['path'], handler=handler, encrypt=True)
            patched_count += 1
            logging.info(f"Patched: {basename} ({msg})")

            # Track latest file by filename date prefix
            if latest_file is None or basename > latest_file['basename']:
                latest_file = entry

        except Exception as e:
            error_count += 1
            logging.error(f"Failed to patch {basename}: {e}")

    # Update the "current" copies with the latest patched data
    if latest_file:
        current_data = os.path.join(data_dir, 'gas_storage.json')
        current_docs = os.path.join(docs_dir, 'gas_storage.json')

        try:
            shutil.copy(latest_file['path'], current_data)
            logging.info(f"Updated data/gas_storage.json from {latest_file['basename']}")
        except Exception as e:
            logging.error(f"Failed to update data/gas_storage.json: {e}")

        try:
            shutil.copy(latest_file['path'], current_docs)
            logging.info(f"Updated docs/gas_storage.json from {latest_file['basename']}")
        except Exception as e:
            logging.error(f"Failed to update docs/gas_storage.json: {e}")

    logging.info(f"\nBackfill complete:")
    logging.info(f"  Patched: {patched_count} files")
    logging.info(f"  Errors:  {error_count} files")


def main():
    parser = argparse.ArgumentParser(
        description='Backfill broken gas storage timestamps from metadata'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Report degraded files without patching'
    )
    args = parser.parse_args()

    setup_logging()
    backfill(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
