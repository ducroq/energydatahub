"""
Backfill the historical-API feeds of a scheduled run that never published.

When a daily run fails, its timestamped files are never written. For most feeds
the next run's window overlaps and nothing is lost. Six feeds come back with
ONE day of data, so a missed run leaves a one-day hole in `data/` history:

    ned_production      collect(today, tomorrow)    -> hole on the run date
                        (the window spans two days, but NED serves nothing
                        past today: the 261003 file carries 10-03 only)
    grid_imbalance      collect(yesterday, today)   -> hole on run date - 1
    cross_border_flows  collect(yesterday, today)
    generation_mix      collect(yesterday, today)
    gas_flows           collect(yesterday, today)
    air_quality_buurt   collect(yesterday, today)

All six sources serve history (memory/project_data_backfill_gaps.md), so this
re-runs those collectors with the windows the missed run WOULD have used and
writes `<yymmdd>_235959_<feed>.json` files for the run date.

What it deliberately does NOT do:
  - Forecast feeds (Open-Meteo) are not backfilled. Their past forecasts are
    not served, and the neighbouring runs' horizons cover the gap anyway.
  - market_proxies (`collect(today, today)`) is not backfilled: market_history
    carries the running series, and the daily close is only partially
    recoverable (memory/project_data_backfill_gaps.md).
  - The current copies (`data/<feed>.json`), `docs/`, the shape sidecar and the
    quality report are untouched. A backfill must never become "the latest".
  - Prices: the previous run's day-ahead file already carries the run date.

The files are gitignored like every timestamped data file — CI commits them
with `git add -f` — so a backfill reaches consumers only once you run
`git add -f data/<yymmdd>_235959_*.json` and push.

The 235959 stamp is a sort key, not a collection time: Augur's consolidate
merges files in filename order (later overwrites earlier), so the backfill
sorts after the previous day's run and before the next day's run, which it
must. The envelope's own metadata carries the real collection time.

Usage:
    venv/bin/python scripts/backfill_missed_run.py --run-date 2026-10-04 --dry-run
    venv/bin/python scripts/backfill_missed_run.py --run-date 2026-10-04
"""

import argparse
import asyncio
import base64
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import (  # noqa: E402
    EntsoeFlowsCollector,
    EntsoeGenerationCollector,
    EntsogFlowsCollector,
    LuchtmeetnetCollector,
    NedCollector,
    TennetCollector,
)
from collectors.base import RetryConfig  # noqa: E402
from data_fetcher import assemble_buurt_air_envelope  # noqa: E402
from utils.data_quality import _count_data_points  # noqa: E402
from utils.helpers import load_secrets, load_settings, save_data_file  # noqa: E402
from utils.secure_data_handler import SecureDataHandler  # noqa: E402
from utils.timezone_helpers import get_timezone_and_country  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'data')

# Mirrors data_fetcher.main(). Keep in step with it — this list is duplicated
# here because the orchestrator builds it inline.
BUURT_LOCATIONS = [
    {"name": "Elsweide_Arnhem_NL",  "lat": 51.98955, "lon": 5.95470},
    {"name": "Elderveld_Arnhem_NL", "lat": 51.96069, "lon": 5.86010},
]


def build_collectors(secrets):
    """The six one-day collectors, configured exactly as data_fetcher.main() does."""
    entsoe_key = secrets.get('api_keys', 'entsoe')
    return {
        'ned': NedCollector(
            api_key=secrets.get('api_keys', 'ned'),
            energy_types=['solar', 'wind_onshore', 'wind_offshore'],
            include_forecast=True,
            include_actual=True,
            granularity='15min',
        ),
        'tennet': TennetCollector(api_key=secrets.get('api_keys', 'tennet')),
        'flows': EntsoeFlowsCollector(
            api_key=entsoe_key,
            retry_config=RetryConfig(max_attempts=5, initial_delay=30.0,
                                     max_delay=120.0, exponential_base=1.5),
        ),
        'genmix': EntsoeGenerationCollector(
            api_key=entsoe_key,
            country_codes=['NL', 'DE_LU', 'BE'],
            generation_types=[
                'nuclear', 'fossil_gas', 'fossil_hard_coal',
                'wind_onshore', 'wind_offshore', 'solar',
                'hydro_run_of_river', 'hydro_reservoir',
            ],
            include_forecast=False,
            include_actual=True,
        ),
        'entsog': EntsogFlowsCollector(country_code='NL'),
        'aq': [LuchtmeetnetCollector(latitude=loc['lat'], longitude=loc['lon'])
               for loc in BUURT_LOCATIONS],
    }


async def collect(run_date, timezone, secrets, keys):
    """Run the collectors in ``keys`` with the windows the missed run would have used."""
    today = datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone)
    tomorrow = today + timedelta(days=2) - timedelta(seconds=1)
    yesterday = today - timedelta(days=1)
    c = build_collectors(secrets)
    windows = {
        'ned': (today, tomorrow),
        'tennet': (yesterday, today),
        'flows': (yesterday, today),
        'genmix': (yesterday, today),
        'entsog': (yesterday, today),
    }

    single = [k for k in keys if k in windows]
    coros = [c[k].collect(*windows[k]) for k in single]
    if 'aq' in keys:
        coros += [aq.collect(yesterday, today) for aq in c['aq']]
    results = await asyncio.gather(*coros, return_exceptions=True)

    out = {}
    for name, res in zip(single, results[:len(single)]):
        if isinstance(res, BaseException):
            logging.error(f"{name}: {res!r}")
        out[name] = None if isinstance(res, BaseException) or not res else res
    if 'aq' in keys:
        aq_results = [None if isinstance(r, BaseException) else r
                      for r in results[len(single):]]
        out['aq'] = assemble_buurt_air_envelope(BUURT_LOCATIONS, aq_results)
    return out


FEED_FILES = {
    'ned': 'ned_production',
    'tennet': 'grid_imbalance',
    'flows': 'cross_border_flows',
    'genmix': 'generation_mix',
    'entsog': 'gas_flows',
    'aq': 'air_quality_buurt',
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--run-date', required=True,
                        help='date of the run that failed to publish, YYYY-MM-DD')
    parser.add_argument('--dry-run', action='store_true',
                        help='collect and report, write nothing')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    run_date = datetime.strptime(args.run_date, '%Y-%m-%d').date()
    settings = load_settings(PROJECT_DIR, 'settings.ini')
    timezone, _ = get_timezone_and_country(
        float(settings.get('location', 'latitude')),
        float(settings.get('location', 'longitude')),
    )
    encrypt = bool(settings.getint('data', 'encryption'))
    secrets = load_secrets(PROJECT_DIR, 'secrets.ini')
    handler = SecureDataHandler(
        base64.b64decode(secrets.get('security_keys', 'encryption')),
        base64.b64decode(secrets.get('security_keys', 'hmac')),
    )

    prefix = run_date.strftime('%y%m%d') + '_235959'
    # Per feed, not per date: a feed that already has a file for the run date
    # (the run published it, or an earlier backfill did) is skipped, so a
    # partial backfill can be re-run for just what is still missing.
    day = run_date.strftime('%y%m%d') + '_'
    present = set(os.listdir(OUTPUT_DIR))
    todo = {}
    for key, feed in FEED_FILES.items():
        have = sorted(f for f in present if f.startswith(day) and f.endswith(f'_{feed}.json'))
        if have:
            logging.info(f"{feed}: already present ({have[0]}) — skipped")
        else:
            todo[key] = feed
    if not todo:
        logging.info(f"nothing to backfill for {run_date}")
        return 0

    results = asyncio.run(collect(run_date, timezone, secrets, list(todo)))
    failed = []
    for key, feed in todo.items():
        ds = results.get(key)
        path = os.path.join(OUTPUT_DIR, f"{prefix}_{feed}.json")
        # Count points, never test truthiness: an EnhancedDataSet is always
        # truthy, and a collector that trapped every sub-request returns an
        # envelope with NO points (gotcha log 2026-09-10). Written, it would
        # also block a re-run via the already-present skip above.
        points = _count_data_points(ds.data) if ds is not None else 0
        if not points:
            failed.append(feed)
            logging.warning(f"{feed}: no data points — not written")
            continue
        if args.dry_run:
            logging.info(f"{feed}: would write {os.path.relpath(path, PROJECT_DIR)} ({points} points)")
            continue
        save_data_file(data=ds, file_path=path, handler=handler, encrypt=encrypt)
        logging.info(f"{feed}: wrote {os.path.relpath(path, PROJECT_DIR)} ({points} points)")
    if failed:
        logging.warning(f"not backfilled: {failed}")
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
