"""
Ad-hoc range sampler for issue #28.

Scans `data/` for solar_forecast, solar_forecast_buurt, and load_forecast
files spanning the requested date window, decrypts each, walks every
leaf field, and prints observed min/max + count per field.

Used once to derive proposed FIELD_RANGES entries for #28 — the output
is the source of truth for the bounds eventually committed to
`utils/data_quality.py::SOLAR_FIELD_RANGES` / `LOAD_FIELD_RANGES`.

Usage:
    python scripts/sample_observed_ranges.py --since 260301
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.helpers import load_secrets
from utils.secure_data_handler import SecureDataHandler


def get_handler() -> SecureDataHandler:
    config = load_secrets(str(REPO_ROOT))
    enc = base64.b64decode(config.get("security_keys", "encryption"))
    hmac = base64.b64decode(config.get("security_keys", "hmac"))
    return SecureDataHandler(enc, hmac)


def file_date(name: str) -> str | None:
    parts = name.split("_", 2)
    if parts and parts[0].isdigit() and len(parts[0]) == 6:
        return parts[0]
    return None


def descend_to_leaves(obj, out: dict[str, list[float]]) -> None:
    """Walk arbitrary nested dict/list, recording scalar numerics by leaf key."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                out[k].append(float(v))
            else:
                descend_to_leaves(v, out)
    elif isinstance(obj, list):
        for v in obj:
            descend_to_leaves(v, out)


def extract_data_payload(raw: dict) -> dict:
    """
    Return the per-record payload portion of a schema-v2.x envelope.

    Without this, `descend_to_leaves` walks into `metadata` too and
    contaminates field stats with envelope-internal numerics
    (forecast_days, lat, lon, location_count, schema_version, etc.) —
    sonnet review MEDIUM. Only the `data` subtree contains the
    per-timestamp records we want to derive bounds from.

    For envelopes lacking the canonical `{metadata, data}` shape (older
    files, plaintext snapshots), fall through to the full object.
    """
    if isinstance(raw, dict) and 'metadata' in raw and 'data' in raw:
        return raw['data']
    return raw


def sample(feed_suffix: str, since: str, handler: SecureDataHandler) -> None:
    data_dir = REPO_ROOT / "data"
    matches = sorted(
        f for f in data_dir.iterdir()
        if f.is_file()
        and f.name.endswith(f"_{feed_suffix}.json")
        and (file_date(f.name) or "") >= since
    )
    print(f"\n=== {feed_suffix}  ({len(matches)} files since {since}) ===")
    if not matches:
        return

    field_values: dict[str, list[float]] = defaultdict(list)
    files_read = 0
    decrypt_fail = 0
    for f in matches:
        try:
            content = f.read_text()
        except Exception:
            continue
        try:
            raw = json.loads(content)
        except json.JSONDecodeError:
            try:
                raw = handler.decrypt_and_verify(content)
            except Exception:
                decrypt_fail += 1
                continue
        # Only walk the `data` payload — descending into `metadata` too
        # would record envelope-internal numerics (forecast_days, lat,
        # lon, location_count) and pollute the derived bounds.
        descend_to_leaves(extract_data_payload(raw), field_values)
        files_read += 1

    print(f"  files read: {files_read}, decrypt-fail: {decrypt_fail}")
    print(f"  {'field':<32} {'n':>8} {'min':>12} {'max':>12} {'mean':>12}")
    for field, vals in sorted(field_values.items()):
        if not vals or len(vals) < 50:  # noise filter — skip rarely-emitted leaves
            continue
        lo, hi = min(vals), max(vals)
        mean = sum(vals) / len(vals)
        print(f"  {field:<32} {len(vals):>8} {lo:>12.4f} {hi:>12.4f} {mean:>12.4f}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="260301", help="YYMMDD lower bound (inclusive)")
    p.add_argument(
        "--feeds",
        default="solar_forecast,solar_forecast_buurt,load_forecast",
        help="Comma-separated feed suffixes to sample",
    )
    args = p.parse_args()

    handler = get_handler()
    for feed in args.feeds.split(","):
        feed = feed.strip()
        if feed:
            sample(feed, args.since, handler)
    return 0


if __name__ == "__main__":
    sys.exit(main())
