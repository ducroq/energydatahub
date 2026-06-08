"""
Ad-hoc probe for issue #25 root cause.

Queries the TenneT API across several past-date windows to identify the
publication-availability boundary. Output identifies the oldest window
that returns 422 and the most recent window that succeeds — the
difference is TenneT's actual publication lag, which determines whether
`data_fetcher.py` should request an older window (and how much older).

Usage:
    python scripts/probe_tennet_windows.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.helpers import load_secrets

try:
    from tenneteu import TenneTeuClient
except ImportError as e:
    print(f"tenneteu-py not installed: {e}", file=sys.stderr)
    sys.exit(2)


def _scrub(msg: str, secret: str) -> str:
    """
    Remove the API key from a message so it never lands in stdout.

    Defense for security review L2: if the tenneteu client ever embeds
    the API key in an exception's repr/str (some libraries include the
    full request URL with `?api_key=...`), the message would leak the
    secret. Replace literal occurrences with `[REDACTED]`.
    """
    if secret and secret in msg:
        return msg.replace(secret, "[REDACTED]")
    return msg


def _try_window(
    client: TenneTeuClient,
    label: str,
    start: datetime,
    end: datetime,
    api_key: str,
) -> None:
    """Run both API calls for a window and report status + row counts."""
    print(f"\n=== {label}: {start.date()} -> {end.date()} ===")
    for method_name, method in (
        ("settlement_prices", client.query_settlement_prices),
        ("balance_delta",     client.query_balance_delta),
    ):
        try:
            df = method(start, end)
            print(f"  {method_name:18} OK: {len(df)} rows")
        except Exception as e:
            status = ""
            r = getattr(e, "response", None)
            if r is not None and hasattr(r, "status_code"):
                status = f" (HTTP {r.status_code})"
            safe = _scrub(str(e)[:120], api_key)
            print(f"  {method_name:18} FAIL{status}: {type(e).__name__}: {safe}")


def main() -> int:
    secrets = load_secrets(str(REPO_ROOT))
    api_key = secrets.get("api_keys", "tennet")
    client = TenneTeuClient(api_key=api_key)

    ams = ZoneInfo("Europe/Amsterdam")
    today_midnight = datetime.now(ams).replace(hour=0, minute=0, second=0, microsecond=0)

    # Each window is a one-day slice ending at `today - n` days.
    # n=0 -> yesterday -> today (the failing window from data_fetcher.py)
    # n=2 -> 3 days ago -> 2 days ago
    # etc.
    windows = [
        (0,  "yesterday->today (current pipeline window)"),
        (1,  "2d ago -> 1d ago"),
        (2,  "3d ago -> 2d ago"),
        (3,  "4d ago -> 3d ago"),
        (7,  "8d ago -> 7d ago"),
        (14, "15d ago -> 14d ago"),
        (30, "31d ago -> 30d ago"),
    ]
    for n, label in windows:
        end = today_midnight - timedelta(days=n)
        start = end - timedelta(days=1)
        _try_window(client, label, start, end, api_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
