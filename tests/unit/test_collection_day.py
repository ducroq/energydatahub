"""Tests for resolve_collection_day — the late-cron anchor (run 37381385635)."""
from datetime import datetime
from zoneinfo import ZoneInfo

from data_fetcher import resolve_collection_day

AMS = ZoneInfo('Europe/Amsterdam')


def test_no_anchor_uses_local_day():
    now = datetime(2026, 10, 5, 20, 30, tzinfo=AMS)
    assert resolve_collection_day(now) == datetime(2026, 10, 5, tzinfo=AMS)


def test_late_run_past_local_midnight_stays_on_scheduled_day():
    # 22:16 UTC on 10-05 is 00:16 Amsterdam on 10-06.
    now = datetime(2026, 10, 5, 22, 16, tzinfo=ZoneInfo('UTC')).astimezone(AMS)
    assert now.date().isoformat() == '2026-10-06'
    day = resolve_collection_day(now, '2026-10-05')
    assert day == datetime(2026, 10, 5, tzinfo=AMS)
    assert day.utcoffset() == now.utcoffset()


def test_anchor_matching_local_day_is_a_no_op():
    now = datetime(2026, 10, 5, 21, 0, tzinfo=AMS)
    assert resolve_collection_day(now, '2026-10-05') == resolve_collection_day(now)


def test_anchor_across_dst_change_gets_that_days_offset():
    # 2026-10-25 is the CEST->CET switch; the anchored midnight is still CEST.
    now = datetime(2026, 10, 26, 0, 30, tzinfo=AMS)
    day = resolve_collection_day(now, '2026-10-25')
    assert day.utcoffset().total_seconds() == 2 * 3600
