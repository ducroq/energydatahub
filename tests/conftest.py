"""
Shared pytest fixtures.

File: tests/conftest.py
Created: 2026-09-02
"""

import pytest

from collectors._host_breaker import reset_all as reset_host_breakers


@pytest.fixture(autouse=True)
def _reset_host_breakers():
    """Clear the process-wide host-breaker registry around every test.

    `collectors/_host_breaker._BREAKERS` is module-global and deliberately
    lives for the life of the process (a scheduled run is one process). Under
    pytest that lifetime spans the whole session, so the first test that lets a
    real failing `_retry_single` reach `ENTSOE_API_HOST` leaves an OPEN breaker
    behind, and every later test touching an ENTSO-E collector silently has its
    requests suppressed — an order-dependent failure that reproduces only when
    the modules run in a particular sequence.

    Nothing triggers that today (the existing ENTSO-E tests patch
    `_retry_single` wholesale), which is exactly why this belongs here rather
    than in the one test module that currently remembers to do it. Note that
    `tests/conftest.py` is outside `.claude/hooks/verify_edit.py`'s scope, so
    changes here are not covered by the edit hook — run the full suite.
    """
    reset_host_breakers()
    yield
    reset_host_breakers()
