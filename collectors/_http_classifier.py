"""
HTTP Error Classification for API Collectors
---------------------------------------------
Shared helpers for classifying HTTP errors raised by external API
clients (requests, aiohttp, library wrappers) into retryable vs
permanent. Extracted from `collectors/tennet.py` after the multi-model
review battery on 7c0de64 flagged that the pattern (35 lines of
boilerplate) would be copy-pasted at the next ENTSO-E/NED/GIE
incident.

Usage in a collector's `_fetch_raw_data`:

    from collectors._http_classifier import raise_if_permanent

    try:
        return await api_call(...)
    except Exception as e:
        raise_if_permanent(e, context="MyCollector")  # re-raises as
        raise                                          # NonRetryableError
                                                       # or propagates for retry

`raise_if_permanent` looks at the exception's HTTP status (if any) and
re-raises as `NonRetryableError` when the status is in the permanent
set. Anything else (no status, retryable status, network error) is left
alone so `BaseCollector._retry_with_backoff` retries normally.

File: collectors/_http_classifier.py
Created: 2026-06-07
"""

from __future__ import annotations

from typing import Optional

from collectors.base import NonRetryableError

# HTTP statuses that are permanent for the current request window. Refer
# to RFC 7231 §6.5 for client-error semantics:
#   400 Bad Request          — malformed parameter / unsupported value
#   401 Unauthorized         — bad/missing/expired credentials
#   403 Forbidden            — credentials lack permission for this endpoint
#   404 Not Found            — resource doesn't exist at this URL
#   422 Unprocessable Entity — request well-formed but semantically invalid
#                              (TenneT returns this when data hasn't been
#                              published yet for the requested window)
# Statuses NOT included: 408 (request timeout — retry), 409 (conflict),
# 423 (locked), 429 (rate-limited — retry with backoff), all 5xx (server
# errors — retry).
PERMANENT_HTTP_STATUSES = frozenset({400, 401, 403, 404, 422})


def extract_http_status(exc: Exception) -> Optional[int]:
    """
    Extract an HTTP status code from an exception, if one is available.

    Tries several common shapes:
      - `requests.exceptions.HTTPError` → `exc.response.status_code`
      - `aiohttp.ClientResponseError`   → `exc.status`
      - Library-wrapped errors that set `.code` or `.status_code` directly

    Returns None when the status cannot be determined — the caller
    should treat that as "retryable" (don't suppress unknown errors).

    Args:
        exc: The exception caught from an API call.

    Returns:
        Integer HTTP status code, or None when not present.
    """
    # The canonical requests-library path
    response = getattr(exc, 'response', None)
    if response is not None:
        status = getattr(response, 'status_code', None)
        if isinstance(status, int):
            return status
    # aiohttp.ClientResponseError sets .status directly; some library
    # wrappers use .code or .status_code on the exception itself.
    for attr in ('status', 'code', 'status_code'):
        val = getattr(exc, attr, None)
        if isinstance(val, int):
            return val
    return None


def raise_if_permanent(
    exc: Exception,
    *,
    context: str = "",
) -> None:
    """
    Inspect an exception and re-raise as `NonRetryableError` if its HTTP
    status indicates a permanent client error.

    Pattern for use inside a collector's `_fetch_raw_data`:

        try:
            return await api.fetch(...)
        except Exception as e:
            raise_if_permanent(e, context="MyCollector")
            raise

    The `context` string is included in the wrapping `NonRetryableError`
    message so error logs identify the collector that bailed out.

    Args:
        exc:     The exception under inspection.
        context: Optional short string identifying the calling collector,
                 used in the NonRetryableError message.

    Raises:
        NonRetryableError: If the exception carries a status in
            `PERMANENT_HTTP_STATUSES`. The original exception is chained
            as `__cause__` so the original stack trace is preserved.
    """
    status = extract_http_status(exc)
    if status is None or status not in PERMANENT_HTTP_STATUSES:
        return
    prefix = f"{context}: " if context else ""
    raise NonRetryableError(
        f"{prefix}upstream returned HTTP {status} — permanent for this "
        f"request window. Original error: {exc}"
    ) from exc
