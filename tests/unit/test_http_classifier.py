"""
Tests for the shared HTTP-error classifier (Bundle B item 5).

Mirrors and supersedes the TenneT-specific tests for the same
functionality — these tests cover the shared module so future
collectors that adopt it have an explicit contract to point at.

File: tests/unit/test_http_classifier.py
Created: 2026-06-07
"""

from unittest.mock import MagicMock

import pytest

from collectors._http_classifier import (
    PERMANENT_HTTP_STATUSES,
    extract_http_status,
    raise_if_permanent,
)
from collectors.base import NonRetryableError


def _http_error_with_status(status: int) -> Exception:
    """Build a requests-style exception with .response.status_code."""
    exc = Exception(f"{status} Client Error: Unknown for url: ...")
    exc.response = MagicMock(status_code=status)
    return exc


class TestPermanentStatusSet:
    def test_includes_canonical_permanent_codes(self):
        for code in (400, 401, 403, 404, 422):
            assert code in PERMANENT_HTTP_STATUSES

    def test_excludes_transient_codes(self):
        for code in (408, 409, 423, 429, 500, 502, 503, 504):
            assert code not in PERMANENT_HTTP_STATUSES


class TestExtractHttpStatus:
    def test_requests_style_response_status_code(self):
        assert extract_http_status(_http_error_with_status(422)) == 422
        assert extract_http_status(_http_error_with_status(500)) == 500

    def test_aiohttp_style_status_attribute(self):
        exc = Exception("aiohttp.ClientResponseError shape")
        exc.status = 401
        assert extract_http_status(exc) == 401

    def test_library_wrapped_code_attribute(self):
        exc = Exception("some library exception")
        exc.code = 403
        assert extract_http_status(exc) == 403

    def test_library_wrapped_status_code_attribute(self):
        """Some libraries set .status_code directly on the exception."""
        exc = Exception("some library exception")
        exc.status_code = 404
        assert extract_http_status(exc) == 404

    def test_no_status_returns_none(self):
        assert extract_http_status(ValueError("nothing here")) is None

    def test_pre_response_connection_error_returns_none(self):
        """A connection error fires before the HTTP response object exists.
        The exception's .response attribute is None (canonical requests
        behavior). Function returns None → caller retries normally.
        """
        exc = Exception("connection reset")
        exc.response = None
        assert extract_http_status(exc) is None

    def test_non_int_status_attr_is_skipped(self):
        """An attacker-crafted or library-buggy exception sets .status to
        a string. The isinstance guard skips it and returns None."""
        exc = Exception("malformed")
        exc.status = "not an int"
        assert extract_http_status(exc) is None


class TestRaiseIfPermanent:
    def test_422_raises_non_retryable(self):
        with pytest.raises(NonRetryableError, match="HTTP 422"):
            raise_if_permanent(_http_error_with_status(422))

    def test_400_raises_non_retryable(self):
        with pytest.raises(NonRetryableError, match="HTTP 400"):
            raise_if_permanent(_http_error_with_status(400))

    def test_429_does_not_raise(self):
        """Rate-limited is transient — must NOT promote to NonRetryableError."""
        raise_if_permanent(_http_error_with_status(429))  # should silently return

    def test_500_does_not_raise(self):
        raise_if_permanent(_http_error_with_status(500))

    def test_unknown_status_does_not_raise(self):
        """No status → treat as retryable. Don't suppress unknown errors."""
        raise_if_permanent(ValueError("just a generic error"))

    def test_context_included_in_message(self):
        with pytest.raises(NonRetryableError, match=r"MyCollector:.*HTTP 422"):
            raise_if_permanent(
                _http_error_with_status(422), context="MyCollector"
            )

    def test_original_exception_chained_as_cause(self):
        """`from exc` so the original stack trace is preserved."""
        original = _http_error_with_status(422)
        try:
            raise_if_permanent(original)
            pytest.fail("Should have raised")
        except NonRetryableError as nre:
            assert nre.__cause__ is original
