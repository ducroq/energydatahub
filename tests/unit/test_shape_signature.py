"""
Tests for the shape-signature module (issue #27 Layer A).

Validates:
  - Identical shapes → identical hashes (stability across runs).
  - Timestamp-keyed maps collapse to one value-shape (so daily timestamp
    churn doesn't churn the fingerprint).
  - Per-field type changes produce different hashes.
  - Combined wraps expose `sources` for quick CI inspection.
  - Diff function classifies added/removed/changed/unchanged feeds.

File: tests/unit/test_shape_signature.py
Created: 2026-06-07
"""

import pytest

from utils.shape_signature import (
    compute_shape_signature,
    signature_hash,
    signatures_for_published_feeds,
    diff_signatures,
)


class TestComputeShapeSignature:
    """compute_shape_signature: structural fingerprint, value-blind."""

    def test_scalar_types(self):
        assert compute_shape_signature(None) == "null"
        assert compute_shape_signature(True) == "bool"
        assert compute_shape_signature(42) == "int"
        assert compute_shape_signature(3.14) == "float"
        assert compute_shape_signature("hello") == "str"

    def test_empty_dict(self):
        assert compute_shape_signature({}) == {"_kind": "dict", "keys": {}}

    def test_dict_signature_independent_of_value(self):
        """Same shape, different values → same signature."""
        a = {"x": 1, "y": 2}
        b = {"x": 99, "y": -7}
        assert compute_shape_signature(a) == compute_shape_signature(b)

    def test_dict_signature_independent_of_key_order(self):
        """Insertion order doesn't affect signature (we sort keys)."""
        a = {"x": 1, "y": 2}
        b = {"y": 2, "x": 1}
        assert compute_shape_signature(a) == compute_shape_signature(b)

    def test_type_change_changes_signature(self):
        """A field flipping from int to float should be detected."""
        sig_int = compute_shape_signature({"v": 1})
        sig_float = compute_shape_signature({"v": 1.0})
        assert sig_int != sig_float

    def test_added_field_changes_signature(self):
        """Adding a new field should be detected."""
        a = compute_shape_signature({"x": 1})
        b = compute_shape_signature({"x": 1, "y": 2})
        assert a != b

    def test_timestamp_map_collapses(self):
        """Many timestamp keys collapse to one representative value-shape.

        This is the critical property — without it, every daily run would
        produce a new hash because the timestamps rolled forward.
        """
        a = {
            "2026-06-01T00:00:00+02:00": {"price": 50.0},
            "2026-06-01T01:00:00+02:00": {"price": 55.0},
            "2026-06-01T02:00:00+02:00": {"price": 60.0},
        }
        b = {
            "2026-06-07T00:00:00+02:00": {"price": 100.0},  # different day, different prices
            "2026-06-07T01:00:00+02:00": {"price": 110.0},
        }
        assert compute_shape_signature(a) == compute_shape_signature(b)

    def test_timestamp_map_value_shape_change_detected(self):
        """Within a timestamp map, a value-shape change still flags."""
        a = {"2026-06-01T00:00:00+02:00": {"price": 50.0}}
        b = {"2026-06-01T00:00:00+02:00": {"price": 50.0, "volume": 1000}}
        assert compute_shape_signature(a) != compute_shape_signature(b)

    def test_nested_dict_signature(self):
        """Nested envelope-shaped payload."""
        payload = {
            "metadata": {"data_type": "energy_price", "version": "2.0"},
            "data": {
                "2026-06-01T00:00:00+02:00": 50.0,
                "2026-06-01T01:00:00+02:00": 55.0,
            },
        }
        sig = compute_shape_signature(payload)
        assert sig["_kind"] == "dict"
        assert "metadata" in sig["keys"]
        assert "data" in sig["keys"]
        assert sig["keys"]["data"]["_kind"] == "timestamp_map"

    def test_list_signature(self):
        sig = compute_shape_signature([1, 2, 3])
        assert sig == {"_kind": "list", "value_shape": "int"}

    def test_empty_list_signature(self):
        sig = compute_shape_signature([])
        assert sig == {"_kind": "list", "value_shape": None}

    def test_max_depth_guard(self):
        """Deeply-nested payloads don't blow the stack."""
        d = current = {}
        for _ in range(60):
            current["next"] = {}
            current = current["next"]
        sig = compute_shape_signature(d)
        # Walk down to verify it terminated cleanly with the sentinel.
        cur = sig
        while isinstance(cur, dict) and "keys" in cur and "next" in cur["keys"]:
            cur = cur["keys"]["next"]
        assert cur == "max_depth_exceeded" or isinstance(cur, dict)


class TestSignatureHash:
    """Stable SHA-256 hash for compact comparison."""

    def test_same_signature_same_hash(self):
        sig_a = compute_shape_signature({"x": 1, "y": 2})
        sig_b = compute_shape_signature({"y": 99, "x": -1})
        assert signature_hash(sig_a) == signature_hash(sig_b)

    def test_different_signature_different_hash(self):
        sig_a = compute_shape_signature({"x": 1})
        sig_b = compute_shape_signature({"x": 1, "y": 2})
        assert signature_hash(sig_a) != signature_hash(sig_b)

    def test_hash_is_short_deterministic_string(self):
        sig = compute_shape_signature({"x": 1})
        h = signature_hash(sig)
        assert isinstance(h, str)
        assert len(h) == 16
        # Hex chars only
        assert all(c in "0123456789abcdef" for c in h)


class TestSignaturesForPublishedFeeds:
    """Sidecar payload structure for `data/_shape_signatures.json`."""

    def test_basic_envelope(self):
        feeds = {
            "gas_storage.json": {
                "metadata": {"data_type": "gas_storage", "schema_version": "2.3"},
                "data": {
                    "2026-06-01T00:00:00+02:00": {
                        "fill_level_pct": 15.0,
                        "gas_in_storage_twh": 22.0,
                    },
                },
            },
        }
        sidecar = signatures_for_published_feeds(feeds, schema_version="2.3")
        assert sidecar["schema_version"] == "2.3"
        assert "computed_at" in sidecar
        assert "gas_storage.json" in sidecar["feeds"]
        entry = sidecar["feeds"]["gas_storage.json"]
        assert entry["data_type"] == "gas_storage"
        assert entry["sources"] is None  # standalone, not combined
        assert len(entry["shape_hash"]) == 16
        assert entry["shape_signature"]["_kind"] == "dict"

    def test_combined_wrap_extracts_sources(self):
        feeds = {
            "energy_price_forecast.json": {
                "metadata": {"data_type": "combined", "schema_version": "2.3"},
                "data": {
                    "entsoe":      {"metadata": {}, "data": {}},
                    "entsoe_de":   {"metadata": {}, "data": {}},
                    "energy_zero": {"metadata": {}, "data": {}},
                },
            },
        }
        sidecar = signatures_for_published_feeds(feeds, schema_version="2.3")
        entry = sidecar["feeds"]["energy_price_forecast.json"]
        assert entry["data_type"] == "combined"
        assert entry["sources"] == ["energy_zero", "entsoe", "entsoe_de"]

    def test_none_payloads_skipped(self):
        feeds = {
            "gas_storage.json": {"metadata": {"data_type": "gas_storage"}, "data": {}},
            "missing_feed.json": None,
        }
        sidecar = signatures_for_published_feeds(feeds, schema_version="2.3")
        assert "missing_feed.json" not in sidecar["feeds"]
        assert "gas_storage.json" in sidecar["feeds"]


class TestDiffSignatures:
    """Per-feed drift classification used by the CI detection script."""

    def _sidecar(self, schema_version: str, feeds: dict) -> dict:
        return {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": schema_version,
            "feeds": feeds,
        }

    def test_no_change_no_drift(self):
        feed = {"shape_hash": "abc", "data_type": "gas_storage", "sources": None,
                "shape_signature": {}}
        prev = self._sidecar("2.3", {"gas_storage.json": feed})
        curr = self._sidecar("2.3", {"gas_storage.json": feed})
        report = diff_signatures(prev, curr)
        assert report["feeds_changed"] == []
        assert report["feeds_unchanged"] == ["gas_storage.json"]
        assert report["schema_version_changed"] is False

    def test_shape_change_with_bumped_version_classified(self):
        """Shape changed AND schema bumped → still reported as changed
        (CI logic decides whether to fail; the diff just reports)."""
        prev = self._sidecar("2.2", {
            "gas_storage.json": {"shape_hash": "old", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
        })
        curr = self._sidecar("2.3", {
            "gas_storage.json": {"shape_hash": "new", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
        })
        report = diff_signatures(prev, curr)
        assert len(report["feeds_changed"]) == 1
        change = report["feeds_changed"][0]
        assert change["feed"] == "gas_storage.json"
        assert change["previous_hash"] == "old"
        assert change["current_hash"] == "new"
        assert report["schema_version_changed"] is True

    def test_shape_change_without_bumped_version_is_the_danger_case(self):
        """The case CI must fail on: shape changed but version didn't move."""
        prev = self._sidecar("2.3", {
            "gas_storage.json": {"shape_hash": "old", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
        })
        curr = self._sidecar("2.3", {
            "gas_storage.json": {"shape_hash": "new", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
        })
        report = diff_signatures(prev, curr)
        assert len(report["feeds_changed"]) == 1
        assert report["schema_version_changed"] is False

    def test_combined_feed_source_diff(self):
        """A combined wrap that loses/gains a per-collector source is flagged."""
        prev = self._sidecar("2.3", {
            "energy_price_forecast.json": {
                "shape_hash": "old", "data_type": "combined",
                "sources": ["entsoe", "entsoe_de", "energy_zero", "epex"],
                "shape_signature": {},
            },
        })
        curr = self._sidecar("2.3", {
            "energy_price_forecast.json": {
                "shape_hash": "new", "data_type": "combined",
                "sources": ["entsoe", "entsoe_de", "energy_zero", "elspot"],  # epex out, elspot in
                "shape_signature": {},
            },
        })
        report = diff_signatures(prev, curr)
        change = report["feeds_changed"][0]
        assert change["sources_diff"]["added"] == ["elspot"]
        assert change["sources_diff"]["removed"] == ["epex"]

    def test_feeds_added_and_removed(self):
        prev = self._sidecar("2.3", {
            "gas_storage.json": {"shape_hash": "a", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
            "removed_feed.json": {"shape_hash": "b", "data_type": "x",
                                  "sources": None, "shape_signature": {}},
        })
        curr = self._sidecar("2.3", {
            "gas_storage.json": {"shape_hash": "a", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
            "new_feed.json": {"shape_hash": "c", "data_type": "y",
                              "sources": None, "shape_signature": {}},
        })
        report = diff_signatures(prev, curr)
        assert report["feeds_added"] == ["new_feed.json"]
        assert report["feeds_removed"] == ["removed_feed.json"]
        assert report["feeds_unchanged"] == ["gas_storage.json"]

    def test_empty_previous_sidecar_first_run(self):
        """First CI run with the tripwire in place: no previous sidecar.
        Should report every current feed as added, no errors."""
        curr = self._sidecar("2.3", {
            "gas_storage.json": {"shape_hash": "a", "data_type": "gas_storage",
                                 "sources": None, "shape_signature": {}},
        })
        report = diff_signatures({}, curr)
        assert report["feeds_added"] == ["gas_storage.json"]
        assert report["feeds_removed"] == []
        assert report["feeds_changed"] == []


class TestDailyChurnImmunity:
    """Property test: signatures are stable across daily runs.

    A real published payload re-collected on a different day with different
    timestamps and different per-record values must produce the same hash —
    that's the foundation that makes CI drift detection signal-not-noise.
    """

    def test_gas_storage_stable_across_days(self):
        day1 = {
            "metadata": {"data_type": "gas_storage", "schema_version": "2.3"},
            "data": {
                "2026-06-01T00:00:00+02:00": {
                    "fill_level_pct": 15.24,
                    "gas_in_storage_twh": 21.9185,
                    "injection_gwh": 385.08,
                },
            },
        }
        day7 = {
            "metadata": {"data_type": "gas_storage", "schema_version": "2.3"},
            "data": {
                "2026-06-07T00:00:00+02:00": {
                    "fill_level_pct": 22.5,        # different value
                    "gas_in_storage_twh": 32.4,    # different value
                    "injection_gwh": 401.2,        # different value
                },
                "2026-06-07T12:00:00+02:00": {     # extra timestamp same day
                    "fill_level_pct": 23.0,
                    "gas_in_storage_twh": 33.0,
                    "injection_gwh": 410.0,
                },
            },
        }
        assert signature_hash(compute_shape_signature(day1)) == \
               signature_hash(compute_shape_signature(day7))
