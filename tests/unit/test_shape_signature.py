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
    classify_data_member_drift,
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

    def test_date_only_keys_collapse(self):
        """Date-only keys ('YYYY-MM-DD', no T separator) also collapse.

        Required by `market_proxies.gas_ttf.history` and
        `market_history.*.data` which key rolling history windows by
        calendar date. Without this, the signature enumerated every day
        and the fingerprint churned each time the window advanced.
        """
        a = {
            "2026-06-01": 50.0,
            "2026-06-02": 55.0,
            "2026-06-03": 60.0,
        }
        b = {
            "2026-06-07": 100.0,  # different day, different price
            "2026-06-08": 110.0,
            "2026-06-09": 120.0,
            "2026-06-10": 130.0,  # one extra date
        }
        sig_a = compute_shape_signature(a)
        sig_b = compute_shape_signature(b)
        assert sig_a == sig_b
        assert sig_a["_kind"] == "timestamp_map"
        assert sig_a["value_shape"] == "float"

    def test_date_only_value_shape_change_still_detected(self):
        """Collapse must not blind us to real shape drift inside a
        date-keyed map (e.g. a value flips from float to dict)."""
        a = {"2026-06-01": 50.0}
        b = {"2026-06-01": {"price": 50.0}}
        assert compute_shape_signature(a) != compute_shape_signature(b)

    def test_mixed_keys_do_not_collapse(self):
        """Documented design constraint: a dict mixing timestamp keys with
        a non-timestamp key (e.g. `{ts1: v1, ts2: v2, 'metadata': m}`)
        falls through to the plain dict branch and does NOT collapse to
        a timestamp_map. The fingerprint will include every timestamp
        key. This is OK for the canonical envelope (where timestamps and
        metadata live at separate nesting levels) but will churn
        day-over-day for any feed that mixes them — reviewer LOW finding
        on 7c0de64.
        """
        day1 = {
            "2026-06-01T00:00:00+02:00": 50.0,
            "metadata": {"source": "X"},
        }
        day7 = {
            "2026-06-07T00:00:00+02:00": 100.0,  # different timestamp
            "metadata": {"source": "X"},
        }
        # Different fingerprints — the timestamps churn.
        sig1 = compute_shape_signature(day1)
        sig7 = compute_shape_signature(day7)
        assert sig1["_kind"] == "dict"
        assert sig7["_kind"] == "dict"
        assert sig1 != sig7

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
        # 32 hex chars = 128 bits after security review extended from 64
        # (reviewer recommendation on 7c0de64). At 16 hex chars (the old
        # length) the test must NOT pass — that's the regression guard.
        assert len(h) == 32
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
        assert len(entry["shape_hash"]) == 32  # 128-bit (extended from 64)
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

    def test_diff_signatures_contract_keys_always_lists(self):
        """opus M3: pin the contract that feeds_added/removed/changed
        are always lists (not None) regardless of input shape. Defends
        the detect_schema_drift.py truthy checks against silent None-vs-
        empty-list confusion."""
        # Empty inputs
        r = diff_signatures({}, {})
        assert isinstance(r["feeds_added"], list)
        assert isinstance(r["feeds_removed"], list)
        assert isinstance(r["feeds_changed"], list)
        assert isinstance(r["feeds_unchanged"], list)
        # Inputs without `feeds` key at all
        r = diff_signatures({"schema_version": "2.3"}, {"schema_version": "2.3"})
        assert r["feeds_added"] == []
        assert r["feeds_removed"] == []
        assert r["feeds_changed"] == []
        # Non-dict input defensively coerced
        r = diff_signatures(None, {"schema_version": "2.3", "feeds": {}})
        assert r["feeds_added"] == []


class TestDailyChurnImmunity:
    """Property test: signatures are stable across daily runs.

    A real published payload re-collected on a different day with different
    timestamps and different per-record values must produce the same hash —
    that's the foundation that makes CI drift detection signal-not-noise.
    """

    def test_market_history_stable_across_days(self):
        """Real-world regression guard from CI run 27278130553 (2026-06-10):
        market_history.json and market_proxies.json drifted daily because
        their rolling history windows were keyed by 'YYYY-MM-DD' (date-only,
        no T) — which the original _TS_PATTERN didn't recognise as
        timestamp-like, so every date was enumerated individually in the
        signature and the fingerprint churned each time the window rolled.
        """
        day1 = {
            "metadata": {"data_type": "market_history", "schema_version": "2.4"},
            "data": {
                "gas_ttf": {
                    "metadata": {"source": "yfinance", "ticker": "TTF=F", "units": "EUR/MWh"},
                    "data": {
                        "2026-02-24": 42.5,
                        "2026-02-25": 43.1,
                        "2026-02-26": 41.8,
                    },
                },
            },
        }
        day7 = {
            "metadata": {"data_type": "market_history", "schema_version": "2.4"},
            "data": {
                "gas_ttf": {
                    "metadata": {"source": "yfinance", "ticker": "TTF=F", "units": "EUR/MWh"},
                    "data": {
                        "2026-03-02": 45.0,  # window rolled forward
                        "2026-03-03": 46.2,
                        "2026-03-04": 44.9,
                        "2026-03-05": 47.1,  # one extra date
                    },
                },
            },
        }
        assert signature_hash(compute_shape_signature(day1)) == \
               signature_hash(compute_shape_signature(day7))

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


class TestClassifyDataMemberDrift:
    """A location/source dropping out of a multi-member feed is a data-catalog
    change, not a schema change (2026-08-14: one dropped buurt location failed
    the drift tripwire and blocked the publish of 18 healthy feeds).

    The classifier must be narrow: it downgrades ONLY when every surviving
    member is structurally identical and the envelope is otherwise untouched.
    """

    @staticmethod
    def _buurt_payload(members):
        """Envelope shaped like weather_forecast_buurt: {metadata, data}."""
        return {
            "metadata": {
                "data_type": "weather_forecast",
                "schema_version": "2.4",
                "location_count": len(members),
                "locations": list(members),
                "units": {"temperature_2m": "C"},
            },
            "data": {
                name: {
                    "2026-08-14T00:00:00+02:00": {
                        "temperature_2m": 18.4,
                        "cloud_cover": 55.0,
                    },
                }
                for name in members
            },
        }

    def _sig(self, members):
        return compute_shape_signature(self._buurt_payload(members))

    def test_dropped_member_classified_as_member_drift(self):
        prev = self._sig(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        curr = self._sig(["Elderveld_Arnhem_NL"])
        verdict = classify_data_member_drift(prev, curr)
        assert verdict is not None
        assert verdict["removed"] == ["Elsweide_Arnhem_NL"]
        assert verdict["added"] == []
        assert verdict["retained"] == ["Elderveld_Arnhem_NL"]

    def test_recovered_member_classified_as_member_drift(self):
        """The symmetric case: yesterday's baseline was the degraded shape."""
        prev = self._sig(["Elderveld_Arnhem_NL"])
        curr = self._sig(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        verdict = classify_data_member_drift(prev, curr)
        assert verdict is not None
        assert verdict["added"] == ["Elsweide_Arnhem_NL"]
        assert verdict["removed"] == []

    def test_field_change_in_surviving_member_is_not_member_drift(self):
        """The whole point of the narrowness: a real structural break that
        happens to ALSO drop a member must still enforce."""
        prev = self._sig(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        degraded = self._buurt_payload(["Elderveld_Arnhem_NL"])
        # Surviving member loses a field — a genuine shape break.
        for record in degraded["data"]["Elderveld_Arnhem_NL"].values():
            del record["cloud_cover"]
        assert classify_data_member_drift(
            prev, compute_shape_signature(degraded)
        ) is None

    def test_metadata_shape_change_is_not_member_drift(self):
        prev = self._sig(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        changed = self._buurt_payload(["Elderveld_Arnhem_NL"])
        changed["metadata"]["new_envelope_key"] = "x"
        assert classify_data_member_drift(
            prev, compute_shape_signature(changed)
        ) is None

    def test_member_dropout_does_not_change_metadata_shape(self):
        """Guards the assumption the classifier rests on: `locations` is a
        list-of-str and `location_count` an int no matter how many members
        there are, so a dropout leaves metadata's SHAPE identical. This is
        what made the 2026-08-14 failing hash reproducible by deleting the
        data key alone."""
        two = compute_shape_signature(
            self._buurt_payload(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        )
        one = compute_shape_signature(
            self._buurt_payload(["Elderveld_Arnhem_NL"])
        )
        assert two["keys"]["metadata"] == one["keys"]["metadata"]
        assert two["keys"]["data"] != one["keys"]["data"]

    def test_all_members_gone_is_not_member_drift(self):
        """An emptied member map never legitimately reaches here — the six
        Open-Meteo feeds are coerced to absent upstream by
        PRESENT_EMPTY_GRACE_FEEDS, and every other feed fails the completeness
        gate. Either way it must not be laundered into a warning."""
        prev = self._sig(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        empty = self._buurt_payload(["Elsweide_Arnhem_NL"])
        empty["data"] = {}
        assert classify_data_member_drift(
            prev, compute_shape_signature(empty)
        ) is None

    def test_identical_signatures_return_none(self):
        sig = self._sig(["Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"])
        assert classify_data_member_drift(sig, sig) is None

    def test_timestamp_keyed_data_block_never_matches(self):
        """A feed whose `data` is timestamp-keyed collapses to a
        timestamp_map node, so the member rule cannot fire on it."""
        prev = compute_shape_signature({
            "metadata": {"data_type": "gas_storage"},
            "data": {"2026-08-13T00:00:00+02:00": {"fill_level_pct": 15.2}},
        })
        curr = compute_shape_signature({
            "metadata": {"data_type": "gas_storage"},
            "data": {"2026-08-14T00:00:00+02:00": {"fill_level_pct": 15.2,
                                                   "extra": 1}},
        })
        assert classify_data_member_drift(prev, curr) is None

    def test_malformed_signatures_return_none(self):
        """Existing tripwire tests carry `shape_signature: {}` — those must
        keep enforcing, so junk input must never downgrade."""
        assert classify_data_member_drift({}, {}) is None
        assert classify_data_member_drift(None, None) is None
        assert classify_data_member_drift("str", "str") is None
        assert classify_data_member_drift(
            {"_kind": "dict", "keys": {"data": {"_kind": "dict", "keys": {}}}},
            {"_kind": "dict", "keys": {"other": "str"}},
        ) is None


class TestMemberHomogeneity:
    """The homogeneity rule: a member catalog is a map of LIKE things.

    Defence in depth behind `MEMBER_MAPPED_FEEDS`. It rejects field-keyed
    `data` blocks on structure alone, and validates members that appear only
    on the current side — the two holes the /review-changes battery found in
    the first draft.
    """

    def test_field_keyed_block_rejected_on_structure_alone(self):
        """grid_imbalance's `data` keys are field names, and one of them is a
        str-map next to two float-maps. Even without the eligibility gate,
        dropping `imbalance_price` must not read as a member dropout."""
        def payload(fields):
            return {
                "metadata": {"data_type": "grid_imbalance"},
                "data": {
                    name: {"2026-08-14T00:00:00+02:00":
                           "up" if name == "direction" else 1.5}
                    for name in fields
                },
            }
        prev = compute_shape_signature(
            payload(["balance_delta", "direction", "imbalance_price"]))
        curr = compute_shape_signature(payload(["balance_delta", "direction"]))
        assert classify_data_member_drift(prev, curr) is None

    def test_added_member_with_foreign_shape_rejected(self):
        """A new key must match its peers, not arrive carrying anything."""
        prev = compute_shape_signature({
            "metadata": {"m": 1},
            "data": {"A": {"2026-08-14T00:00:00+02:00": {"t": 1.0}}},
        })
        curr = compute_shape_signature({
            "metadata": {"m": 1},
            "data": {
                "A": {"2026-08-14T00:00:00+02:00": {"t": 1.0}},
                "B": "just a string",
            },
        })
        assert classify_data_member_drift(prev, curr) is None

    def test_homogeneous_added_member_accepted(self):
        """A recovering location arrives with its peers' shape — allowed."""
        prev = compute_shape_signature({
            "metadata": {"m": 1},
            "data": {"A": {"2026-08-14T00:00:00+02:00": {"t": 1.0}}},
        })
        curr = compute_shape_signature({
            "metadata": {"m": 1},
            "data": {
                "A": {"2026-08-14T00:00:00+02:00": {"t": 1.0}},
                "B": {"2026-08-14T00:00:00+02:00": {"t": 2.0}},
            },
        })
        verdict = classify_data_member_drift(prev, curr)
        assert verdict is not None and verdict["added"] == ["B"]


class TestDiagnosticEnvelopeKeys:
    """`metadata['collector_quality_issues']` is attached only when a run
    raised an issue, so it appears on exactly the degraded runs member drift
    exists to let through — the collectors' `location_completeness` issue now
    guarantees it on a dropout.

    Without the exemption these runs fail the gate for the act of reporting
    their own degradation, restoring the 2026-08-14 publish block. Verified by
    mutation: replacing `_without_diagnostic_keys` with the identity function
    left the whole suite green before these tests existed.
    """

    QUALITY_ISSUES = [{
        "check_name": "location_completeness",
        "severity": "warning",
        "message": "1 of 2 location(s) returned no data",
        "details": {"requested": ["A", "B"], "delivered": ["B"],
                    "missing": ["A"]},
    }]

    def _payload(self, members, *, issues=False, extra_meta=None):
        meta = {"data_type": "weather_forecast", "location_count": len(members),
                "locations": list(members)}
        if issues:
            meta["collector_quality_issues"] = self.QUALITY_ISSUES
        if extra_meta:
            meta.update(extra_meta)
        return {
            "metadata": meta,
            "data": {m: {"2026-08-14T00:00:00+02:00": {"t": 1.0}}
                     for m in members},
        }

    def test_dropout_with_new_quality_issue_is_member_drift(self):
        """The case the collectors now produce on every dropout."""
        prev = compute_shape_signature(self._payload(["A", "B"]))
        curr = compute_shape_signature(self._payload(["B"], issues=True))
        verdict = classify_data_member_drift(prev, curr)
        assert verdict is not None, (
            "a degraded run that reports its own degradation must still "
            "classify as member drift"
        )
        assert verdict["removed"] == ["A"]

    def test_real_metadata_break_alongside_the_diagnostic_key_still_fails(self):
        """The exemption must be surgical: it excuses the diagnostic key and
        nothing travelling with it."""
        prev = compute_shape_signature(self._payload(["A", "B"]))
        curr = compute_shape_signature(
            self._payload(["B"], issues=True, extra_meta={"brand_new": "x"})
        )
        assert classify_data_member_drift(prev, curr) is None

    def test_field_loss_alongside_the_diagnostic_key_still_fails(self):
        prev = compute_shape_signature(self._payload(["A", "B"]))
        broken = self._payload(["B"], issues=True)
        for record in broken["data"]["B"].values():
            record["t"] = "now a string"
        assert classify_data_member_drift(
            prev, compute_shape_signature(broken)) is None

    def test_diagnostic_key_disappearing_on_recovery_is_also_excused(self):
        """Symmetric: the run after a dropout drops the key again."""
        prev = compute_shape_signature(self._payload(["B"], issues=True))
        curr = compute_shape_signature(self._payload(["A", "B"]))
        verdict = classify_data_member_drift(prev, curr)
        assert verdict is not None and verdict["added"] == ["A"]


class TestMagnitudeFloor:
    """A majority of previous members must survive for a member-set change to
    read as routine. `demand_weather_forecast` losing 10 of 11 locations
    satisfied the old "at least one retained" rule and exited 0 on the real
    sidecar, with nothing downstream catching it."""

    @staticmethod
    def _payload(members):
        return {
            "metadata": {"data_type": "weather_forecast"},
            "data": {m: {"2026-08-14T00:00:00+02:00": {"t": 1.0}}
                     for m in members},
        }

    def _verdict(self, prev_members, curr_members):
        return classify_data_member_drift(
            compute_shape_signature(self._payload(prev_members)),
            compute_shape_signature(self._payload(curr_members)),
        )

    def test_mass_dropout_is_not_member_drift(self):
        eleven = [f"loc{i}" for i in range(11)]
        assert self._verdict(eleven, ["loc0"]) is None

    def test_exactly_half_retained_is_allowed(self):
        """The buurt pair is 2 members losing 1 — the motivating case must
        survive the floor, so the boundary is majority-inclusive."""
        verdict = self._verdict(["A", "B"], ["B"])
        assert verdict is not None and verdict["removed"] == ["A"]

    def test_just_under_half_is_rejected(self):
        seven = [f"loc{i}" for i in range(7)]
        assert self._verdict(seven, seven[:4]) is not None   # 4 of 7 kept
        assert self._verdict(seven, seven[:3]) is None       # 3 of 7 kept

    def test_recovery_is_not_limited_by_the_floor(self):
        """The floor bounds LOSS. A feed recovering from a degraded baseline
        adds members, and must not be blocked by it."""
        verdict = self._verdict(["A"], ["A", "B", "C", "D"])
        assert verdict is not None
        assert verdict["added"] == ["B", "C", "D"]
