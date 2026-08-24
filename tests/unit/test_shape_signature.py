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

import itertools

import pytest

from utils.shape_signature import (
    _conflict_node,
    _is_dict_signature,
    _merge_signatures,
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


class TestTimestampMapUnion:
    """A timestamp map's value_shape is the MERGE of every record.

    Before 2026-08-23 it was the shape of one sampled record, which made the
    fingerprint order-dependent and one record deep. See the module docstring
    of utils/shape_signature.py for the incident.
    """

    @staticmethod
    def _map(*records):
        return {
            f"2026-08-23T{i:02d}:00:00+02:00": rec
            for i, rec in enumerate(records)
        }

    def test_field_in_any_record_survives(self):
        """THE core guard: {ts1:{a}, ts2:{a,b}} must keep `b`."""
        sig = compute_shape_signature(
            self._map({"a": 1.0}, {"a": 2.0, "b": 3.0})
        )
        assert sig["_kind"] == "timestamp_map"
        assert sig["value_shape"] == {
            "_kind": "dict", "keys": {"a": "float", "b": "float"},
        }

    def test_sparse_first_record_matches_complete_map(self):
        """The 2026-08-23 load_forecast incident, reduced.

        ENTSO-E published no actual load for DE_LU, so the FIRST record
        carried only `load_forecast` while later records were complete. Under
        first-record sampling that read as "two fields removed" and aborted
        the whole 20-feed publish. The merge must make it indistinguishable
        from the all-complete map.
        """
        complete = {"load_actual": 1234.5, "load_forecast": 1200.0,
                    "forecast_error": 34.5}
        degraded = self._map({"load_forecast": 1200.0}, *([complete] * 73))
        pristine = self._map(*([complete] * 74))
        assert signature_hash(compute_shape_signature(degraded)) == \
            signature_hash(compute_shape_signature(pristine))

    def test_union_is_order_independent(self):
        """The same record SET in any order yields one signature."""
        records = [
            {"a": 1.0, "b": 2.0},
            {"a": 1.0},
            {"a": 1.0, "b": 2.0, "c": "x"},
        ]
        sigs = {
            signature_hash(compute_shape_signature(self._map(*perm)))
            for perm in itertools.permutations(records)
        }
        assert len(sigs) == 1

    def test_null_absorbs_into_concrete_type(self):
        """`calendar_features.data[*].holiday_name_nl` is null on ordinary
        days and a str on holidays; the informative shape must win, and it
        must not depend on which record came first."""
        null_first = self._map({"holiday_name": None},
                               {"holiday_name": "Tweede Kerstdag"})
        str_first = self._map({"holiday_name": "Tweede Kerstdag"},
                              {"holiday_name": None})
        assert compute_shape_signature(null_first) == \
            compute_shape_signature(str_first)
        assert compute_shape_signature(null_first)["value_shape"] == {
            "_kind": "dict", "keys": {"holiday_name": "str"},
        }

    def test_all_null_records_stay_null(self):
        """Absorption must not invent a type that no record ever carried."""
        sig = compute_shape_signature(
            self._map({"holiday_name": None}, {"holiday_name": None})
        )
        assert sig["value_shape"] == {
            "_kind": "dict", "keys": {"holiday_name": "null"},
        }

    def test_int_and_float_widen_within_one_map(self):
        """JSON has one number type; `0` next to `0.5` is representation."""
        sig = compute_shape_signature(self._map({"v": 1}, {"v": 1.5}))
        assert sig["value_shape"]["keys"]["v"] == "float"

    def test_int_vs_float_still_distinct_across_maps(self):
        """Widening is WITHIN a map — a whole-map type flip still drifts."""
        assert compute_shape_signature(self._map({"v": 1})) != \
            compute_shape_signature(self._map({"v": 1.0}))

    def test_field_absent_from_every_record_still_drifts(self):
        """The detector is not weakened: a real removal takes the field out
        of every record, so it leaves the union too."""
        day1 = self._map({"a": 1.0}, {"a": 2.0, "b": 3.0})
        day2 = self._map({"a": 1.0}, {"a": 2.0})
        assert compute_shape_signature(day1) != compute_shape_signature(day2)

    def test_field_surviving_in_one_record_is_deliberately_tolerated(self):
        """Pins the BOUNDARY of the tolerance, not a desirable property.

        A field removed from all-but-one record hashes the same as one present
        throughout. Sampling could catch this, but only by luck — if the
        sampled record happened to be a sparse one, the same coin-flip that
        aborted the 2026-08-23 publish. A genuine schema removal takes the
        field out of EVERY record and still drifts (see the test above); what
        is tolerated here is intra-day completeness, which is the FMEA gate's
        job in utils/data_quality.py, not a structural fingerprint's.

        If you are here because you want this to drift, you are moving the
        boundary back — read `_merge_signatures`' "WHAT THE UNION NO LONGER
        CATCHES" note first.
        """
        complete = {"a": 1.0, "b": 2.0}
        all_present = self._map(*([complete] * 3))
        one_straggler = self._map({"a": 1.0}, {"a": 1.0}, complete)
        assert signature_hash(compute_shape_signature(all_present)) == \
            signature_hash(compute_shape_signature(one_straggler))

    def test_empty_list_absorbs_into_populated(self):
        sig = compute_shape_signature(
            self._map({"alerts": []}, {"alerts": ["storm"]})
        )
        assert sig["value_shape"]["keys"]["alerts"] == {
            "_kind": "list", "value_shape": "str",
        }

    def test_bool_versus_numeric_is_a_loud_conflict(self):
        """bool is a distinct JSON type — not widened into the number tower."""
        sig = compute_shape_signature(self._map({"v": True}, {"v": 1}))
        node = sig["value_shape"]["keys"]["v"]
        assert node["_kind"] == "conflict"
        assert node["shapes"] == ['"bool"', '"int"']

    def test_dict_versus_scalar_is_a_loud_conflict(self):
        """Irreconcilable nodes must change the hash, not be silently
        resolved in favour of whichever record iterated first."""
        sig = compute_shape_signature(
            self._map({"v": {"x": 1}}, {"v": "oops"})
        )
        node = sig["value_shape"]["keys"]["v"]
        assert node["_kind"] == "conflict"
        assert len(node["shapes"]) == 2

    def test_conflicts_are_flat_and_order_independent(self):
        """Three-way disagreement folds to ONE flat conflict node regardless
        of record order. Nesting here would break associativity and hand the
        signature back its order-dependence."""
        records = [{"v": True}, {"v": 1}, {"v": "s"}]
        nodes = []
        for perm in itertools.permutations(records):
            node = compute_shape_signature(
                self._map(*perm))["value_shape"]["keys"]["v"]
            nodes.append(node)
        assert all(n == nodes[0] for n in nodes)
        assert nodes[0]["_kind"] == "conflict"
        assert nodes[0]["shapes"] == ['"bool"', '"int"', '"str"']

    def test_conflict_does_not_leak_into_dict_signature_checks(self):
        """A conflict node is `_kind: conflict`, so the member-drift and
        envelope helpers (which gate on `_kind == 'dict'`) can never mistake
        it for a plain dict node."""
        sig = compute_shape_signature(self._map({"v": True}, {"v": 1}))
        assert not _is_dict_signature(sig["value_shape"]["keys"]["v"])

    def test_homogeneous_map_is_byte_identical_to_sampling(self):
        """The migration guarantee: where every record agrees, the merge
        returns exactly what sampling returned, so no committed baseline
        moved when this landed."""
        rec = {"price": 50.0, "volume": 10}
        sig = compute_shape_signature(self._map(rec, dict(rec), dict(rec)))
        assert sig == {
            "_kind": "timestamp_map",
            "value_shape": {"_kind": "dict",
                            "keys": {"price": "float", "volume": "int"}},
        }


class TestMergeSignatures:
    """`_merge_signatures` directly — the algebraic properties the fold
    depends on. If these break, record order starts changing hashes."""

    # The pool must include the awkward cases, not just the tidy ones: a
    # conflict node, conflicts NESTED inside a dict value and inside a
    # list/timestamp_map value_shape, the depth sentinel, an unknown `_kind`,
    # and dicts that themselves conflict when merged. A pool of only tidy
    # nodes would have passed while the two real associativity bugs — null
    # absorption ordered after the conflict path, and int/float widening
    # losing the int — were live.
    _CONFLICT = {"_kind": "conflict", "shapes": ['"bool"', '"int"']}
    NODES = [
        "null", "int", "float", "str", "bool", None, "max_depth_exceeded",
        {"_kind": "dict", "keys": {"x": "int"}},
        {"_kind": "dict", "keys": {"x": "float", "y": "str"}},
        {"_kind": "dict", "keys": {"x": "bool"}},
        {"_kind": "dict", "keys": {"x": _CONFLICT}},
        {"_kind": "list", "value_shape": "str"},
        {"_kind": "list", "value_shape": None},
        {"_kind": "list", "value_shape": _CONFLICT},
        {"_kind": "timestamp_map", "value_shape": "float"},
        {"_kind": "timestamp_map", "value_shape": _CONFLICT},
        {"_kind": "some_future_kind", "z": 1},
        _CONFLICT,
    ]

    def test_commutative(self):
        for a, b in itertools.product(self.NODES, repeat=2):
            assert _merge_signatures(a, b) == _merge_signatures(b, a), \
                f"not commutative for {a!r} + {b!r}"

    def test_associative(self):
        for a, b, c in itertools.product(self.NODES, repeat=3):
            left = _merge_signatures(_merge_signatures(a, b), c)
            right = _merge_signatures(a, _merge_signatures(b, c))
            assert left == right, f"not associative for {a!r},{b!r},{c!r}"

    def test_idempotent(self):
        for node in self.NODES:
            assert _merge_signatures(node, node) == node

    def test_does_not_mutate_its_inputs(self):
        a = {"_kind": "dict", "keys": {"x": "int"}}
        b = {"_kind": "dict", "keys": {"y": "str"}}
        _merge_signatures(a, b)
        assert a == {"_kind": "dict", "keys": {"x": "int"}}
        assert b == {"_kind": "dict", "keys": {"y": "str"}}

    def test_dict_key_union_recurses(self):
        merged = _merge_signatures(
            {"_kind": "dict", "keys": {"x": "int", "shared": "null"}},
            {"_kind": "dict", "keys": {"y": "str", "shared": "float"}},
        )
        assert merged == {"_kind": "dict", "keys": {
            "x": "int", "y": "str", "shared": "float"}}

    def test_differing_kinds_conflict(self):
        merged = _merge_signatures(
            {"_kind": "list", "value_shape": "str"},
            {"_kind": "timestamp_map", "value_shape": "str"},
        )
        assert merged["_kind"] == "conflict"

    def test_unknown_kind_does_not_recurse_forever(self):
        """Two same-kind nodes whose `_kind` the merge does not dispatch on
        must NOT be routed back into the merge by conflict normalisation.

        They were, once: `_merge_signatures` had no branch for the kind so it
        returned `_conflict_node(a, b)`, which grouped them by kind and handed
        the same pair straight back — unbounded mutual recursion, and a
        RecursionError in the 16:00 UTC run rather than in a test. Not
        reachable from `compute_shape_signature`'s own output today, which is
        exactly why it needs pinning: it would be introduced by adding a new
        `_kind` there and forgetting `_MERGEABLE_KINDS`.
        """
        a = {"_kind": "some_future_kind", "x": 1}
        b = {"_kind": "some_future_kind", "x": 2}
        merged = _merge_signatures(a, b)
        assert merged["_kind"] == "conflict"
        assert len(merged["shapes"]) == 2
        assert _merge_signatures(a, b) == _merge_signatures(b, a)

    def test_conflict_node_absorbs_a_bare_none_member(self):
        """`None` must be filtered by `_conflict_node`, not just the `"null"`
        tag — they arrive disguised as each other.

        `json.dumps(None)` is the string `"null"`, and `json.loads` turns it
        back into `None`, so a `!= "null"` test lets it through to
        `sorted(scalars)`, which raises TypeError comparing None to str.
        `_merge_signatures` absorbs `None` before the conflict path can be
        reached, so this is about the two functions agreeing on what `None`
        means rather than a reachable crash.
        """
        assert _conflict_node(None, "str") == "str"
        assert _conflict_node(None, None) == "null"

    def test_max_depth_sentinel_propagates(self):
        assert _merge_signatures("max_depth_exceeded", "float") == \
            "max_depth_exceeded"
        assert _merge_signatures("float", "max_depth_exceeded") == \
            "max_depth_exceeded"


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

    @staticmethod
    def _multi_record_payload(members, sparse_first=()):
        """Buurt envelope where each member carries TWO records.

        Members named in `sparse_first` have an incomplete FIRST record — the
        2026-08-23 partial-availability shape, one level down from the feed.
        """
        complete = {"temperature_2m": 18.4, "cloud_cover": 55.0}
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
                    "2026-08-23T00:00:00+02:00": (
                        {"temperature_2m": 18.4} if name in sparse_first
                        else dict(complete)
                    ),
                    "2026-08-23T01:00:00+02:00": dict(complete),
                }
                for name in members
            },
        }

    def test_member_with_sparse_first_record_stays_homogeneous(self):
        """A member whose FIRST record is incomplete must still read as the
        same shape as its peers, so a dropout alongside it still downgrades.

        Under first-record sampling the sparse member looked like a different
        shape, homogeneity failed, and the dropout was enforced — the
        2026-08-23 defect reproduced one level down.
        """
        prev = compute_shape_signature(
            self._multi_record_payload(["Elsweide_Arnhem_NL",
                                        "Elderveld_Arnhem_NL"])
        )
        curr = compute_shape_signature(
            self._multi_record_payload(["Elderveld_Arnhem_NL"],
                                       sparse_first=("Elderveld_Arnhem_NL",))
        )
        verdict = classify_data_member_drift(prev, curr)
        assert verdict is not None
        assert verdict["removed"] == ["Elsweide_Arnhem_NL"]

    def test_member_losing_field_from_every_record_still_enforced(self):
        """The union must not launder a real break: when a field leaves EVERY
        record of a member it leaves the union too, so the shape genuinely
        differs and the tripwire keeps enforcing."""
        prev = compute_shape_signature(
            self._multi_record_payload(["Elsweide_Arnhem_NL",
                                        "Elderveld_Arnhem_NL"])
        )
        degraded = self._multi_record_payload(["Elderveld_Arnhem_NL"])
        for records in degraded["data"].values():
            for record in records.values():
                record.pop("cloud_cover")
        assert classify_data_member_drift(
            prev, compute_shape_signature(degraded)
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
