"""
Tests for `scripts/report_span_shortfall.py` — the #51 CI reporter.

Drives the CLI via subprocess so the exit code and the Actions annotations are
exercised exactly as CI will see them.

The load-bearing assertion is that it ALWAYS exits 0. It is an alarm, not a
gate: the drift tripwire already withholds all 20 feeds when it fires, that
cost five consecutive publishes on 2026-08-31..09-03, and a second blocking
gate would cost more availability than this detection is worth.

File: tests/unit/test_report_span_shortfall.py
Created: 2026-09-04
"""

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = REPO_ROOT / "scripts" / "report_span_shortfall.py"


def _run(report_path, github_output=None):
    env = {"PATH": "/usr/bin:/bin"}
    if github_output:
        env["GITHUB_OUTPUT"] = str(github_output)
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--report", str(report_path)],
        cwd=REPO_ROOT, capture_output=True, text=True, env=env,
    )


def _outputs(path):
    if not Path(path).is_file():
        return {}
    out = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


class TestNeverGates:
    """Every path exits 0. If one of these fails, the reporter has become a gate."""

    def test_clean_exits_0(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text("[]")
        assert _run(r).returncode == 0

    def test_shortfalls_exit_0(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text(json.dumps([
            {"feed": "load_forecast.json", "member": "DE_LU",
             "observed": 1, "expected": 2, "ratio": 0.5}]))
        assert _run(r).returncode == 0

    def test_missing_report_exits_0(self, tmp_path):
        assert _run(tmp_path / "absent.json").returncode == 0

    def test_malformed_report_exits_0(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text("{not json")
        assert _run(r).returncode == 0

    def test_wrong_type_exits_0(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text('{"a": 1}')
        assert _run(r).returncode == 0


class TestNotCheckedIsNotClean:
    """The distinction that stops a broken reporter reading as a healthy run."""

    def test_missing_report_says_not_checked(self, tmp_path):
        out = tmp_path / "gh_out"
        res = _run(tmp_path / "absent.json", github_output=out)
        assert "NOT checked" in res.stdout
        assert "not a clean result" in res.stdout
        assert _outputs(out)["checked"] == "false"

    def test_malformed_report_says_not_checked(self, tmp_path):
        out = tmp_path / "gh_out"
        r = tmp_path / "s.json"
        r.write_text("{nope")
        res = _run(r, github_output=out)
        assert "NOT checked" in res.stdout
        assert _outputs(out)["checked"] == "false"

    def test_clean_run_says_checked(self, tmp_path):
        out = tmp_path / "gh_out"
        r = tmp_path / "s.json"
        r.write_text("[]")
        _run(r, github_output=out)
        o = _outputs(out)
        assert o["checked"] == "true" and o["shortfalls"] == "0"


class TestAnnotationsAndOutputs:
    def test_clean_emits_notice_not_error(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text("[]")
        res = _run(r)
        assert "::notice::" in res.stdout
        assert "::error::" not in res.stdout

    def test_shortfall_emits_error_and_per_member_warnings(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text(json.dumps([
            {"feed": "load_forecast.json", "member": "DE_LU",
             "observed": 1, "expected": 2, "ratio": 0.5},
            {"feed": "wind_forecast.json", "member": "offshore/x",
             "observed": 3, "expected": 10, "ratio": 0.3}]))
        res = _run(r)
        assert "::error::2 member(s) are short" in res.stdout
        assert "load_forecast.json:DE_LU carries 1 day(s), usually 2" in res.stdout
        assert "wind_forecast.json:offshore/x carries 3 day(s), usually 10" in res.stdout

    def test_worst_first(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text(json.dumps([
            {"feed": "a.json", "member": "x", "observed": 9,
             "expected": 10, "ratio": 0.9},
            {"feed": "b.json", "member": "y", "observed": 1,
             "expected": 10, "ratio": 0.1}]))
        res = _run(r)
        assert res.stdout.index("b.json:y") < res.stdout.index("a.json:x")

    def test_summary_output_set(self, tmp_path):
        out = tmp_path / "gh_out"
        r = tmp_path / "s.json"
        r.write_text(json.dumps([
            {"feed": "load_forecast.json", "member": "DE_LU",
             "observed": 1, "expected": 2, "ratio": 0.5}]))
        _run(r, github_output=out)
        o = _outputs(out)
        assert o["shortfalls"] == "1"
        assert o["summary"] == "load_forecast.json:DE_LU 1d/2d"

    def test_root_member_labelled(self, tmp_path):
        r = tmp_path / "s.json"
        r.write_text(json.dumps([
            {"feed": "calendar_features.json", "member": "",
             "observed": 3, "expected": 10, "ratio": 0.3}]))
        res = _run(r)
        assert "calendar_features.json:(root)" in res.stdout
