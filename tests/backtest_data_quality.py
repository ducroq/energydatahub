"""
Backtest Data Quality Framework Against Historical Data
---------------------------------------------------------
Scans all historical JSON files in the data/ directory through the
data quality validation framework to:

1. Validate the framework works on real data (not just test fixtures)
2. Surface actual quality issues in our historical archive
3. Identify patterns (e.g., recurring failures on certain days/types)
4. Detect schema version distribution across files

Usage:
    python tests/backtest_data_quality.py
    python tests/backtest_data_quality.py --verbose
    python tests/backtest_data_quality.py --type energy_price_forecast
"""

import os
import sys
import json
import base64
import argparse
from collections import defaultdict
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils.data_quality import (
    validate_dataset,
    validate_value_ranges,
    validate_completeness,
    validate_null_ratio,
    validate_staleness,
    validate_duplicate_timestamps,
    Severity,
    DatasetQualityReport,
)
from utils.schema_registry import detect_version, read_json_file
from utils.data_types import EnhancedDataSet
from utils.helpers import load_secrets
from utils.secure_data_handler import SecureDataHandler


DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


def get_handler():
    """Load encryption keys and create SecureDataHandler."""
    config = load_secrets(PROJECT_ROOT)
    encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
    hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
    return SecureDataHandler(encryption_key, hmac_key)


def extract_file_type(filename: str) -> str:
    """Extract the file type from a timestamped filename."""
    # Remove timestamp prefix (YYMMDD_HHMMSS_)
    parts = filename.split('_', 2)
    if len(parts) >= 3 and parts[0].isdigit() and parts[1].isdigit():
        return parts[2]
    return filename


def extract_date(filename: str) -> str:
    """Extract date string from filename."""
    parts = filename.split('_')
    if parts and parts[0].isdigit() and len(parts[0]) == 6:
        yy, mm, dd = parts[0][:2], parts[0][2:4], parts[0][4:6]
        return f"20{yy}-{mm}-{dd}"
    return 'unknown'


def load_and_validate_file(filepath: str, filename: str, handler=None, skip_staleness: bool = True):
    """
    Load a JSON file (possibly encrypted) and run quality checks.

    Args:
        filepath: Full path to JSON file
        filename: Just the filename
        handler: SecureDataHandler for decryption (None = plaintext)
        skip_staleness: Skip staleness check for historical data (default True)

    Returns:
        (version, file_type, report_or_none, error_or_none)
    """
    file_type = extract_file_type(filename)

    try:
        with open(filepath, 'r') as f:
            content = f.read()

        # Try parsing as JSON first (plaintext)
        try:
            raw_data = json.loads(content)
        except json.JSONDecodeError:
            # Likely encrypted — try decryption
            if handler is None:
                return None, file_type, None, "Encrypted file but no handler provided"
            try:
                raw_data = handler.decrypt_and_verify(content)
            except Exception as e:
                return None, file_type, None, f"Decryption failed: {e}"
    except Exception as e:
        return None, file_type, None, f"Read error: {e}"

    # Detect schema version
    version = detect_version(raw_data)

    # Extract datasets for validation
    reports = []

    if version >= '2.0':
        # v2.0+ format: {version, dataset_name: {metadata, data}}
        for key, value in raw_data.items():
            if key == 'version':
                continue
            if isinstance(value, dict) and 'metadata' in value and 'data' in value:
                try:
                    ds = EnhancedDataSet(
                        metadata=value['metadata'],
                        data=value['data'],
                    )
                    report = validate_dataset(ds, key)

                    # Remove staleness issues for historical data
                    if skip_staleness:
                        report.issues = [
                            i for i in report.issues
                            if i.check_name != 'staleness'
                        ]
                        report.checks_failed = sum(1 for _ in report.issues)
                        report.checks_passed = (report.checks_passed + report.checks_failed) - report.checks_failed

                    reports.append(report)
                except Exception as e:
                    reports.append(None)
    else:
        # v1.0 format: try to validate raw data directly
        try:
            data_type = 'unknown'
            if 'energy_price' in file_type:
                data_type = 'energy_price'
            elif 'weather' in file_type:
                data_type = 'weather'

            ds = EnhancedDataSet(
                metadata={'data_type': data_type, 'source': 'v1.0'},
                data=raw_data if isinstance(raw_data, dict) else {},
            )
            report = validate_dataset(ds, file_type.replace('.json', ''))
            if skip_staleness:
                report.issues = [i for i in report.issues if i.check_name != 'staleness']
            reports.append(report)
        except Exception as e:
            return version, file_type, None, f"Validation error: {e}"

    return version, file_type, reports, None


def main():
    parser = argparse.ArgumentParser(description='Backtest data quality on historical files')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show per-file details')
    parser.add_argument('--type', '-t', type=str, help='Filter by file type (e.g., energy_price_forecast)')
    parser.add_argument('--limit', '-l', type=int, default=0, help='Limit number of files to process')
    parser.add_argument('--issues-only', action='store_true', help='Only show files with issues')
    args = parser.parse_args()

    # Collect all JSON files
    files = sorted([
        f for f in os.listdir(DATA_DIR)
        if f.endswith('.json') and f[0].isdigit()  # Only timestamped files
    ])

    if args.type:
        files = [f for f in files if args.type in f]

    if args.limit > 0:
        files = files[:args.limit]

    # Load encryption handler
    try:
        handler = get_handler()
        print("Encryption keys loaded successfully.")
    except Exception as e:
        handler = None
        print(f"Warning: Could not load encryption keys ({e}). Only plaintext files will be processed.")

    print(f"Backtesting data quality on {len(files)} historical files...\n")

    # Tracking
    version_counts = defaultdict(int)
    file_type_stats = defaultdict(lambda: {
        'total': 0, 'clean': 0, 'with_issues': 0, 'errors': 0,
        'issues_by_check': defaultdict(int),
        'issues_by_severity': defaultdict(int),
    })
    total_issues = 0
    parse_errors = 0
    issue_examples = defaultdict(list)  # check_name -> [(file, message)]

    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        file_type = extract_file_type(filename)
        file_date = extract_date(filename)

        version, ftype, reports, error = load_and_validate_file(filepath, filename, handler=handler)

        stats = file_type_stats[ftype.replace('.json', '')]
        stats['total'] += 1

        if version:
            version_counts[version] += 1

        if error:
            parse_errors += 1
            stats['errors'] += 1
            if args.verbose:
                print(f"  ERROR {filename}: {error}")
            continue

        if reports is None:
            continue

        file_has_issues = False
        for report in reports:
            if report is None:
                continue
            if report.issues:
                file_has_issues = True
                for issue in report.issues:
                    total_issues += 1
                    stats['issues_by_check'][issue.check_name] += 1
                    stats['issues_by_severity'][issue.severity.value] += 1

                    # Keep a few examples per check type
                    key = f"{issue.check_name}:{ftype}"
                    if len(issue_examples[key]) < 3:
                        issue_examples[key].append((filename, issue.message))

                    if args.verbose:
                        sev = issue.severity.value.upper()
                        print(f"  [{sev}] {filename} / {report.dataset_name}: "
                              f"{issue.check_name} - {issue.message}")

        if file_has_issues:
            stats['with_issues'] += 1
        else:
            stats['clean'] += 1
            if args.verbose and not args.issues_only:
                print(f"  OK {filename}")

    # --- Summary Report ---
    print("\n" + "=" * 70)
    print("BACKTEST DATA QUALITY REPORT")
    print("=" * 70)

    print(f"\nFiles scanned:    {len(files)}")
    print(f"Parse errors:     {parse_errors}")
    print(f"Total issues:     {total_issues}")

    print(f"\n--- Schema Version Distribution ---")
    for ver, count in sorted(version_counts.items()):
        pct = count / len(files) * 100
        print(f"  v{ver}: {count} files ({pct:.1f}%)")

    print(f"\n--- Quality by File Type ---")
    print(f"{'File Type':<40} {'Total':>6} {'Clean':>6} {'Issues':>7} {'Errors':>7}")
    print("-" * 70)
    for ftype in sorted(file_type_stats.keys()):
        s = file_type_stats[ftype]
        print(f"{ftype:<40} {s['total']:>6} {s['clean']:>6} "
              f"{s['with_issues']:>7} {s['errors']:>7}")

    print(f"\n--- Issue Breakdown by Check Type ---")
    all_check_counts = defaultdict(int)
    all_severity_counts = defaultdict(int)
    for ftype, s in file_type_stats.items():
        for check, count in s['issues_by_check'].items():
            all_check_counts[check] += count
        for sev, count in s['issues_by_severity'].items():
            all_severity_counts[sev] += count

    for check, count in sorted(all_check_counts.items(), key=lambda x: -x[1]):
        print(f"  {check}: {count}")

    print(f"\n--- Issue Breakdown by Severity ---")
    for sev in ['critical', 'error', 'warning', 'info']:
        if sev in all_severity_counts:
            print(f"  {sev.upper()}: {all_severity_counts[sev]}")

    if issue_examples:
        print(f"\n--- Example Issues (first 3 per type) ---")
        for key in sorted(issue_examples.keys()):
            print(f"\n  [{key}]")
            for filename, message in issue_examples[key]:
                print(f"    {filename}: {message}")

    # Write machine-readable report
    report_path = os.path.join(DATA_DIR, 'backtest_quality_report.json')
    report_data = {
        'timestamp': datetime.now().isoformat(),
        'files_scanned': len(files),
        'parse_errors': parse_errors,
        'total_issues': total_issues,
        'schema_versions': dict(version_counts),
        'by_file_type': {
            ftype: {
                'total': s['total'],
                'clean': s['clean'],
                'with_issues': s['with_issues'],
                'errors': s['errors'],
                'issues_by_check': dict(s['issues_by_check']),
                'issues_by_severity': dict(s['issues_by_severity']),
            }
            for ftype, s in file_type_stats.items()
        },
        'issue_examples': {
            key: [(f, m) for f, m in examples]
            for key, examples in issue_examples.items()
        },
    }
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=2)
    print(f"\nFull report saved to: {report_path}")


if __name__ == '__main__':
    main()
