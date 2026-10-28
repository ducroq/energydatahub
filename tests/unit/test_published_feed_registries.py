"""The published-feed registries must agree (#85).

Adding a published feed means touching several hard-coded lists in lock-step:
the save block, the copy to the current filename and the `published_feeds`
sidecar in `data_fetcher.main`, the `quality_datasets` map, `EXPECTED_DATA_TYPE` in `utils/data_quality.py`, and
two lists in `.github/workflows/collect-data.yml`. Missing one fails silently:
the file is collected but never reaches `docs/`, or skips the drift sidecar or
the FMEA gate (c40a53b, #3). This test replaces the prose checklist for every
touchpoint it can read.

Python is parsed with `ast`, never grepped: a substring grep false-passed on a
collision with the `quality_datasets` map on 2026-09-10. The workflow lists are
regexes over one named step each; a renamed step fails loudly.

Not covered: `DATASET_MISSING_SEVERITY`, which lacks 9 published feeds (a
missing one never reaches the quality report), and opt-in lists such as
`CRITICAL_FEEDS` and `scripts/backfill_missed_run.py`'s `FEED_FILES`.
"""
import ast
import re
from pathlib import Path

import yaml

from utils.data_quality import EXPECTED_DATA_TYPE

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github' / 'workflows' / 'collect-data.yml'

# Published files with no `quality_datasets` entry of the same name, and why.
NOT_QUALITY_CHECKED_UNDER_OWN_NAME = {
    'energy_price_forecast': 'combined; its sources are checked as entsoe, epex, ...',
    'calendar_features': 'derived locally from the calendar, no upstream to fail',
}


def _main_function():
    tree = ast.parse((ROOT / 'data_fetcher.py').read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == 'main':
            return node
    raise AssertionError('data_fetcher.main not found')


def _saved_stems(main):
    """Stems of every `f'{timestamp}_<stem>.json'` path the save blocks write."""
    stems = set()
    for node in ast.walk(main):
        if isinstance(node, ast.JoinedStr) and node.values:
            last = node.values[-1]
            if isinstance(last, ast.Constant) and isinstance(last.value, str):
                m = re.fullmatch(r'_(\w+)\.json', last.value)
                if m:
                    stems.add(m.group(1))
    return stems


def _current_copy_stems(main):
    """Stems of every `shutil.copy(src, os.path.join(output_path, '<stem>.json'))`.

    The docs step copies `data/<stem>.json`, so a feed saved under its dated name
    but never copied to this one is silently skipped. The target may be a name
    bound earlier to such a join (`market_history_file`).
    """
    def join_constant(node):
        if (isinstance(node, ast.Call) and ast.unparse(node.func) == 'os.path.join'
                and isinstance(node.args[-1], ast.Constant)):
            return node.args[-1].value
        return None

    bound = {}
    for node in ast.walk(main):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                and isinstance(node.targets[0], ast.Name):
            value = join_constant(node.value)
            if value is not None:
                bound[node.targets[0].id] = value
    stems = set()
    for node in ast.walk(main):
        if isinstance(node, ast.Call) and ast.unparse(node.func) == 'shutil.copy':
            target = node.args[1]
            name = bound.get(target.id) if isinstance(target, ast.Name) else join_constant(target)
            if name and name.endswith('.json'):
                stems.add(name.removesuffix('.json'))
    return stems


def _published_feed_stems(main):
    """Keys of the `published_feeds` sidecar: direct assignments plus the loop list."""
    stems = set()
    for node in ast.walk(main):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (isinstance(target, ast.Subscript)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == 'published_feeds'
                        and isinstance(target.slice, ast.Constant)):
                    stems.add(target.slice.value.removesuffix('.json'))
        if isinstance(node, ast.For) and isinstance(node.iter, ast.List):
            body_src = ''.join(ast.unparse(stmt) for stmt in node.body)
            if 'published_feeds[' not in body_src:
                continue
            for elt in node.iter.elts:
                if isinstance(elt, ast.Tuple) and isinstance(elt.elts[0], ast.Constant):
                    stems.add(elt.elts[0].value.removesuffix('.json'))
    return stems


def _quality_dataset_keys(main):
    for node in ast.walk(main):
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == 'quality_datasets' for t in node.targets)
                and isinstance(node.value, ast.Dict)):
            return {k.value for k in node.value.keys}
    raise AssertionError('quality_datasets dict literal not found in data_fetcher.main')


def _workflow_step_script(name):
    workflow = yaml.safe_load(WORKFLOW.read_text())
    for job in workflow['jobs'].values():
        for step in job.get('steps', []):
            if step.get('name') == name:
                return step['run']
    raise AssertionError(f'workflow step {name!r} not found')


def _expected_output_stems():
    script = _workflow_step_script('Verify expected output files')
    m = re.search(r'expected=\(([^)]*)\)', script)
    assert m, 'expected=( ... ) array not found'
    return set(m.group(1).split())


def _docs_copy_stems():
    script = _workflow_step_script('Prepare docs folder with only current data')
    m = re.search(r'for file in ([^;]*);', script)
    assert m, 'for file in ...; loop not found'
    return {f.removesuffix('.json') for f in m.group(1).split()}


def test_publish_lists_agree():
    main = _main_function()
    lists = {
        'data_fetcher save blocks': _saved_stems(main),
        'data_fetcher copy to current filename': _current_copy_stems(main),
        'data_fetcher published_feeds sidecar': _published_feed_stems(main),
        'workflow: Verify expected output files': _expected_output_stems(),
        'workflow: Prepare docs folder': _docs_copy_stems(),
    }
    # Denominator first: an empty parse would otherwise agree with an empty parse.
    # A floor, not today's count, so retiring a feed does not turn this red.
    for where, stems in lists.items():
        assert len(stems) >= 10, f'{where}: parsed only {len(stems)} feeds: {sorted(stems)}'
    union = set().union(*lists.values())
    gaps = {where: sorted(union - stems) for where, stems in lists.items() if union - stems}
    assert not gaps, f'feeds missing from some publish lists: {gaps}'


def test_every_published_feed_is_quality_checked():
    main = _main_function()
    published = _published_feed_stems(main)
    quality = _quality_dataset_keys(main)
    unchecked = published - quality - set(NOT_QUALITY_CHECKED_UNDER_OWN_NAME)
    assert not unchecked, f'published feeds with no quality_datasets entry: {sorted(unchecked)}'
    stale = set(NOT_QUALITY_CHECKED_UNDER_OWN_NAME) - published
    assert not stale, f'exemptions for feeds no longer published: {sorted(stale)}'


def test_every_quality_dataset_is_type_pinned():
    quality = _quality_dataset_keys(_main_function())
    assert len(quality) >= 10, f'parsed only {len(quality)} quality datasets'
    unpinned = quality - set(EXPECTED_DATA_TYPE)
    assert not unpinned, (
        f'quality datasets lacking an EXPECTED_DATA_TYPE pin (CWE-20): {sorted(unpinned)}'
    )
