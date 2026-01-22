# CI/CD Setup Guide

## Overview

The Energy Data Hub project uses GitHub Actions for Continuous Integration and Continuous Deployment (CI/CD). This ensures code quality and prevents regressions.

## Workflows

### Test Workflow (`.github/workflows/test.yml`)

**Triggers**:
- Push to `main` or `dev` branches
- Pull requests to `main` or `dev`
- Manual workflow dispatch

**Jobs**:

#### 1. Test Job
Runs on multiple Python versions (3.11, 3.12, 3.13) to ensure compatibility.

**Steps**:
1. **Checkout code** - Gets the latest code
2. **Setup Python** - Installs Python with pip caching
3. **Install dependencies** - Installs from `requirements.txt`
4. **Run tests with coverage** - Executes all 40+ tests
5. **Upload coverage** - Sends coverage report to Codecov (Python 3.13 only)
6. **Check malformed timestamps** - Ensures no +00:09 or +00:18 in code
7. **Run critical tests** - Re-runs critical timezone tests for extra verification

#### 2. Code Quality Job
Validates code structure and syntax.

**Steps**:
1. **Check syntax errors** - Compiles all Python files
2. **Validate structure** - Ensures required files exist

## Status Badges

Add these to your README.md:

```markdown
![Tests](https://github.com/ducroq/energydatahub/actions/workflows/test.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
![Coverage](https://img.shields.io/codecov/c/github/ducroq/energydatahub)
```

## Running Tests Locally

### All Tests
```bash
python -m pytest tests/ -v
```

### With Coverage
```bash
python -m pytest tests/ --cov=utils --cov=energy_data_fetchers --cov=weather_data_fetchers --cov-report=html
```

Then open `htmlcov/index.html` in your browser.

### Critical Tests Only
```bash
python -m pytest tests/ -v -m critical
```

### Specific Test File
```bash
python -m pytest tests/unit/test_timezone.py -v
```

## Coverage Reports

### Codecov Integration
Coverage reports are automatically uploaded to Codecov after successful test runs on Python 3.13.

**Setup**:
1. Go to https://codecov.io/
2. Sign in with GitHub
3. Enable the `energydatahub` repository
4. Add `CODECOV_TOKEN` to GitHub Secrets (if private repo)

### Local HTML Reports
After running tests with `--cov-report=html`, view detailed coverage:
```bash
# Open in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

## Test Markers

Use markers to run specific test categories:

```bash
# Unit tests only (fast)
python -m pytest -m unit

# Integration tests only
python -m pytest -m integration

# Critical tests (timezone fixes)
python -m pytest -m critical

# Timezone-related tests
python -m pytest -m timezone
```

## Continuous Deployment

### Current Workflow (`.github/workflows/collect-data.yml`)
- Runs daily at 16:00 UTC
- Collects energy and weather data
- Publishes to GitHub Pages

### Future Enhancement
Consider adding deployment step to test workflow:
```yaml
- name: Deploy to GitHub Pages (on main only)
  if: github.ref == 'refs/heads/main' && success()
  run: |
    # Deployment steps
```

## Debugging Failed Workflows

### View Workflow Runs
1. Go to https://github.com/ducroq/energydatahub/actions
2. Click on the failed workflow
3. Click on the failed job
4. Expand failed steps to see error messages

### Common Issues

#### Import Errors
**Problem**: `ModuleNotFoundError`
**Solution**: Ensure all dependencies are in `requirements.txt`

#### Test Failures
**Problem**: Tests pass locally but fail in CI
**Solution**:
- Check Python version differences
- Verify timezone handling (CI runs on UTC)
- Check for hardcoded paths

#### Coverage Threshold
**Problem**: `Coverage failure: total of XX is less than fail-under=25`
**Solution**:
- Increase test coverage
- Adjust threshold in `pytest.ini` (line 28)

## Best Practices

### Before Pushing Code
```bash
# 1. Run all tests locally
python -m pytest tests/ -v

# 2. Check coverage
python -m pytest tests/ --cov=utils --cov=energy_data_fetchers --cov-weather_data_fetchers

# 3. Run critical tests
python -m pytest -m critical

# 4. Check for common issues
python -m py_compile $(find . -name "*.py" -not -path "*/venv/*")
```

### Writing New Tests
1. Add tests to `tests/unit/` or `tests/integration/`
2. Use appropriate markers: `@pytest.mark.unit`, `@pytest.mark.critical`
3. Run tests locally before pushing
4. Ensure new code has >75% coverage

### Updating Dependencies
```bash
# 1. Update requirements.txt
pip freeze > requirements.txt

# 2. Test locally
pip install -r requirements.txt
python -m pytest

# 3. Push and verify CI passes
git push
```

## Workflow Configuration

### Customize Test Matrix
Edit `.github/workflows/test.yml`:
```yaml
strategy:
  matrix:
    python-version: ['3.11', '3.12', '3.13']  # Add/remove versions
    os: [ubuntu-latest]  # Add: windows-latest, macos-latest
```

### Adjust Coverage Threshold
Edit `pytest.ini`:
```ini
[pytest]
addopts =
    --cov-fail-under=25  # Increase as coverage improves
```

### Add More Quality Checks
Examples to add to code-quality job:
```yaml
- name: Check code formatting (black)
  run: |
    pip install black
    black --check .

- name: Lint with flake8
  run: |
    pip install flake8
    flake8 . --max-line-length=120
```

## Monitoring

### GitHub Actions Dashboard
- **URL**: https://github.com/ducroq/energydatahub/actions
- **View**: Recent workflow runs, pass/fail rates
- **Notifications**: Configure in Settings → Notifications

### Codecov Dashboard
- **URL**: https://codecov.io/gh/ducroq/energydatahub
- **View**: Coverage trends, file-by-file coverage
- **Alerts**: Configure coverage drop alerts

## Known Issues & Workarounds

### Timezone Bug in entsoe-py and tenneteu-py
**Problem**: Both libraries use lowercase timezone names (`europe/amsterdam`) which fail on Linux where `tzdata` is case-sensitive.

**Workaround**: The `collect-data.yml` workflow includes a post-install fix:
```yaml
- name: Fix timezone bugs in dependencies
  run: |
    SITE_PACKAGES=$(pip show entsoe-py | grep Location | cut -d' ' -f2)
    find "$SITE_PACKAGES/entsoe" "$SITE_PACKAGES/tenneteu" -name "*.py" \
      -exec sed -i "s/europe\/amsterdam/Europe\/Amsterdam/g" {} \;
```

### pandas TimeRange Compatibility
**Problem**: pandas 3.0+ returns `TimeRange` objects that break datetime comparisons.

**Workaround**: Pin pandas in `requirements.txt`:
```
pandas>=2.0.0,<3.0.0
```

### EnergyZero TimeRange Objects
**Problem**: The `energyzero` library uses custom `TimeRange` objects with `start_including` attribute (not `start`).

**Solution**: The `EnergyZeroCollector` handles both pandas and energyzero TimeRange variants.

---

## Troubleshooting

### Workflow Not Triggering
**Check**:
1. Workflow file in `.github/workflows/`
2. Correct YAML syntax (use YAML validator)
3. Branch protection rules not blocking

### Secrets Not Available
**Check**:
1. Secrets added in Settings → Secrets and variables → Actions
2. Secret names match workflow file exactly
3. Using correct context: `${{ secrets.SECRET_NAME }}`

### Tests Timeout
**Solution**:
Add timeout to job or steps:
```yaml
jobs:
  test:
    timeout-minutes: 30  # Job level
    steps:
      - name: Run tests
        timeout-minutes: 10  # Step level
```

## Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [pytest Documentation](https://docs.pytest.org/)
- [pytest-cov Plugin](https://pytest-cov.readthedocs.io/)
- [Codecov Documentation](https://docs.codecov.com/)

## Support

For issues with CI/CD:
1. Check workflow logs in GitHub Actions
2. Review this documentation
3. Check `docs/TEST_RESULTS.md` for test details
4. Raise an issue on GitHub

---

**Last Updated**: January 22, 2026
**Workflows**: 2 active (test.yml, collect-data.yml)
**Status**: ✅ Configured and operational
