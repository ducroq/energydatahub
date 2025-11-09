# Energy Dashboard Setup & Maintenance Guide

This document explains how the Energy Dashboard at **https://energy.jeroenveen.nl** works and how to maintain it.

## 📋 Table of Contents
- [Overview](#overview)
- [How It Works](#how-it-works)
- [Dashboard URLs](#dashboard-urls)
- [Automatic Updates](#automatic-updates)
- [Manual Updates](#manual-updates)
- [Troubleshooting](#troubleshooting)
- [Configuration](#configuration)

---

## Overview

The Energy Dashboard is a live web application that displays energy price forecasts from multiple sources. It automatically updates daily with fresh data from this energyDataHub project.

### Key Components

1. **energyDataHub** (this repository)
   - Collects data from 7+ APIs daily at 16:00 UTC
   - Encrypts and publishes to GitHub Pages
   - Triggers dashboard rebuild via webhook

2. **energyDataDashboard** (separate repository)
   - Hosted on Netlify at https://energy.jeroenveen.nl
   - Fetches encrypted data from energyDataHub
   - Decrypts and displays interactive charts

---

## How It Works

### Daily Automatic Process

```
┌─────────────────────────────────────────────────────────────┐
│  1. energyDataHub GitHub Actions (16:00 UTC daily)          │
│     - Collects 48-hour forecast (full today + full tomorrow)│
│     - Encrypts with AES-256-CBC                             │
│     - Publishes to GitHub Pages                             │
│     - Triggers Netlify webhook                              │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Netlify Rebuild (triggered automatically)               │
│     - Fetches encrypted data from GitHub Pages             │
│     - Decrypts using stored encryption keys                 │
│     - Rebuilds Hugo static site                             │
│     - Deploys to https://energy.jeroenveen.nl              │
└─────────────────────────────────────────────────────────────┘
```

### Data Collection Window

**Changed on: October 26, 2025**

- **Old behavior**: Collected rolling 24 hours from "now" to "now + 24h"
  - Example: Running at 17:15 → collected until 17:15 next day
  - Result: Only ~1 hour of tomorrow's data

- **New behavior**: Collects full day boundaries
  - **Start**: 00:00 today
  - **End**: 23:59:59 tomorrow
  - Result: **Full 48 hours** of forecast data
  - Example data points: 196 (ENTSO-E), 47-48 (other sources)

---

## Dashboard URLs

### Live Production Site
- **URL**: https://energy.jeroenveen.nl
- **Hosted on**: Netlify
- **Repository**: https://github.com/ducroq/energyDataDashboard
- **Auto-updates**: Daily at 16:00 UTC (via webhook)

### Data Source (Backend)
- **URL**: https://ducroq.github.io/energydatahub/
- **Repository**: https://github.com/ducroq/energydatahub (this repo)
- **Endpoints**:
  - `energy_price_forecast.json` (encrypted)
  - `weather_forecast.json` (encrypted)
  - `sun_forecast.json` (encrypted)
  - `air_history.json` (encrypted)

---

## Automatic Updates

### How Automatic Updates Work

The dashboard automatically updates daily through a webhook system:

1. **energyDataHub workflow runs** (`.github/workflows/collect-data.yml`)
   - Scheduled: Daily at 16:00 UTC (18:00 CET)
   - Can also be triggered manually

2. **Workflow publishes data** to GitHub Pages

3. **Workflow triggers Netlify webhook**
   ```yaml
   - name: Trigger dashboard rebuild
     run: |
       curl -X POST -d {} ${{ secrets.NETLIFY_BUILD_HOOK }}
   ```

4. **Netlify automatically rebuilds** the dashboard with fresh data

### Configuration (Already Set Up)

#### GitHub Secret: `NETLIFY_BUILD_HOOK`
- **Value**: `https://api.netlify.com/build_hooks/6862ddc9a0da969b6e383d94`
- **Location**: energydatahub repo → Settings → Secrets and variables → Actions
- **Set on**: October 26, 2025

#### Netlify Environment Variables
In Netlify dashboard → Site settings → Environment variables:
- `ENCRYPTION_KEY_B64`: Base64-encoded encryption key (32 bytes)
- `HMAC_KEY_B64`: Base64-encoded HMAC key (32 bytes)

*These must match the keys in energyDataHub's `secrets.ini` file*

---

## Manual Updates

### Trigger Dashboard Rebuild Manually

If you need to update the dashboard outside the scheduled time:

#### Option 1: Via Netlify Dashboard (Easiest)
1. Go to https://app.netlify.com
2. Select site "energydatadashboard"
3. Go to **Deploys** tab
4. Click **Trigger deploy** → **Deploy site**

For fresh data without cache:
- Click **Trigger deploy** → **Clear cache and deploy site**

#### Option 2: Via energyDataHub Workflow
1. Go to https://github.com/ducroq/energydatahub/actions
2. Select "Collect and Publish Data" workflow
3. Click **Run workflow** → **Run workflow**
4. This will collect fresh data AND trigger dashboard rebuild

#### Option 3: Via GitHub CLI
```bash
cd /c/Users/scbry/HAN/HAN\ H2\ LAB\ IPKW\ -\ Projects\ -\ WebBasedControl/01.\ Software/energyDataHub
gh workflow run "collect-data.yml"
```

---

## Troubleshooting

### Dashboard Shows "Loading..." Forever

**Cause**: Data files not properly decrypted or not present

**Solution**:
1. Go to Netlify → Deploys → Latest deploy
2. Check build log for:
   - "Successfully decrypted and saved energy data"
   - "Data contains X records"
3. If missing, **Trigger deploy** → **Clear cache and deploy site**
4. Verify environment variables are set correctly

### Dashboard Shows Old Data

**Cause**: Netlify is using cached data

**Solution**:
1. Check when energyDataHub last ran:
   - Go to https://github.com/ducroq/energydatahub/actions
   - Should run daily at 16:00 UTC
2. Clear cache: **Trigger deploy** → **Clear cache and deploy site**

### Webhook Not Triggering

**Cause**: GitHub secret missing or incorrect

**Solution**:
1. Verify secret exists:
   ```bash
   cd energyDataHub
   gh secret list -R ducroq/energydatahub | grep NETLIFY
   ```
   Should show: `NETLIFY_BUILD_HOOK`

2. If missing, recreate it:
   ```bash
   echo "https://api.netlify.com/build_hooks/6862ddc9a0da969b6e383d94" | gh secret set NETLIFY_BUILD_HOOK -R ducroq/energydatahub
   ```

3. Verify workflow has the trigger step:
   - Check `.github/workflows/collect-data.yml`
   - Should have "Trigger dashboard rebuild" step

### Build Fails on Netlify

**Cause**: Missing or incorrect encryption keys

**Solution**:
1. Check environment variables in Netlify:
   - Site settings → Environment variables
   - Verify `ENCRYPTION_KEY_B64` and `HMAC_KEY_B64` are set

2. Keys must match `secrets.ini` in energyDataHub:
   ```ini
   [security_keys]
   encryption = <base64_key>
   hmac = <base64_key>
   ```

3. If keys don't match, update Netlify environment variables

---

## Configuration

### Netlify Build Settings

Configured in `netlify.toml` (in energyDataDashboard repo):

```toml
[build]
  command = """
    pip install cryptography &&
    python decrypt_data_cached.py &&
    hugo --minify
  """
  publish = "public"

[build.environment]
  HUGO_VERSION = "0.124.0"
  PYTHON_VERSION = "3.11"
```

### Data Caching

The dashboard uses intelligent caching:
- **Cache duration**: 24 hours
- **Cache validation**: Checks if remote data hash changed
- **Benefits**:
  - Faster builds (~25s vs ~50s)
  - Reduced API calls to GitHub Pages
  - Automatic refresh when data changes

To bypass cache:
- Use "Clear cache and deploy site" in Netlify
- Or pass `--force` flag to `decrypt_data_cached.py`

### energyDataHub Workflow Schedule

In `.github/workflows/collect-data.yml`:

```yaml
on:
  schedule:
    - cron: '0 16 * * *'  # 16:00 UTC daily (18:00 CET)
  workflow_dispatch:       # Manual trigger allowed
```

---

## Local Development

### For energyDataDashboard (Local Testing)

If you want to run the dashboard locally with the latest data:

1. **Clone the dashboard repo** (if not already):
   ```bash
   cd /c/local_dev
   git clone https://github.com/ducroq/energyDataDashboard.git
   cd energyDataDashboard
   npm install
   pip install cryptography
   ```

2. **Fetch and decrypt latest data**:
   ```bash
   python refresh_dashboard_data.py
   ```

   This script:
   - Loads encryption keys from energyDataHub's `secrets.ini`
   - Fetches encrypted data from GitHub Pages
   - Decrypts and saves to `static/data/`

3. **Run Hugo development server**:
   ```bash
   npm run dev
   # or
   hugo server -D
   ```

4. **View locally**:
   - Open http://localhost:1313

### Manual Data Refresh (Local)

The `refresh_dashboard_data.py` script was created on October 26, 2025 to simplify local development:

```python
# Location: C:\local_dev\energyDataDashboard\refresh_dashboard_data.py
# Usage: python refresh_dashboard_data.py
#
# What it does:
# 1. Loads encryption keys from energyDataHub secrets.ini
# 2. Fetches latest data from GitHub Pages
# 3. Decrypts and saves to static/data/
# 4. Ready for Hugo to serve
```

---

## Important Files

### In energyDataHub (this repo)

| File | Purpose |
|------|---------|
| `.github/workflows/collect-data.yml` | Main workflow - collects data daily, triggers webhook |
| `data_fetcher.py` | Main script - collects 48-hour forecast |
| `secrets.ini` | Encryption keys (NOT in git) |
| `settings.ini` | Location settings |

### In energyDataDashboard

| File | Purpose |
|------|---------|
| `netlify.toml` | Netlify build configuration |
| `decrypt_data_cached.py` | Fetches and decrypts data during build |
| `refresh_dashboard_data.py` | Local development data refresh script |
| `hugo.toml` | Hugo site configuration |

---

## Change Log

### October 26, 2025
- **Fixed date range collection**: Now collects full 48 hours (00:00 today to 23:59:59 tomorrow)
- **Added Netlify webhook**: Dashboard auto-updates when energyDataHub publishes
- **Created `refresh_dashboard_data.py`**: Simplified local development
- **Verified live site**: https://energy.jeroenveen.nl working with new data

### Previous Setup
- Dashboard deployed to Netlify
- Encryption keys configured
- Custom domain configured: energy.jeroenveen.nl

---

## Support & Maintenance

### Regular Maintenance

**None required!** The system runs automatically.

### Monitoring

Check occasionally that:
1. energyDataHub workflow runs successfully (check GitHub Actions)
2. Netlify deployments succeed (check Netlify dashboard)
3. Dashboard shows current data (visit https://energy.jeroenveen.nl)

### When to Take Action

- **Dashboard shows old data**: Clear Netlify cache and redeploy
- **Build fails**: Check Netlify build logs, verify environment variables
- **No data visible**: Check browser console for JavaScript errors

---

## Quick Reference

### URLs
- **Live Dashboard**: https://energy.jeroenveen.nl
- **Data Source**: https://ducroq.github.io/energydatahub/
- **Netlify Admin**: https://app.netlify.com → energydatadashboard
- **GitHub Actions**: https://github.com/ducroq/energydatahub/actions

### Key Commands

```bash
# Trigger data collection manually
cd energyDataHub
gh workflow run "collect-data.yml"

# Check if webhook secret is set
gh secret list -R ducroq/energydatahub | grep NETLIFY

# Refresh local dashboard data
cd energyDataDashboard
python refresh_dashboard_data.py

# Run local development server
npm run dev
```

### Webhook URL
```
https://api.netlify.com/build_hooks/6862ddc9a0da969b6e383d94
```

---

## Questions?

If something doesn't work:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review the build logs in Netlify
3. Check GitHub Actions workflow logs
4. Verify environment variables are set correctly

**Remember**: The system is fully automated. You should rarely need to intervene!
