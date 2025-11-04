# Google Weather API - Personal Setup (No Git Commit)

## ⚠️ Important: Protecting Your Personal API Key

Your Google Weather API key is linked to **your personal credit card**. You should **NOT** commit it to the git repository where colleagues can see it.

## Safe Setup Method

The system has been configured to load secrets in this priority order:

1. **`secrets.ini`** - Shared team secrets (committed to git)
2. **`secrets.local.ini`** - Your personal secrets (NOT committed - in `.gitignore`)
3. **Environment variables** - GitHub Actions / CI/CD

## Quick Setup Steps

### 1. Create Your Personal Secrets File

The file `secrets.local.ini` has been created for you and is already in `.gitignore`.

**Edit `secrets.local.ini`** and add your API key:

```ini
# Personal secrets file - NOT COMMITTED TO GIT
# This file is in .gitignore and will not be shared with colleagues
# Only used for local development on this machine

[api_keys]
google_weather = YOUR_ACTUAL_API_KEY_HERE
```

Replace `YOUR_ACTUAL_API_KEY_HERE` with your real API key.

### 2. Verify It's Not Tracked by Git

```bash
git status
```

You should **NOT** see `secrets.local.ini` in the output. It's in `.gitignore` so it's safe.

### 3. Test It

```bash
cd "C:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - WebBasedControl\01. Software\energyDataHub"
python test_google_weather.py
```

The logs will show:
```
INFO:root:Loading secrets from secrets.ini
INFO:root:Loading personal secrets from secrets.local.ini
```

This confirms it's loading your personal key.

## How It Works

### File Priority

```
secrets.ini (shared)         ← Loaded first
    ↓ (overrides)
secrets.local.ini (personal) ← Your personal keys override shared ones
    ↓ (overrides)
Environment variables        ← CI/CD overrides everything
```

### What's in Each File

**`secrets.ini` (committed to git):**
```ini
[api_keys]
entsoe = e764dd40-0108-4de7-81e7-b32f7fa55d69
openweather = 4852b00585c8d9000573557d530ac9e7
meteo = 7daf22bed0
google = AIzaSyB_zfr11b74KMsFzmOdR87MTZgyn2uf2EQ
# google_weather = (add to secrets.local.ini for local dev)
```

**`secrets.local.ini` (NOT committed - your machine only):**
```ini
[api_keys]
google_weather = YOUR_PERSONAL_KEY
```

## For Your Colleagues

Your colleagues should:

1. **NOT use Google Weather locally** (if they don't have billing)
2. **OR** create their own `secrets.local.ini` with their own API key
3. **GitHub Actions will work** (uses `GOOGLE_WEATHER_API_KEY` secret you added)

### Instructions for Colleagues

Add this to the project README or share with team:

```markdown
## Local Development with Google Weather API

If you want to test Google Weather API locally:

1. Get your own Google Cloud API key with Weather API enabled
2. Create `secrets.local.ini` (this file is gitignored)
3. Add your key:
   ```ini
   [api_keys]
   google_weather = YOUR_KEY
   ```

**Note:** The API requires billing enabled. If you don't have billing,
the GitHub Actions workflow will still work (uses project secret).
```

## GitHub Actions (Already Set Up)

You already added `GOOGLE_WEATHER_API_KEY` to GitHub secrets, so:

- ✅ **Local dev**: Uses your `secrets.local.ini`
- ✅ **GitHub Actions**: Uses the secret you added
- ✅ **Your colleagues**: Can either skip Google Weather locally or add their own key

## Verify Safety

**Check what will be committed:**

```bash
git status
```

Should show:
- ✅ `secrets.ini` - modified (safe, no personal key)
- ✅ `.gitignore` - modified (safe, adds secrets.local.ini)
- ✅ `utils/helpers.py` - modified (safe, code changes)
- ❌ **NOT** `secrets.local.ini` (this should be invisible)

**Double-check gitignore:**

```bash
cat .gitignore | grep secrets
```

Should show:
```
secrets.ini
secrets.local.ini
```

## Summary

✅ **Your personal API key**: In `secrets.local.ini` (gitignored)
✅ **Team shared secrets**: In `secrets.ini` (committed, but no personal key)
✅ **GitHub Actions**: Uses secret you added to repository
✅ **Colleagues**: Won't see your personal key
✅ **Safe to commit**: All changes are safe now

---

**Next Steps:**
1. Add your API key to `secrets.local.ini`
2. Run `python test_google_weather.py` to test
3. Commit your changes (everything is safe now)
