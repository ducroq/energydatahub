# Google Drive Archival Setup Guide

## Overview

The Energy Data Hub automatically archives all timestamped data files to Google Drive, creating a historical database while keeping the GitHub repository lightweight.

**Benefits**:
- ✅ Unlimited historical data storage on Google Drive
- ✅ Organized by year/month for easy retrieval
- ✅ Automatic archival after each data collection
- ✅ Local data rotation (keeps last 7 days in repo)
- ✅ Non-blocking (failures don't stop data collection)

## Architecture

```
Data Collection → Archive to Google Drive → Rotate Local Files → Publish Current

Daily @ 16:00 UTC:
1. Collect data from APIs
2. Save timestamped files: data/251025_161234_energy_price_forecast.json
3. Archive timestamped files to Google Drive (organized by YYYY/MM/)
4. Delete local files older than 7 days
5. Publish current files to GitHub Pages
```

### Google Drive Structure

```
energyDataHub/                    ← Root folder
├── 2025/
│   ├── 10/
│   │   ├── 251025_161234_energy_price_forecast.json
│   │   ├── 251025_161234_weather_forecast.json
│   │   ├── 251025_161234_sun_forecast.json
│   │   ├── 251025_161234_air_history.json
│   │   ├── 251026_161234_energy_price_forecast.json
│   │   └── ...
│   ├── 11/
│   │   └── ...
│   └── 12/
│       └── ...
└── 2026/
    └── ...
```

## Setup Instructions

### Step 1: Create Google Service Account

1. **Go to Google Cloud Console**
   - Visit: https://console.cloud.google.com/

2. **Create or Select a Project**
   - Click "Select a project" → "New Project"
   - Name: "Energy Data Hub" (or any name)
   - Click "Create"

3. **Enable Google Drive API**
   - Go to "APIs & Services" → "Library"
   - Search for "Google Drive API"
   - Click "Enable"

4. **Create Service Account**
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "Service Account"
   - Name: "energydatahub-archiver"
   - Description: "Service account for automated data archival"
   - Click "Create and Continue"
   - Skip optional steps (Grant access, Grant users access)
   - Click "Done"

5. **Create Service Account Key**
   - Click on the newly created service account
   - Go to "Keys" tab
   - Click "Add Key" → "Create new key"
   - Select "JSON"
   - Click "Create"
   - **Save the downloaded JSON file** (you'll need this)

### Step 2: Create Google Drive Folder

1. **Open Google Drive**
   - Go to: https://drive.google.com/

2. **Create Folder Structure**
   - Create a new folder named "energyDataHub"
   - Right-click the folder → "Share"

3. **Share with Service Account**
   - Copy the service account email from the JSON file
     (looks like: `energydatahub-archiver@project-id.iam.gserviceaccount.com`)
   - Paste it in the "Share with people and groups" field
   - Set role to "Editor"
   - Click "Share"

4. **Get Folder ID**
   - Open the energyDataHub folder
   - Copy the folder ID from the URL:
     ```
     https://drive.google.com/drive/folders/1AbC123XyZ789...
                                              ^^^^^^^^^^^^^^^^^
                                              This is the folder ID
     ```

### Step 3: Configure GitHub Secrets

1. **Go to GitHub Repository Settings**
   - Navigate to: https://github.com/ducroq/energydatahub/settings/secrets/actions

2. **Add Secret: GDRIVE_SERVICE_ACCOUNT_JSON**
   - Click "New repository secret"
   - Name: `GDRIVE_SERVICE_ACCOUNT_JSON`
   - Value: Paste the **entire contents** of the service account JSON file
   - Click "Add secret"

3. **Add Secret: GDRIVE_ROOT_FOLDER_ID**
   - Click "New repository secret"
   - Name: `GDRIVE_ROOT_FOLDER_ID`
   - Value: Paste the folder ID from Step 2.4
   - Click "Add secret"

### Step 4: Test the Setup

1. **Manual Workflow Trigger**
   - Go to: https://github.com/ducroq/energydatahub/actions/workflows/collect-data.yml
   - Click "Run workflow"
   - Select branch "main" or "dev"
   - Click "Run workflow"

2. **Check Workflow Logs**
   - Wait for workflow to complete
   - Click on the workflow run
   - Expand "Archive timestamped data to Google Drive" step
   - Look for: "✓ All files uploaded successfully"

3. **Verify on Google Drive**
   - Open your energyDataHub folder on Google Drive
   - You should see: `energyDataHub/2025/10/` with JSON files

### Step 5: Verify Rotation

After 7 days, check that old files are deleted from the repo but remain on Google Drive.

## Local Testing

### Install Dependencies
```bash
pip install google-auth google-api-python-client
```

### Test Connection
```bash
# Set environment variables
export GDRIVE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
export GDRIVE_ROOT_FOLDER_ID='your-folder-id'

# Test connection
python storage/gdrive.py test --verbose
```

**Expected Output**:
```
✓ Successfully connected to Google Drive
✓ Root folder ID: 1AbC123XyZ789...
```

### Upload Files Manually
```bash
# Upload specific files
python storage/gdrive.py upload data/251025_*.json --verbose

# Upload all timestamped files
python storage/gdrive.py upload data/2*_*.json
```

## Troubleshooting

### Issue: "Permission denied" or "403 Forbidden"

**Cause**: Service account doesn't have access to the folder

**Solution**:
1. Check that you shared the Google Drive folder with the service account email
2. Verify the email address matches exactly (from JSON file)
3. Ensure role is set to "Editor", not "Viewer"

### Issue: "Invalid credentials"

**Cause**: Service account JSON is malformed or incorrect

**Solution**:
1. Verify the JSON is valid (use a JSON validator)
2. Check that you copied the entire JSON file contents
3. Regenerate the service account key if needed

### Issue: "Folder not found"

**Cause**: Incorrect folder ID

**Solution**:
1. Double-check the folder ID from the URL
2. Make sure you're using the folder ID, not the file ID
3. Verify the folder still exists and hasn't been deleted

### Issue: Workflow step shows "Warning: Google Drive archival failed"

**Cause**: Archival failed but didn't stop the workflow (non-blocking)

**Solution**:
1. Check the workflow logs for specific error message
2. Verify secrets are set correctly in GitHub
3. Test locally with the same credentials

### Issue: Files not organized by month

**Cause**: Filename doesn't match expected pattern

**Solution**:
- Files must follow pattern: `YYMMDD_HHMMSS_*.json`
- Example: `251025_161234_energy_price_forecast.json`
- Files not matching pattern are uploaded to root `energyDataHub/` folder

## Monitoring

### Check Archival Status

**GitHub Actions**:
- Go to: https://github.com/ducroq/energydatahub/actions
- Look for "Archive timestamped data to Google Drive" step
- Green checkmark = success
- Yellow warning = non-blocking failure

**Google Drive**:
- Check folder sizes grow daily
- Verify latest files are present
- Expected: ~4 files per day (energy, weather, sun, air)

### Storage Usage

**GitHub Repository**:
- Should stabilize at ~7 days of data
- Approximately 7 × 4 files = 28 files in data/ folder

**Google Drive**:
- Grows indefinitely (or until you set up rotation)
- ~4 files/day × 365 days/year = ~1,460 files/year
- Estimated size: ~1-5 MB per file = ~2-8 GB/year

### Set Up Alerts

Consider adding Slack/email notifications for archival failures:

```yaml
- name: Notify on archival failure
  if: failure()
  run: |
    curl -X POST ${{ secrets.SLACK_WEBHOOK }} \
      -d '{"text":"Google Drive archival failed!"}'
```

## Advanced Configuration

### Change Rotation Period

Edit `.github/workflows/collect-data.yml`:
```yaml
- name: Rotate local data (keep last 30 days)
  run: |
    find data -name "2*_*.json" -type f -mtime +30 -delete
```

### Organize by Different Structure

Modify `storage/gdrive.py` method `_get_folder_path()` to organize differently:
```python
# By year only
def _get_folder_path(self, year: str, month: str) -> str:
    root_id = self._get_or_create_folder('energyDataHub', self.root_folder_id)
    year_id = self._get_or_create_folder(year, root_id)
    return year_id  # Skip month folder

# By data type
def _get_folder_path(self, data_type: str) -> str:
    root_id = self._get_or_create_folder('energyDataHub', self.root_folder_id)
    type_id = self._get_or_create_folder(data_type, root_id)
    return type_id
```

### Add Google Drive Retention Policy

To automatically delete files older than X days from Google Drive:

```python
# Add to storage/gdrive.py
def cleanup_old_files(self, days_to_keep: int = 365):
    """Delete files older than specified days from Google Drive."""
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    # Implementation: search files, filter by date, delete
```

## Best Practices

1. **Never commit service account JSON to repository**
   - Always use GitHub Secrets
   - Add `*.json` to `.gitignore` if testing locally

2. **Monitor storage usage**
   - Google Drive free tier: 15 GB
   - Consider paid plan or retention policy for long-term

3. **Test before production**
   - Always test with manual workflow trigger first
   - Verify files appear on Google Drive
   - Check file organization is correct

4. **Backup your service account key**
   - Save the JSON file in a secure location
   - You can't re-download it after creation

5. **Use separate service accounts for dev/prod**
   - Create different service accounts for testing
   - Use different folders for different environments

## Security Considerations

### Service Account Permissions

- ✅ Service account only has access to the specific folder you shared
- ✅ Cannot access your entire Google Drive
- ✅ Can be revoked at any time by unsharing the folder

### GitHub Secrets

- ✅ Secrets are encrypted and not visible in logs
- ✅ Only available to workflow runs
- ✅ Can be rotated by updating the secret

### Data Encryption

- ✅ Files are encrypted before upload (AES-256-CBC)
- ✅ Even if Google Drive is compromised, data is secure
- ✅ Encryption keys are separate (in GitHub Secrets)

## FAQ

**Q: What happens if Google Drive archival fails?**
A: The workflow continues (non-blocking). Data is still published to GitHub Pages. You'll see a warning in the logs.

**Q: Can I archive to a shared drive instead of My Drive?**
A: Yes! Just use the shared drive folder ID and ensure the service account has access.

**Q: How do I retrieve historical data?**
A: Browse Google Drive folders by year/month, or use the Google Drive API to download programmatically.

**Q: Can I disable archival temporarily?**
A: Yes, comment out the "Archive timestamped data to Google Drive" step in the workflow.

**Q: What if I hit Google Drive storage limits?**
A: Implement retention policy to delete old files, or upgrade to Google Workspace for more storage.

## Support

For issues:
1. Check workflow logs in GitHub Actions
2. Test connection locally with `python storage/gdrive.py test`
3. Review this documentation
4. Raise an issue on GitHub

---

**Last Updated**: October 25, 2025
**Status**: ✅ Production Ready
**Auto-archival**: ✅ Enabled in collect-data.yml
