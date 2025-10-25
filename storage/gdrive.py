"""
Google Drive Storage Backend for Energy Data Hub
-------------------------------------------------
Handles archival of timestamped energy data files to Google Drive for historical database.

File: storage/gdrive.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Provides automated archival of energy data files to Google Drive using
    a service account. Organizes files by year/month for easy retrieval.

Dependencies:
    - google-auth
    - google-api-python-client

Usage:
    # From Python
    from storage.gdrive import GoogleDriveArchiver

    archiver = GoogleDriveArchiver(
        credentials_json='path/to/service-account.json',
        folder_id='your-folder-id'
    )
    archiver.upload_file('data/251025_161234_energy_price_forecast.json')

    # From command line
    python storage/gdrive.py upload data/*.json

Notes:
    - Requires Google Service Account with Drive API enabled
    - Files organized as: energyDataHub/{YEAR}/{MONTH}/filename.json
    - Supports batch uploads for efficiency
    - Includes retry logic for transient failures
"""

import os
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
import re

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
except ImportError:
    print("ERROR: Google API libraries not installed.")
    print("Install with: pip install google-auth google-api-python-client")
    raise


class GoogleDriveArchiver:
    """
    Google Drive archiver for historical energy data.

    Organizes files by year/month structure on Google Drive.
    """

    SCOPES = ['https://www.googleapis.com/auth/drive.file']

    def __init__(
        self,
        credentials_json: Optional[str] = None,
        credentials_dict: Optional[Dict] = None,
        root_folder_id: Optional[str] = None
    ):
        """
        Initialize Google Drive archiver.

        Args:
            credentials_json: Path to service account JSON file
            credentials_dict: Service account credentials as dict (from env var)
            root_folder_id: Google Drive folder ID for root storage
        """
        self.logger = logging.getLogger(__name__)

        # Load credentials
        if credentials_dict:
            self.credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=self.SCOPES
            )
        elif credentials_json:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_json,
                scopes=self.SCOPES
            )
        else:
            raise ValueError("Either credentials_json or credentials_dict must be provided")

        # Build Drive API client
        self.service = build('drive', 'v3', credentials=self.credentials)
        self.root_folder_id = root_folder_id

        # Cache for folder IDs to avoid repeated lookups
        self._folder_cache = {}

    def _extract_date_from_filename(self, filename: str) -> Optional[tuple]:
        """
        Extract year and month from timestamped filename.

        Args:
            filename: e.g., "251025_161234_energy_price_forecast.json"

        Returns:
            (year, month) tuple or None if pattern doesn't match

        Examples:
            >>> _extract_date_from_filename("251025_161234_energy.json")
            ('2025', '10')
        """
        # Pattern: YYMMDD_HHMMSS_*.json
        match = re.match(r'(\d{2})(\d{2})(\d{2})_\d{6}_.*\.json', filename)
        if match:
            yy, mm, dd = match.groups()
            year = f"20{yy}"  # Assume 21st century
            month = mm
            return (year, month)
        return None

    def _get_or_create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> str:
        """
        Get existing folder ID or create new folder.

        Args:
            folder_name: Name of the folder
            parent_id: Parent folder ID (None for root)

        Returns:
            Folder ID
        """
        cache_key = f"{parent_id}:{folder_name}"

        # Check cache
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        # Search for existing folder
        query_parts = [
            f"name='{folder_name}'",
            "mimeType='application/vnd.google-apps.folder'",
            "trashed=false"
        ]
        if parent_id:
            query_parts.append(f"'{parent_id}' in parents")

        query = " and ".join(query_parts)

        try:
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()

            items = results.get('files', [])

            if items:
                # Folder exists
                folder_id = items[0]['id']
                self.logger.debug(f"Found existing folder '{folder_name}': {folder_id}")
            else:
                # Create folder
                file_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                if parent_id:
                    file_metadata['parents'] = [parent_id]

                folder = self.service.files().create(
                    body=file_metadata,
                    fields='id'
                ).execute()

                folder_id = folder.get('id')
                self.logger.info(f"Created folder '{folder_name}': {folder_id}")

            # Cache the result
            self._folder_cache[cache_key] = folder_id
            return folder_id

        except HttpError as e:
            self.logger.error(f"Error getting/creating folder '{folder_name}': {e}")
            raise

    def _get_folder_path(self, year: str, month: str) -> str:
        """
        Get or create folder path: energyDataHub/{year}/{month}/

        Args:
            year: e.g., "2025"
            month: e.g., "10"

        Returns:
            Folder ID for the month folder
        """
        # Root folder (energyDataHub)
        root_id = self._get_or_create_folder('energyDataHub', self.root_folder_id)

        # Year folder
        year_id = self._get_or_create_folder(year, root_id)

        # Month folder
        month_id = self._get_or_create_folder(month, year_id)

        return month_id

    def upload_file(
        self,
        file_path: str,
        max_retries: int = 3
    ) -> Optional[str]:
        """
        Upload a single file to Google Drive.

        Args:
            file_path: Path to file to upload
            max_retries: Number of retry attempts for transient failures

        Returns:
            File ID of uploaded file, or None if failed
        """
        file_path = Path(file_path)

        if not file_path.exists():
            self.logger.error(f"File not found: {file_path}")
            return None

        filename = file_path.name

        # Extract date to determine folder
        date_info = self._extract_date_from_filename(filename)
        if not date_info:
            self.logger.warning(f"Could not extract date from filename: {filename}")
            # Upload to root folder
            parent_folder_id = self._get_or_create_folder('energyDataHub', self.root_folder_id)
        else:
            year, month = date_info
            parent_folder_id = self._get_folder_path(year, month)

        # Prepare file metadata
        file_metadata = {
            'name': filename,
            'parents': [parent_folder_id]
        }

        media = MediaFileUpload(
            str(file_path),
            mimetype='application/json',
            resumable=True
        )

        # Upload with retry logic
        for attempt in range(max_retries):
            try:
                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name, webViewLink'
                ).execute()

                file_id = file.get('id')
                web_link = file.get('webViewLink')

                self.logger.info(f"Uploaded {filename} to Google Drive: {file_id}")
                self.logger.debug(f"View at: {web_link}")

                return file_id

            except HttpError as e:
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Upload attempt {attempt + 1} failed for {filename}, retrying... Error: {e}"
                    )
                else:
                    self.logger.error(f"Failed to upload {filename} after {max_retries} attempts: {e}")
                    return None

    def upload_files(self, file_paths: List[str]) -> Dict[str, Optional[str]]:
        """
        Upload multiple files to Google Drive.

        Args:
            file_paths: List of file paths to upload

        Returns:
            Dict mapping file path to file ID (or None if failed)
        """
        results = {}

        for file_path in file_paths:
            file_id = self.upload_file(file_path)
            results[file_path] = file_id

        # Summary
        successful = sum(1 for fid in results.values() if fid is not None)
        self.logger.info(
            f"Upload complete: {successful}/{len(file_paths)} files uploaded successfully"
        )

        return results


def main():
    """Command-line interface for Google Drive archival."""
    parser = argparse.ArgumentParser(
        description='Archive energy data files to Google Drive'
    )
    parser.add_argument(
        'action',
        choices=['upload', 'test'],
        help='Action to perform'
    )
    parser.add_argument(
        'files',
        nargs='*',
        help='Files to upload (for upload action)'
    )
    parser.add_argument(
        '--credentials',
        default=os.environ.get('GDRIVE_CREDENTIALS_PATH'),
        help='Path to service account JSON file (or set GDRIVE_CREDENTIALS_PATH env var)'
    )
    parser.add_argument(
        '--folder-id',
        default=os.environ.get('GDRIVE_ROOT_FOLDER_ID'),
        help='Root folder ID on Google Drive (or set GDRIVE_ROOT_FOLDER_ID env var)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    if args.action == 'test':
        logger.info("Testing Google Drive connection...")

        # Try to load credentials
        credentials_dict = None
        if os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON'):
            try:
                credentials_dict = json.loads(os.environ['GDRIVE_SERVICE_ACCOUNT_JSON'])
                logger.info("Loaded credentials from GDRIVE_SERVICE_ACCOUNT_JSON environment variable")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse GDRIVE_SERVICE_ACCOUNT_JSON: {e}")
                return 1

        try:
            archiver = GoogleDriveArchiver(
                credentials_json=args.credentials,
                credentials_dict=credentials_dict,
                root_folder_id=args.folder_id
            )
            logger.info("✓ Successfully connected to Google Drive")
            logger.info(f"✓ Root folder ID: {args.folder_id or 'Using My Drive root'}")
            return 0
        except Exception as e:
            logger.error(f"✗ Failed to connect to Google Drive: {e}")
            return 1

    elif args.action == 'upload':
        if not args.files:
            logger.error("No files specified for upload")
            return 1

        # Expand glob patterns if needed
        import glob
        file_list = []
        for pattern in args.files:
            file_list.extend(glob.glob(pattern))

        if not file_list:
            logger.error("No files found matching patterns")
            return 1

        logger.info(f"Uploading {len(file_list)} files to Google Drive...")

        # Load credentials
        credentials_dict = None
        if os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON'):
            try:
                credentials_dict = json.loads(os.environ['GDRIVE_SERVICE_ACCOUNT_JSON'])
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse GDRIVE_SERVICE_ACCOUNT_JSON: {e}")
                return 1

        try:
            archiver = GoogleDriveArchiver(
                credentials_json=args.credentials,
                credentials_dict=credentials_dict,
                root_folder_id=args.folder_id
            )

            results = archiver.upload_files(file_list)

            failed = [f for f, fid in results.items() if fid is None]
            if failed:
                logger.warning(f"Failed to upload {len(failed)} files:")
                for f in failed:
                    logger.warning(f"  - {f}")
                return 1

            logger.info("✓ All files uploaded successfully")
            return 0

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
