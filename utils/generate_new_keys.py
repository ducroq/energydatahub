"""
Generate new encryption and HMAC keys for credential rotation.

Run this script to generate new secure keys, then update:
1. secrets.local.ini (for local development)
2. GitHub Secrets (ENCRYPTION_KEY and HMAC_KEY)
"""

import os
import base64

print("="*60)
print("GENERATING NEW ENCRYPTION KEYS")
print("="*60)
print("\nWARNING: Keep these keys secure!")
print("DO NOT commit them to git or share them publicly.\n")

# Generate new 32-byte (256-bit) keys
new_encryption_key = base64.b64encode(os.urandom(32)).decode('utf-8')
new_hmac_key = base64.b64encode(os.urandom(32)).decode('utf-8')

print("=" * 60)
print("NEW ENCRYPTION KEY:")
print("=" * 60)
print(new_encryption_key)
print()

print("=" * 60)
print("NEW HMAC KEY:")
print("=" * 60)
print(new_hmac_key)
print()

print("=" * 60)
print("NEXT STEPS:")
print("=" * 60)
print("1. Add to secrets.local.ini:")
print("   [security_keys]")
print(f"   encryption = {new_encryption_key}")
print(f"   hmac = {new_hmac_key}")
print()
print("2. Update GitHub Secrets:")
print("   - ENCRYPTION_KEY = " + new_encryption_key)
print("   - HMAC_KEY = " + new_hmac_key)
print()
print("3. Run GitHub Actions workflow to re-encrypt all data")
print("4. Verify decryption works with new keys")
print()
print("WARNING: OLD ENCRYPTED DATA WILL BECOME UNREADABLE!")
print("=" * 60)
