"""
Decrypt files from data/ and place them into the 05. Data/<YYYY-MM>/ archive,
matching the existing monthly-folder convention.

Idempotent: skips files that already exist in the target month folder.
By default, processes files from 2026-02 onwards (where the archive ends).
"""
import os
import sys
import glob
import json
import base64
import logging
import argparse
from configparser import ConfigParser

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.secure_data_handler import SecureDataHandler

ARCHIVE_ROOT = r"C:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - WebBasedControl\05. Data"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def load_keys(repo_root):
    config = ConfigParser()
    config.read(os.path.join(repo_root, "secrets.ini"))
    return (
        base64.b64decode(config.get("security_keys", "encryption")),
        base64.b64decode(config.get("security_keys", "hmac")),
    )


def month_folder(filename, archive_root):
    # YYMMDD_HHMMSS_type.json → archive_root/20YY-MM/
    yymmdd = os.path.basename(filename).split("_")[0]
    if len(yymmdd) != 6 or not yymmdd.isdigit():
        return None
    yyyy_mm = f"20{yymmdd[0:2]}-{yymmdd[2:4]}"
    return os.path.join(archive_root, yyyy_mm)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="260201",
                    help="Process files dated >= YYMMDD (default: 260201)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    enc_key, hmac_key = load_keys(repo_root)
    handler = SecureDataHandler(enc_key, hmac_key)

    pattern = os.path.join(DATA_DIR, "[0-9]" * 6 + "_*.json")
    files = sorted(glob.glob(pattern))
    files = [f for f in files if os.path.basename(f)[:6] >= args.since]

    logging.info(f"Found {len(files)} files dated >= {args.since}")

    stats = {"decrypted": 0, "plain_copied": 0, "skipped": 0,
             "errors": 0, "folders_created": set()}

    for src in files:
        name = os.path.basename(src)
        out_dir = month_folder(src, ARCHIVE_ROOT)
        if out_dir is None:
            logging.warning(f"Cannot derive month folder for {name}")
            continue
        out_path = os.path.join(out_dir, name)

        if os.path.exists(out_path):
            stats["skipped"] += 1
            continue

        if args.dry_run:
            logging.info(f"[dry-run] would write {out_path}")
            continue

        if not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)
            stats["folders_created"].add(out_dir)

        try:
            with open(src, "r") as f:
                raw = f.read()
            if not raw:
                raise ValueError("empty file")
            try:
                data = json.loads(raw)
                stats["plain_copied"] += 1
            except json.JSONDecodeError:
                data = handler.decrypt_and_verify(raw)
                stats["decrypted"] += 1
            with open(out_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logging.error(f"FAIL {name}: {e}")
            stats["errors"] += 1

    logging.info(f"Done. decrypted={stats['decrypted']} "
                 f"plain_copied={stats['plain_copied']} "
                 f"skipped={stats['skipped']} errors={stats['errors']} "
                 f"new_folders={len(stats['folders_created'])}")
    for d in sorted(stats["folders_created"]):
        logging.info(f"  created {d}")


if __name__ == "__main__":
    main()
