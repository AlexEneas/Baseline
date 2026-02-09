#!/usr/bin/env python3
r"""
mik_sync_artwork.py

Fills Mixed In Key album art (Song.Artwork BLOB) from embedded artwork in audio files.

- Dry-run by default, use --apply to write changes
- Makes a timestamped backup before applying (unless --no-backup)
- Default behavior only fills missing/empty Artwork (recommended)
- Optional --overwrite to replace existing Artwork too
- Writes a CSV report (optional)

Usage:
  python mik_sync_artwork.py "C:\Path\MIKStore.db" --dry-run --report "art_report.csv"
  python mik_sync_artwork.py "C:\Path\MIKStore.db" --apply --report "art_applied.csv"
  python mik_sync_artwork.py "C:\Path\MIKStore.db" --apply --overwrite

Important:
- Close Mixed In Key before running --apply
"""

import argparse
import csv
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

from mutagen import File as MutagenFile

try:
    from mutagen.flac import FLAC
except Exception:
    FLAC = None

try:
    from mutagen.id3 import ID3
except Exception:
    ID3 = None

try:
    from mutagen.mp4 import MP4
except Exception:
    MP4 = None


def is_sqlite_file(p: Path) -> bool:
    if not p.exists() or not p.is_file():
        return False
    try:
        with p.open("rb") as f:
            header = f.read(16)
        return header.startswith(b"SQLite format 3")
    except Exception:
        return False


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_suffix(db_path.suffix + f".backup_{ts}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def normalize_db_path_value(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()

    # file:// URIs
    if s.lower().startswith("file://"):
        s2 = s[7:]
        if s2.lower().startswith("localhost/"):
            s2 = s2[len("localhost/"):]
        while s2.startswith("/") and len(s2) > 2 and s2[2] == ":":
            s2 = s2[1:]
        s = s2

    # decode %20 etc
    try:
        from urllib.parse import unquote
        s = unquote(s)
    except Exception:
        pass

    # normalize slashes
    s = s.replace("/", "\\") if os.name == "nt" else s.replace("\\", "/")
    return s


def infer_mime(data: bytes) -> str:
    if not data:
        return ""
    if data[:2] == b"\xFF\xD8":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    return ""


def extract_embedded_art_bytes(file_path: Path) -> Tuple[Optional[bytes], str]:
    """
    Returns (image_bytes, mime_type) from embedded artwork, if present.
    Supports common formats:
      - FLAC (PICTURE blocks)
      - MP3 (ID3 APIC)
      - M4A/MP4 (covr atom)
      - AIFF/WAV can sometimes carry ID3 chunks, we attempt via mutagen generic path too
    """
    ext = file_path.suffix.lower()

    # FLAC
    if ext == ".flac" and FLAC is not None:
        try:
            f = FLAC(str(file_path))
            if getattr(f, "pictures", None):
                pic = f.pictures[0]
                mime = getattr(pic, "mime", "") or infer_mime(pic.data)
                return pic.data, mime
        except Exception:
            pass

    # MP3
    if ext == ".mp3" and ID3 is not None:
        try:
            id3 = ID3(str(file_path))
            apics = id3.getall("APIC")
            if apics:
                apic = apics[0]
                data = bytes(apic.data)
                mime = getattr(apic, "mime", "") or infer_mime(data)
                return data, mime
        except Exception:
            pass

    # MP4/M4A
    if ext in (".m4a", ".mp4", ".aac") and MP4 is not None:
        try:
            mp4 = MP4(str(file_path))
            covr = mp4.tags.get("covr") if mp4.tags else None
            if covr:
                data = bytes(covr[0])
                return data, infer_mime(data)
        except Exception:
            pass

    # Generic attempt for AIFF/WAV or anything else mutagen can read
    # Sometimes tags may contain picture data in format-specific keys.
    try:
        audio = MutagenFile(str(file_path))
        if not audio or not getattr(audio, "tags", None):
            return None, ""

        tags = audio.tags

        # Common fallback: ID3 APIC present even in AIFF/WAV containers
        if ID3 is not None:
            try:
                id3 = ID3(str(file_path))
                apics = id3.getall("APIC")
                if apics:
                    apic = apics[0]
                    data = bytes(apic.data)
                    mime = getattr(apic, "mime", "") or infer_mime(data)
                    return data, mime
            except Exception:
                pass

        # No reliable generic mapping found
        return None, ""
    except Exception:
        return None, ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db", help="Path to Mixed In Key DB (MIKStore.db)")
    ap.add_argument("--apply", action="store_true", help="Write changes to the DB")
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run")
    ap.add_argument("--no-backup", action="store_true", help="Do not create a backup before applying")
    ap.add_argument("--report", default=None, help="CSV report filename")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N songs (0 means all)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing Artwork too (default only fills missing)")
    ap.add_argument("--only-existing-files", action="store_true", help="Skip DB rows whose file does not exist (recommended)")

    args = ap.parse_args()

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 2
    if not is_sqlite_file(db_path):
        print("ERROR: This file does not look like a SQLite database.")
        return 2

    apply_changes = bool(args.apply) and not bool(args.dry_run)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Confirm expected schema
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(Song)").fetchall()]
        needed = {"Id", "File", "Artwork"}
        if not needed.issubset(set(cols)):
            print("ERROR: Expected Song table with Id, File, Artwork columns.")
            print(f"Found columns: {cols}")
            return 2

        rows = conn.execute(
            "SELECT Id, File, length(Artwork) AS ArtworkLen FROM Song"
        ).fetchall()

        if args.limit and args.limit > 0:
            rows = rows[: args.limit]

        if apply_changes and not args.no_backup:
            backup_path = backup_db(db_path)
            print(f"Backup created: {backup_path}")

        changes = []
        scanned = 0
        missing_files = 0
        no_embedded_art = 0
        skipped_has_art = 0
        updated = 0

        if apply_changes:
            conn.execute("BEGIN")

        try:
            for r in rows:
                scanned += 1
                song_id = r["Id"]
                raw_path = r["File"]
                current_len = int(r["ArtworkLen"] or 0)

                if (not args.overwrite) and current_len > 0:
                    skipped_has_art += 1
                    continue

                path_str = normalize_db_path_value(raw_path)
                if not path_str:
                    continue

                fpath = Path(path_str)
                if args.only_existing_files and not fpath.exists():
                    missing_files += 1
                    continue

                if not fpath.exists():
                    # if user did not set only-existing-files, still count and skip
                    missing_files += 1
                    continue

                img_bytes, mime = extract_embedded_art_bytes(fpath)
                if not img_bytes:
                    no_embedded_art += 1
                    continue

                if apply_changes:
                    conn.execute(
                        "UPDATE Song SET Artwork = ? WHERE Id = ?",
                        (sqlite3.Binary(img_bytes), song_id),
                    )

                updated += 1
                changes.append({
                    "Id": str(song_id),
                    "File": str(fpath),
                    "PreviousBytes": str(current_len),
                    "NewBytes": str(len(img_bytes)),
                    "Mime": mime,
                })

            if apply_changes:
                conn.commit()
        except Exception:
            if apply_changes:
                conn.rollback()
            raise

        print(f"Rows scanned: {scanned}")
        print(f"Skipped (already had art): {skipped_has_art}")
        print(f"Missing files skipped: {missing_files}")
        print(f"No embedded artwork found in file: {no_embedded_art}")
        print(f"Artwork updates: {updated}")

        if args.report:
            report_path = Path(args.report).expanduser()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            with report_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["Id", "File", "PreviousBytes", "NewBytes", "Mime"])
                w.writeheader()
                w.writerows(changes)
            print(f"Report written: {report_path}")

        if not apply_changes:
            print("Dry-run mode, no DB updates performed.")
            print("Re-run with: --apply")
        else:
            print("Applied artwork updates to the database.")

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
