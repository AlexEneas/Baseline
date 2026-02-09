#!/usr/bin/env python3
r"""
mik_sync_tags_from_files.py

Syncs tags from audio files into a Mixed In Key SQLite database.

- Auto-detects the Song table and common tag columns
- Reads tags from files using mutagen
- Updates DB fields when file tags differ
- Dry-run by default, use --apply to commit
- Makes a backup before applying (unless --no-backup)
- Writes a report (optional)

Examples:
  python mik_sync_tags_from_files.py "C:\Path\MIKStore.db" --dry-run --report "changes.csv"
  python mik_sync_tags_from_files.py "C:\Path\MIKStore.db" --apply --report "changes.csv"

Optional:
  --allow-empty   Allow overwriting DB fields with empty values if the file has no tag
  --limit 1000    Only process first 1000 rows
"""

import argparse
import csv
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mutagen import File as MutagenFile


# Common column names Mixed In Key might use (case-insensitive matching)
COLUMN_CANDIDATES = {
    "artist": ["artist", "artists", "artistname", "artist_name"],
    "title": ["title", "track", "tracktitle", "track_title", "name"],
    "album": ["album", "release", "albumtitle", "album_title"],
    "genre": ["genre", "genres", "style"],
    "bpm": ["bpm", "tempo"],
    "key": ["key", "initialkey", "initial_key", "camelot", "musickey"],
    "year": ["year", "date"],
}

# Mutagen tag keys to try (varies by format)
TAG_KEYS = {
    "artist": ["artist", "artists", "albumartist", "album artist", "tpe1", "tpe2"],
    "title": ["title", "tracktitle", "tit2"],
    "album": ["album", "albumtitle", "talb"],
    "genre": ["genre", "tcon"],
    "bpm": ["bpm", "tbpm", "tempo"],
    "key": ["initialkey", "initial key", "tkey", "key"],
    "year": ["date", "year", "tyer", "tdrc"],
}


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


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [r["name"] for r in cur.fetchall()]


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]


def pick_song_table(conn: sqlite3.Connection) -> str:
    # Prefer "Song" if it exists, otherwise pick the best match containing File/Path
    tables = list_tables(conn)
    if "Song" in tables:
        return "Song"

    best = None  # (score, table)
    for t in tables:
        cols = [c.lower() for c in table_columns(conn, t)]
        score = 0
        if "file" in cols or "path" in cols or "location" in cols:
            score += 5
        if "id" in cols or any(c.endswith("id") for c in cols):
            score += 2
        if "song" in t.lower() or "track" in t.lower():
            score += 2
        if best is None or score > best[0]:
            best = (score, t)

    if not best or best[0] <= 0:
        raise RuntimeError("Could not find a suitable Song/Track table in the database.")
    return best[1]


def normalize_db_path_value(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()

    if s.lower().startswith("file://"):
        s2 = s[7:]
        if s2.lower().startswith("localhost/"):
            s2 = s2[len("localhost/"):]
        while s2.startswith("/") and len(s2) > 2 and s2[2] == ":":
            s2 = s2[1:]
        s = s2

    try:
        from urllib.parse import unquote
        s = unquote(s)
    except Exception:
        pass

    # Normalize separators
    s = s.replace("/", "\\") if os.name == "nt" else s.replace("\\", "/")
    return s


def coerce_to_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (list, tuple)):
        # join multi-value tags
        return "; ".join([coerce_to_str(x) for x in val if x is not None]).strip()
    return str(val).strip()


def simplify(s: str) -> str:
    # For comparisons only, not what we write back
    return " ".join((s or "").strip().split()).lower()


def mutagen_get_tag(audio: Any, keys: List[str]) -> str:
    if not audio:
        return ""
    tags = getattr(audio, "tags", None)
    if not tags:
        return ""

    # Mutagen tag maps can vary, try direct and normalized lookups
    for k in keys:
        # try as-is
        if k in tags:
            return coerce_to_str(tags.get(k))
        # try case variants
        for kk in list(tags.keys()):
            if str(kk).lower() == k.lower():
                return coerce_to_str(tags.get(kk))
    return ""


def read_file_tags(file_path: Path) -> Dict[str, str]:
    audio = MutagenFile(str(file_path), easy=True)
    out: Dict[str, str] = {}
    for field, keys in TAG_KEYS.items():
        out[field] = mutagen_get_tag(audio, keys)

    # Some formats store BPM as numeric text, normalize a little
    bpm = out.get("bpm", "")
    if bpm:
        # keep digits and dot only, best effort
        cleaned = "".join(ch for ch in bpm if ch.isdigit() or ch == ".")
        out["bpm"] = cleaned.strip()

    return out


def map_db_columns(cols: List[str]) -> Dict[str, str]:
    """
    Returns mapping from canonical field name -> actual DB column name
    Only includes fields that exist in DB.
    """
    lower_to_actual = {c.lower(): c for c in cols}
    mapped: Dict[str, str] = {}

    for field, candidates in COLUMN_CANDIDATES.items():
        for cand in candidates:
            if cand.lower() in lower_to_actual:
                mapped[field] = lower_to_actual[cand.lower()]
                break
    return mapped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db", help="Path to the Mixed In Key SQLite database (for example MIKStore.db)")
    ap.add_argument("--apply", action="store_true", help="Apply changes to the DB. Default is dry-run.")
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run.")
    ap.add_argument("--no-backup", action="store_true", help="Do not create a backup before applying changes.")
    ap.add_argument("--report", default=None, help="Write a CSV report of changes to this file.")
    ap.add_argument("--limit", type=int, default=0, help="Process only the first N rows (0 means all).")
    ap.add_argument("--allow-empty", action="store_true", help="Allow overwriting DB with empty file tags.")

    args = ap.parse_args()

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 2
    if not is_sqlite_file(db_path):
        print("ERROR: This file does not look like a SQLite database.")
        return 2

    apply_changes = bool(args.apply) and not bool(args.dry_run)

    if apply_changes and not args.no_backup:
        backup_path = backup_db(db_path)
        print(f"Backup created: {backup_path}")

    conn = connect(db_path)
    try:
        song_table = pick_song_table(conn)
        cols = table_columns(conn, song_table)
        col_lc = [c.lower() for c in cols]

        # Expect at least Id and File in your case
        id_col = "Id" if "id" in col_lc else next((c for c in cols if c.lower().endswith("id")), cols[0])
        file_col = "File" if "file" in col_lc else next((c for c in cols if c.lower() in ["path", "location", "uri"]), None)
        if not file_col:
            raise RuntimeError("Could not find a file path column (expected something like File/Path/Location).")

        field_to_dbcol = map_db_columns(cols)
        # Remove any accidental mapping to Id/File
        for forbidden in [id_col.lower(), file_col.lower()]:
            for k, v in list(field_to_dbcol.items()):
                if v.lower() == forbidden:
                    field_to_dbcol.pop(k, None)

        tracked_fields = list(field_to_dbcol.keys())

        print(f"Using table: {song_table}")
        print(f"ID column:   {id_col}")
        print(f"File column: {file_col}")
        if tracked_fields:
            print("Will sync fields:")
            for f in tracked_fields:
                print(f"  {f} -> {field_to_dbcol[f]}")
        else:
            print("WARNING: Could not detect any tag fields in the DB to update (Artist/Title/Album/Genre/BPM/Key/etc).")
            print("If you tell me the column names in your Song table, I can add them.")

        select_cols = [id_col, file_col] + [field_to_dbcol[f] for f in tracked_fields]
        select_sql = f"SELECT {', '.join(select_cols)} FROM {song_table}"

        cur = conn.execute(select_sql)
        rows = cur.fetchall()

        if args.limit and args.limit > 0:
            rows = rows[: args.limit]
            print(f"Limit applied: processing {len(rows)} rows")

        changes: List[Dict[str, str]] = []
        missing_files = 0
        scanned = 0

        if apply_changes:
            conn.execute("BEGIN")

        try:
            for r in rows:
                scanned += 1
                rid = r[id_col]
                raw_path = r[file_col]
                norm_path = normalize_db_path_value(raw_path)
                if not norm_path:
                    continue

                p = Path(norm_path)
                if not p.exists():
                    missing_files += 1
                    continue

                file_tags = read_file_tags(p)

                update_pairs: Dict[str, str] = {}
                for field in tracked_fields:
                    dbcol = field_to_dbcol[field]
                    db_val = coerce_to_str(r[dbcol])
                    file_val = coerce_to_str(file_tags.get(field, ""))

                    # Default: do not overwrite DB with empty tag
                    if not args.allow_empty and file_val == "":
                        continue

                    if simplify(db_val) != simplify(file_val):
                        update_pairs[dbcol] = file_val

                if not update_pairs:
                    continue

                # Record changes
                for dbcol, new_val in update_pairs.items():
                    changes.append({
                        "Id": str(rid),
                        "File": str(p),
                        "Column": dbcol,
                        "OldValue": coerce_to_str(r[dbcol]),
                        "NewValue": new_val,
                    })

                if apply_changes:
                    set_clause = ", ".join([f"{c} = ?" for c in update_pairs.keys()])
                    params = list(update_pairs.values()) + [rid]
                    conn.execute(f"UPDATE {song_table} SET {set_clause} WHERE {id_col} = ?", params)

            if apply_changes:
                conn.commit()
        except Exception:
            if apply_changes:
                conn.rollback()
            raise

        print(f"Total rows scanned: {scanned}")
        print(f"Missing files skipped: {missing_files}")
        print(f"Field changes detected: {len(changes)}")

        if args.report:
            rp = Path(args.report).expanduser()
            rp.parent.mkdir(parents=True, exist_ok=True)
            with rp.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["Id", "File", "Column", "OldValue", "NewValue"])
                w.writeheader()
                w.writerows(changes)
            print(f"Report written: {rp}")

        if not apply_changes:
            print("Dry-run mode, no DB updates performed.")
            print("Re-run with: --apply")
        else:
            print("Applied updates to the database.")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
