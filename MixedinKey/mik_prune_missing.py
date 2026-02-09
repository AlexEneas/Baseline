#!/usr/bin/env python3
r"""
miK_prune_missing.py

Scans a Mixed In Key SQLite database and deletes track rows whose file path no longer exists.

Safety features:
- Creates a timestamped backup of the DB before modifying (unless --no-backup)
- Dry-run mode shows what would be deleted without changing anything
- Writes a report file (optional)

Usage examples:
  python miK_prune_missing.py "C:\Path\to\MixedInKey.db" --dry-run
  python miK_prune_missing.py "C:\Path\to\MixedInKey.db" --apply
  python miK_prune_missing.py "C:\Path\to\MixedInKey.db" --apply --report "deleted.txt"

If auto-detection fails, specify:
  --table <table_name> --path-col <column_name> --id-col <column_name>
"""

import argparse
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional


LIKELY_PATH_COLS = [
    "path", "file_path", "filepath", "filename", "file", "location", "url", "uri",
    "track_path", "fullpath", "full_path",
]

LIKELY_ID_COLS = ["id", "track_id", "uuid", "pk"]


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
    # Using URI mode lets us open read-only if needed (not used here, but handy)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r["name"] for r in cur.fetchall()]


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]


def score_table_for_paths(cols: List[str]) -> int:
    score = 0
    lower = [c.lower() for c in cols]
    for c in lower:
        if c in LIKELY_PATH_COLS:
            score += 3
        if "path" in c or "file" in c or "location" in c or "uri" in c:
            score += 1
    # Tables with an obvious id get a small bonus
    for c in lower:
        if c in LIKELY_ID_COLS or c.endswith("id"):
            score += 1
    return score


def pick_table_and_columns(
    conn: sqlite3.Connection,
    forced_table: Optional[str],
    forced_path_col: Optional[str],
    forced_id_col: Optional[str],
) -> Tuple[str, str, str]:
    if forced_table and forced_path_col and forced_id_col:
        return forced_table, forced_path_col, forced_id_col

    tables = list_tables(conn)
    if not tables:
        raise RuntimeError("No tables found in the database.")

    # If user forced table, only search within it
    candidate_tables = [forced_table] if forced_table else tables

    best = None  # (score, table, path_col, id_col)
    for t in candidate_tables:
        cols = table_columns(conn, t)
        if not cols:
            continue

        lower_map = {c.lower(): c for c in cols}

        # Choose path column
        path_col = None
        if forced_path_col and forced_path_col in cols:
            path_col = forced_path_col
        else:
            for key in LIKELY_PATH_COLS:
                if key in lower_map:
                    path_col = lower_map[key]
                    break
            if not path_col:
                # fallback: any column containing "path" or "file"
                for c in cols:
                    cl = c.lower()
                    if "path" in cl or "file" in cl or "location" in cl or "uri" in cl:
                        path_col = c
                        break

        if not path_col:
            continue

        # Choose id column
        id_col = None
        if forced_id_col and forced_id_col in cols:
            id_col = forced_id_col
        else:
            for key in LIKELY_ID_COLS:
                if key in lower_map:
                    id_col = lower_map[key]
                    break
            if not id_col:
                # fallback: first column ending with id, else first column
                for c in cols:
                    if c.lower().endswith("id"):
                        id_col = c
                        break
            if not id_col:
                id_col = cols[0]

        score = score_table_for_paths(cols)
        # Bonus if path col name is a strong match
        pcl = path_col.lower()
        if pcl in LIKELY_PATH_COLS:
            score += 5
        if "path" in pcl or "file" in pcl:
            score += 2

        if best is None or score > best[0]:
            best = (score, t, path_col, id_col)

    if not best:
        raise RuntimeError(
            "Could not auto-detect a table/column that contains file paths. "
            "Use --table, --path-col, and --id-col."
        )

    return best[1], best[2], best[3]


def normalize_db_path_value(val: str) -> str:
    """
    Mixed In Key may store:
    - Windows paths: C:\\Music\\track.flac
    - file URIs: file:///C:/Music/track.flac
    - urlencoded paths
    """
    if val is None:
        return ""

    s = str(val).strip()

    # Handle file:// URIs
    if s.lower().startswith("file://"):
        # file:///C:/path... or file://localhost/C:/path...
        s2 = s[7:]
        if s2.lower().startswith("localhost/"):
            s2 = s2[len("localhost/"):]
        # Remove leading slashes before drive letter
        while s2.startswith("/") and len(s2) > 2 and s2[2] == ":":
            s2 = s2[1:]
        s = s2

    # Best effort decode %20 etc
    try:
        from urllib.parse import unquote
        s = unquote(s)
    except Exception:
        pass

    # Normalize slashes
    s = s.replace("/", "\\") if os.name == "nt" else s.replace("\\", "/")
    return s


def file_exists(path_str: str) -> bool:
    if not path_str:
        return False
    p = Path(path_str)
    return p.exists()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db", help="Path to the Mixed In Key database file (SQLite).")
    ap.add_argument("--apply", action="store_true", help="Actually delete rows. Without this, it will dry-run.")
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run (default if --apply not set).")
    ap.add_argument("--no-backup", action="store_true", help="Do not create a backup before applying changes.")
    ap.add_argument("--report", default=None, help="Write deleted rows (id and path) to this text file.")
    ap.add_argument("--limit", type=int, default=0, help="Limit how many missing tracks to delete (0 means no limit).")

    ap.add_argument("--table", default=None, help="Table name to use (if auto-detection fails).")
    ap.add_argument("--path-col", default=None, help="Column name containing the file path.")
    ap.add_argument("--id-col", default=None, help="Column name containing the unique id (primary key).")

    args = ap.parse_args()

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 2

    if not is_sqlite_file(db_path):
        print("ERROR: This file does not look like a SQLite database.")
        print("If Mixed In Key is using a different DB type on your version, tell me the file name and extension.")
        return 2

    apply_changes = bool(args.apply) and not bool(args.dry_run)

    if apply_changes and not args.no_backup:
        backup_path = backup_db(db_path)
        print(f"Backup created: {backup_path}")

    conn = connect(db_path)
    try:
        table, path_col, id_col = pick_table_and_columns(conn, args.table, args.path_col, args.id_col)
        print(f"Using table: {table}")
        print(f"Path column: {path_col}")
        print(f"ID column:   {id_col}")

        # Pull rows
        cur = conn.execute(f"SELECT {id_col} AS _id, {path_col} AS _path FROM {table}")
        rows = cur.fetchall()

        missing: List[Tuple[str, str]] = []
        for r in rows:
            rid = r["_id"]
            raw_path = r["_path"]
            norm_path = normalize_db_path_value(raw_path)
            if not file_exists(norm_path):
                missing.append((str(rid), norm_path))

        print(f"Total rows scanned: {len(rows)}")
        print(f"Missing files found: {len(missing)}")

        if args.limit and args.limit > 0:
            missing = missing[: args.limit]
            print(f"Limit applied, will process first: {len(missing)}")

        if args.report:
            rp = Path(args.report).expanduser()
            rp.parent.mkdir(parents=True, exist_ok=True)
            with rp.open("w", encoding="utf-8") as f:
                for rid, pth in missing:
                    f.write(f"{rid}\t{pth}\n")
            print(f"Report written: {rp}")

        if not missing:
            print("Nothing to do.")
            return 0

        if not apply_changes:
            print("Dry-run mode, no deletions performed.")
            print("Re-run with: --apply")
            return 0

        # Delete rows by id
        conn.execute("BEGIN")
        try:
            for rid, _pth in missing:
                conn.execute(f"DELETE FROM {table} WHERE {id_col} = ?", (rid,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        print(f"Deleted rows: {len(missing)}")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
