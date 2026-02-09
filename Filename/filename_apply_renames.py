#!/usr/bin/env python3
"""
Baseline, Filename Rename (apply from CSV)

Second step of the workflow:
1) Run Filename Check to generate a CSV of suggestions.
2) User reviews/edits CSV, then marks rows to apply.
3) This tool renames files accordingly.

Safety:
- Default is --dry-run (no changes).
- Only applies rows where column "apply" is truthy (y/yes/true/1).
- Never overwrites existing files. If a target exists, appends " (1)", " (2)" etc.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import csv
import sqlite3
import shutil
from pathlib import Path
from typing import Dict, Tuple


TRUTHY = {"y", "yes", "true", "1", "apply", "ok"}


def _is_truthy(v: str) -> bool:
    return str(v or "").strip().lower() in TRUTHY



def _dedupe_target(target: Path) -> Path:
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    parent = target.parent
    i = 1
    while True:
        cand = parent / f"{stem} ({i}){suffix}"
        if not cand.exists():
            return cand
        i += 1


def _backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}_baseline_backup_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    return backup


def update_mik_paths(db_path: Path, mapping: list[tuple[Path, Path]], table: str = "Song", col: str = "File") -> tuple[int, int]:
    """Update MIK DB file paths based on rename mapping.
    Returns (updated_rows, not_found)."""
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    pairs = [(str(a), str(b)) for a, b in mapping]

    updated = 0
    not_found = 0
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        if col not in cols:
            raise RuntimeError(f"MIK table '{table}' does not have column '{col}'. Found: {cols}")

        for oldp, newp in pairs:
            cur.execute(f"UPDATE {table} SET {col} = ? WHERE {col} = ?", (newp, oldp))
            if cur.rowcount and cur.rowcount > 0:
                updated += cur.rowcount
            else:
                not_found += 1
        conn.commit()
    finally:
        conn.close()
    return updated, not_found


def rename_from_csv(csv_path: Path, apply_changes: bool) -> Tuple[int, int, int, list[tuple[Path, Path]]]:
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        cols = [c.strip() for c in (reader.fieldnames or [])]
        required = {"file_path", "suggested_filename"}
        missing = required - set(cols)
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")

        if "apply" not in cols:
            raise ValueError('CSV must include an "apply" column. Mark rows you want to rename with y/yes/true/1.')

        scanned = 0
        to_apply = 0
        renamed = 0

        for row in reader:
            scanned += 1
            if not _is_truthy(row.get("apply", "")):
                continue

            src = Path(row["file_path"])
            suggested = (row.get("suggested_filename") or "").strip()
            if not suggested:
                continue

            to_apply += 1

            if not src.exists():
                print(f"SKIP (missing): {src}")
                continue

            dst = src.with_name(suggested)
            if dst.resolve() == src.resolve():
                print(f"NO CHANGE: {src.name}")
                continue

            dst = _dedupe_target(dst)
            mapping.append((src, dst))

            if apply_changes:
                try:
                    src.rename(dst)
                    renamed += 1
                    print(f"RENAMED: {src} -> {dst}")
                except Exception as e:
                    print(f"ERROR renaming: {src} -> {dst} | {e}")
            else:
                print(f"DRY-RUN: {src} -> {dst}")

    return scanned, to_apply, renamed, mapping


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Baseline filename rename, apply from edited CSV.")
    ap.add_argument("csv", help="Edited filename_suggestions.csv (must include 'apply' column)")
    ap.add_argument("--update-mik", action="store_true", help="Update Mixed In Key DB paths after renaming")
    ap.add_argument("--mik-db", default="", help="Path to MIKStore.db (auto-detect in GUI)")

    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Actually rename files")
    mode.add_argument("--dry-run", action="store_true", help="Show what would happen (default)")
    args = ap.parse_args(argv)

    apply_changes = bool(args.apply)
    csv_path = Path(args.csv)

    scanned, to_apply, renamed, mapping = rename_from_csv(csv_path, apply_changes=apply_changes)

    print(f"Scanned rows: {scanned}")
    print(f"Marked to apply: {to_apply}")
    if apply_changes:
        print(f"Renamed: {renamed}")
    else:
        print("Dry-run mode: no renames performed.")

    if apply_changes and args.update_mik:
        if not args.mik_db:
            print("MIK update requested but --mik-db not provided, skipping.")
        else:
            dbp = Path(args.mik_db)
            try:
                backup = _backup_db(dbp)
                print(f"MIK DB backup created: {backup}")
                updated, not_found = update_mik_paths(dbp, mapping)
                print(f"MIK paths updated: {updated}")
                print(f"Not found in MIK DB: {not_found}")
            except Exception as e:
                print(f"ERROR updating MIK DB paths: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
