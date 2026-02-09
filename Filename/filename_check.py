#!/usr/bin/env python3
"""
Baseline, Filename Check (suggestions only)

Scans audio files, builds the expected filename from tags based on Baseline rules,
and writes a CSV of suggestions. Does NOT rename anything.

Rules enforced (as per Alex):
- Filename: {Artist} - {Title} ({Remix}).ext
- Featuring and presenting markers belong in the Artist field:
    feat., ft., featuring, pres., presents, present
  Normalise to "feat." and "pres."
- Remix must be in parentheses at the end if present (from tags or parsed).
- Suggestions only.

Examples:
Expected:
  Aly & Fila feat. Sue McLaren - I Can Hear You (Den Rize & Mark Andrez Remix).flac

Incorrect:
  Aly & Fila - I Can Hear You feat. Sue McLaren (Den Rize & Mark Andrez Remix).flac
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from mutagen import File as MutagenFile


AUDIO_EXTS = {".mp3", ".flac", ".aiff", ".aif", ".m4a", ".mp4", ".ogg", ".opus", ".wav"}


def _safe_get(tags, key: str) -> Optional[str]:
    """
    Safely read a tag value. Some mutagen tag containers raise ValueError for non-ASCII keys,
    or unsupported key formats. We treat those as missing.
    """
    if tags is None:
        return None
    try:
        if hasattr(tags, "get"):
            v = tags.get(key)
        else:
            v = tags[key] if key in tags else None
    except Exception:
        return None

    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return str(v[0]).strip() if v else None
    return str(v).strip()


def _get_first(tags, keys: Iterable[str]) -> Optional[str]:
    for k in keys:
        v = _safe_get(tags, k)
        if v:
            return v
    return None


def _read_tags(path: Path):
    """
    Prefer Mutagen "easy" tags to avoid format-specific key quirks.
    Fall back to raw tags if needed.
    """
    try:
        audio = MutagenFile(str(path), easy=True)
        if audio and getattr(audio, "tags", None):
            return audio.tags
    except Exception:
        pass

    try:
        audio = MutagenFile(str(path), easy=False)
        if audio and getattr(audio, "tags", None):
            return audio.tags
    except Exception:
        pass

    return None


_RE_FEAT = re.compile(r"\b(?:feat\.|ft\.|featuring)\b", re.IGNORECASE)
_RE_PRES = re.compile(r"\b(?:pres\.|presents|present)\b", re.IGNORECASE)

_RE_SPLIT_FEAT = re.compile(r"\b(?:feat\.|ft\.|featuring)\b\.?\s*", re.IGNORECASE)
_RE_SPLIT_PRES = re.compile(r"\b(?:pres\.|presents|present)\b\.?\s*", re.IGNORECASE)

_RE_REMIX_PARENS_AT_END = re.compile(r"\s*\(([^)]+)\)\s*$")


def _clean_spaces(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s


def _sanitize_filename(s: str) -> str:
    # Windows-forbidden characters and general tidying
    s = (s or "").strip()
    s = s.replace("/", "-")
    s = re.sub(r'[<>:"\\|?*]', "", s)
    s = _clean_spaces(s)
    return s


def _normalise_feat_pres_segment(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Returns (base_artist, feat_artist, pres_text)
    Only extracts if markers exist, otherwise returns base only.
    """
    if not text:
        return "", None, None

    base = text
    feat = None
    pres = None

    # Extract pres. first (rarely appears after feat, but can)
    if _RE_PRES.search(base):
        parts = _RE_SPLIT_PRES.split(base, maxsplit=1)
        if len(parts) == 2:
            base = parts[0].strip()
            pres = parts[1].strip()

    # Extract feat.
    if _RE_FEAT.search(base):
        parts = _RE_SPLIT_FEAT.split(base, maxsplit=1)
        if len(parts) == 2:
            base = parts[0].strip()
            feat = parts[1].strip()

    return _clean_spaces(base), _clean_spaces(feat) if feat else None, _clean_spaces(pres) if pres else None


def _extract_feat_or_pres_from_title(title: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    If title contains feat/pres markers, extract them and return (clean_title, feat, pres).
    This supports your example where feat was incorrectly in the title.
    """
    if not title:
        return "", None, None

    t = title
    feat = None
    pres = None

    # Try pres in title
    if _RE_PRES.search(t):
        parts = _RE_SPLIT_PRES.split(t, maxsplit=1)
        if len(parts) == 2:
            t = parts[0].strip()
            pres = parts[1].strip()

    # Try feat in title
    if _RE_FEAT.search(t):
        parts = _RE_SPLIT_FEAT.split(t, maxsplit=1)
        if len(parts) == 2:
            t = parts[0].strip()
            feat = parts[1].strip()

    return _clean_spaces(t), _clean_spaces(feat) if feat else None, _clean_spaces(pres) if pres else None


def _extract_remix(title: str, mixname: Optional[str]) -> Tuple[str, Optional[str]]:
    """
    Enforce remix in parentheses at the end.
    If mixname is present, use it.
    Else parse trailing parentheses from title.
    Returns (clean_title_without_remix, remix_or_none)
    """
    if mixname:
        remix = _clean_spaces(mixname)
        clean_title = _clean_spaces(title)
        # Remove a trailing parens remix from title if it duplicates mixname-ish
        m = _RE_REMIX_PARENS_AT_END.search(clean_title)
        if m:
            clean_title = _clean_spaces(clean_title[: m.start()])
        return clean_title, remix

    clean_title = _clean_spaces(title)
    m = _RE_REMIX_PARENS_AT_END.search(clean_title)
    if not m:
        return clean_title, None

    remix = _clean_spaces(m.group(1))
    clean_title = _clean_spaces(clean_title[: m.start()])
    return clean_title, remix


def suggest_filename(path: Path) -> Tuple[Optional[str], List[str]]:
    tags = _read_tags(path)

    # Use easy keys first. Fall back lists are safe ASCII.
    artist = _get_first(tags, ["artist", "ARTIST"])
    title = _get_first(tags, ["title", "TITLE"])

    # Optional mix/remix tags (easy tags vary)
    mixname = _get_first(tags, ["mixname", "remix", "version", "mix", "MIXNAME", "REMIX", "VERSION"])

    if not artist or not title:
        reasons = []
        if not artist:
            reasons.append("missing artist tag")
        if not title:
            reasons.append("missing title tag")
        return None, reasons

    reasons: List[str] = []

    # Parse artist for feat/pres
    base_artist, feat_from_artist, pres_from_artist = _normalise_feat_pres_segment(artist)

    # Parse title for feat/pres if present, move to artist
    clean_title, feat_from_title, pres_from_title = _extract_feat_or_pres_from_title(title)

    if feat_from_title:
        reasons.append("feat moved from title to artist")
    if pres_from_title:
        reasons.append("pres moved from title to artist")

    feat = feat_from_artist or feat_from_title
    pres = pres_from_artist or pres_from_title

    # Extract remix
    clean_title, remix = _extract_remix(clean_title, mixname)
    if remix and _RE_REMIX_PARENS_AT_END.search(title) is None and mixname:
        reasons.append("remix enforced from tag")
    elif remix and _RE_REMIX_PARENS_AT_END.search(title):
        reasons.append("remix normalised to parentheses at end")

    # Build artist string
    artist_out = base_artist
    if pres:
        artist_out = f"{artist_out} pres. {pres}".strip()
    if feat:
        artist_out = f"{artist_out} feat. {feat}".strip()

    # Build filename core
    out = f"{artist_out} - {clean_title}"
    if remix:
        out = f"{out} ({remix})"

    out = _sanitize_filename(out)

    return out, reasons


def iter_audio_files(root: Path, recursive: bool) -> Iterable[Path]:
    if root.is_file():
        if root.suffix.lower() in AUDIO_EXTS:
            yield root
        return

    if recursive:
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
                yield p
    else:
        for p in root.iterdir():
            if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
                yield p


def current_stem(path: Path) -> str:
    return path.stem


def write_m3u8(paths: List[Path], out_path: Path):
    # Simple UTF-8 m3u8
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")
        for p in paths:
            f.write(str(p).replace("\\", "/") + "\n")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Baseline filename check, suggestions only (no renames).")
    ap.add_argument("root", help="Root folder or file to scan")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--recursive", action="store_true", help="Scan subfolders")
    ap.add_argument("--m3u8", action="store_true", help="Also write a review playlist (.m3u8) next to CSV")
    args = ap.parse_args(argv)

    root = Path(args.root)
    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    review_paths: List[Path] = []
    scanned = 0
    changed = 0

    for p in iter_audio_files(root, recursive=args.recursive):
        scanned += 1
        suggested_core, reasons = suggest_filename(p)
        if suggested_core is None:
            rows.append(
                {
                    "file_path": str(p),
                    "current_filename": p.name,
                    "suggested_filename": "",
                    "reasons": "; ".join(reasons) if reasons else "missing tags",
                    "apply": "",
                }
            )
            continue

        suggested_name = f"{suggested_core}{p.suffix.lower()}"
        if suggested_name != p.name:
            changed += 1
            review_paths.append(p)

        rows.append(
            {
                "file_path": str(p),
                "current_filename": p.name,
                "suggested_filename": suggested_name,
                "reasons": "; ".join(reasons),
                "apply": "",
            }
        )

    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["file_path", "current_filename", "suggested_filename", "reasons", "apply"])
        w.writeheader()
        w.writerows(rows)

    if args.m3u8:
        out_m3u8 = out_csv.with_suffix(".m3u8")
        write_m3u8(review_paths, out_m3u8)

    print(f"Scanned: {scanned}")
    print(f"Suggestions: {changed}")
    print(f"Wrote CSV: {out_csv}")
    if args.m3u8:
        print(f"Wrote M3U8: {out_csv.with_suffix('.m3u8')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
