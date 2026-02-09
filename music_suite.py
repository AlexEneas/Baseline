#!/usr/bin/env python3

r"""
Music Maintenance Suite Launcher

One entry-point to run your existing scripts (Mixed In Key + Discogs tools) via subcommands.

Examples:
  python music_suite.py ? 
  python music_suite.py mik prune-missing "C:\Path\MIKStore.db" --dry-run --report "missing.txt"
  python music_suite.py mik sync-tags     "C:\Path\MIKStore.db" --dry-run --report "changes.csv"
  python music_suite.py mik sync-artwork  "C:\Path\MIKStore.db" --dry-run --report "art.csv"

  python music_suite.py discogs updateart update "D:\Music" -r --min-art 500
  python music_suite.py discogs years-labels-art "D:\Music" -r
  python music_suite.py discogs art-only "D:\Music" -r

Notes:
  - This launcher passes arguments through to the underlying script unchanged.
  - Close Mixed In Key before running commands that modify the DB.
"""

import sys
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parent

def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {module_path}")
    mod = importlib.util.module_from_spec(spec)
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    except Exception:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous
        raise
    return mod

def _print_help():
    print("Music Maintenance Suite")
    print()
    print("Usage:")
    print("  python music_suite.py <group> <tool> [tool-args...]")
    print()
    print("Groups and tools:")
    print("  mik")
    print("    prune-missing      -> MixedinKey/mik_prune_missing.py")
    print("    sync-tags          -> MixedinKey/mik_sync_tags_from_files.py")
    print("    sync-artwork       -> MixedinKey/mik_sync_artwork.py")
    print()
    print("  discogs")
    print("    updateart          -> Discogs/UpdateArt.py (has its own subcommands: update, scan, etc)")
    print("    years-labels-art   -> Discogs/discogs_years_labels_art.py")
    print("    art-only           -> Discogs/discogs_art_only.py")
    print("    playlistupdate     -> Discogs/PlaylistUpdate.py")
    print()
    print("Examples:")
    print(r'  python music_suite.py mik prune-missing "C:\Path\MIKStore.db" --dry-run --report "missing.txt"')
    print(r'  python music_suite.py mik sync-tags "C:\Path\MIKStore.db" --dry-run --report "changes.csv"')
    print(r'  python music_suite.py mik sync-artwork "C:\Path\MIKStore.db" --dry-run --only-existing-files --report "art.csv"')
    print(r'  python music_suite.py discogs updateart update "D:\Music" -r --min-art 500')
    print(r'  python music_suite.py discogs years-labels-art "D:\Music" -r')
    print()

def main() -> int:
    if len(sys.argv) == 1 or sys.argv[1] in ("?", "help", "-h", "--help"):
        _print_help()
        return 0

    group = sys.argv[1].lower()
    tool  = sys.argv[2].lower() if len(sys.argv) > 2 else ""

    mapping = {
        ("mik", "prune-missing"): ROOT / "MixedinKey" / "mik_prune_missing.py",
        ("mik", "sync-tags"): ROOT / "MixedinKey" / "mik_sync_tags_from_files.py",
        ("mik", "sync-artwork"): ROOT / "MixedinKey" / "mik_sync_artwork.py",

        ("discogs", "updateart"): ROOT / "Discogs" / "UpdateArt.py",
        ("discogs", "years-labels-art"): ROOT / "Discogs" / "discogs_years_labels_art.py",
        ("discogs", "art-only"): ROOT / "Discogs" / "discogs_art_only.py",
        ("discogs", "playlistupdate"): ROOT / "Discogs" / "PlaylistUpdate.py",
    }

    key = (group, tool)
    if key not in mapping:
        print(f"Unknown command: {group} {tool}")
        print()
        _print_help()
        return 2

    script_path = mapping[key]
    if not script_path.exists():
        print(f"ERROR: Tool script not found: {script_path}")
        return 2

    # Load tool module and call its main(), while passing through arguments.
    mod = _load_module(script_path, f"{group}_{tool}".replace("-", "_"))

    if not hasattr(mod, "main"):
        print(f"ERROR: Tool does not expose a main() function: {script_path}")
        return 2

    # Rewrite argv for the tool: keep original program name, drop group+tool.
    tool_argv = [str(script_path)] + sys.argv[3:]
    old_argv = sys.argv
    try:
        sys.argv = tool_argv
        return int(mod.main())
    finally:
        sys.argv = old_argv

if __name__ == "__main__":
    raise SystemExit(main())
