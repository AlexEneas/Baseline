#!/usr/bin/env python3
import argparse, hashlib, io, os, re, sys, time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import requests

# ===================== Color output =====================
USE_COLOR = True
try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)
    C_OK     = Fore.GREEN + Style.BRIGHT
    C_WARN   = Fore.YELLOW + Style.BRIGHT
    C_ERR    = Fore.RED + Style.BRIGHT
    C_INFO   = Fore.CYAN + Style.BRIGHT
    C_DIM    = Style.DIM
    C_RESET  = Style.RESET_ALL
except Exception:
    USE_COLOR = False
    class Dummy:
        def __getattr__(self, _): return ""
    Fore = Style = Dummy()
    C_OK = C_WARN = C_ERR = C_INFO = C_DIM = C_RESET = ""

def cstr(s: str, color: str) -> str:
    return f"{color}{s}{C_RESET}" if USE_COLOR else s

def ok(s: str) -> str:   return cstr(s, C_OK)
def warn(s: str) -> str: return cstr(s, C_WARN)
def err(s: str) -> str:  return cstr(s, C_ERR)
def info(s: str) -> str: return cstr(s, C_INFO)
def dim(s: str) -> str:  return cstr(s, C_DIM)

# ===================== Imaging =====================
try:
    from PIL import Image
    PIL_AVAILABLE = True
except Exception:
    Image = None
    PIL_AVAILABLE = False
    print(warn("[INFO]")) if USE_COLOR else None
    print("[INFO] Pillow not installed; album-art size checks/embeds will be disabled.")

def image_size_from_bytes(img_bytes: bytes) -> Tuple[int, int]:
    if not PIL_AVAILABLE:
        return (0, 0)
    im = Image.open(io.BytesIO(img_bytes))
    return im.width, im.height

def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

# ===================== Config =====================
DISCOGS_KEY    = os.getenv("DISCOGS_KEY",    "xACNKfxTAAJOoYlrUPWT")
DISCOGS_SECRET = os.getenv("DISCOGS_SECRET", "OWMNTnnqQgCHPlIJHIHYTmOqCuVEAHWJ")
DISCOGS_TOKEN  = os.getenv("DISCOGS_TOKEN", "")  # optional

USER_AGENT     = "Discogs-Tag-Art-Fixer/ArtOnly-1.0 (+https://github.com/AlexEneas/Discogs-Tag-Art-Fixer)"
DISCOGS_ACCEPT = "application/vnd.discogs.v2.discogs+json"
DISCOGS_SEARCH = "https://api.discogs.com/database/search"

MIN_ART_SIZE_DEFAULT = 500
PLACEHOLDER_FILENAME = "placeholder.jpg"

VERBOSE = False

# ===================== Mutagen (tags) =====================
try:
    from mutagen import MutagenError, File as MutagenFile
    from mutagen.id3 import ID3, APIC, ID3NoHeaderError
    from mutagen.flac import FLAC, Picture
    from mutagen.mp4 import MP4, MP4Cover
except Exception:
    print(err("ERROR: mutagen is required. Install with:  python -m pip install mutagen"))
    sys.exit(1)

ART_EXTS = {".mp3", ".flac", ".m4a", ".mp4", ".alac"}

# ===================== Helpers =====================
def base_headers() -> Dict[str, str]:
    h = {"User-Agent": USER_AGENT, "Accept": DISCOGS_ACCEPT}
    if DISCOGS_KEY and DISCOGS_SECRET:
        h["Authorization"] = f"Discogs key={DISCOGS_KEY}, secret={DISCOGS_SECRET}"
    return h

def auth_params(base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = dict(base or {})
    if DISCOGS_TOKEN:
        p["token"] = DISCOGS_TOKEN
    if DISCOGS_KEY and DISCOGS_SECRET:
        p.setdefault("key", DISCOGS_KEY)
        p.setdefault("secret", DISCOGS_SECRET)
    return p

def debug_rate(resp):
    if not VERBOSE: return
    rl  = resp.headers.get("X-Discogs-Ratelimit")
    use = resp.headers.get("X-Discogs-Ratelimit-Used")
    rem = resp.headers.get("X-Discogs-Ratelimit-Remaining")
    if rl or use or rem:
        print(dim(f"      [rate] limit={rl} used={use} remaining={rem}"))

def smart_throttle(resp, base_delay: float):
    try:
        remaining = int(resp.headers.get("X-Discogs-Ratelimit-Remaining", "0"))
        limit     = int(resp.headers.get("X-Discogs-Ratelimit", "0"))
    except ValueError:
        remaining = 0
        limit = 0
    sleep_for = max(base_delay, 0.0)
    if limit and remaining <= max(1, int(0.1 * limit)):
        sleep_for = max(sleep_for, 0.5)
    if sleep_for > 0:
        time.sleep(sleep_for)

class RetryableDiscogsError(Exception): ...

def fetch_json(url: str, params: Optional[Dict[str, Any]] = None, delay: float = 0.0, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    sess = session or requests.Session()
    r = sess.get(url, headers=base_headers(), params=auth_params(params or {}), timeout=25)
    if r.status_code == 429:
        if VERBOSE: print(warn("      [limit] 429 Too Many Requests — backoff 2s"))
        time.sleep(2.0)
        raise RetryableDiscogsError("Rate limited (429)")
    if r.status_code in (401,403):
        if VERBOSE: print(warn("      [net] auth fallback → query params only"))
        hdr2 = {"User-Agent": USER_AGENT, "Accept": DISCOGS_ACCEPT}
        r = sess.get(url, headers=hdr2, params=auth_params(params or {}), timeout=25)
    r.raise_for_status()
    debug_rate(r)
    data = r.json()
    smart_throttle(r, delay)
    return data

def normalize(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^\w\s&]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def tokens(s: str) -> set:
    return set(normalize(s).split())

def title_similarity(a: str, b: str) -> float:
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb: return 0.0
    return len(ta & tb) / len(ta | tb)

def coerce_year(value) -> str:
    if value is None: return ""
    if isinstance(value, int): return str(value) if 1900 <= value <= 2100 else ""
    s = str(value).replace("\\", "/")
    parts = s.split("/")
    if parts and len(parts[0]) == 4 and parts[0].isdigit():
        y0 = int(parts[0]); return str(y0) if 1900 <= y0 <= 2100 else ""
    m = re.search(r'(?<!\d)(19\d{2}|20\d{2}|2100)(?!\d)', s)
    if not m: return ""
    y = int(m.group(0)); return str(y) if 1900 <= y <= 2100 else ""

# ===================== Tag reading (Artist/Title/Year) =====================
MIX_RE = re.compile(r"\(([^)]+)\)", flags=re.IGNORECASE)

def parse_filename(name: str) -> Tuple[str, str]:
    stem = Path(name).stem
    parts = stem.split(" - ", 1)
    if len(parts) != 2:
        return ("", stem.strip())
    return parts[0].strip(), re.sub(r"\s*\([^)]*\)\s*", " ", parts[1]).strip()

def read_artist_title_year(path: Path) -> Tuple[str, str, str]:
    artist, title, year = "", "", ""
    try:
        audio = MutagenFile(path)
        if audio is None: raise MutagenError("Unrecognized format")
        ext = path.suffix.lower()

        if ext == ".mp3":
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = None
            if tags:
                if tags.get("TPE1") and tags["TPE1"].text:
                    artist = str(tags["TPE1"].text[0]).strip()
                if tags.get("TIT2") and tags["TIT2"].text:
                    title = str(tags["TIT2"].text[0]).strip()
                # Year from TDRC/TYER if present (best-effort)
                if tags.get("TDRC") and tags["TDRC"].text:
                    year = coerce_year(tags["TDRC"].text[0])
                elif tags.get("TYER") and tags["TYER"].text:
                    year = coerce_year(tags["TYER"].text[0])

        elif ext == ".flac":
            if "artist" in audio and audio["artist"]:
                artist = artist or audio["artist"][0].strip()
            if "title" in audio and audio["title"]:
                title = title or audio["title"][0].strip()
            # YEAR / DATE
            if "date" in audio and audio["date"]:
                year = coerce_year(audio["date"][0])
            elif "year" in audio and audio["year"]:
                year = coerce_year(audio["year"][0])

        elif ext in (".m4a",".mp4",".alac"):
            if audio.tags:
                if "\xa9ART" in audio.tags and audio.tags["\xa9ART"]:
                    artist = artist or str(audio.tags["\xa9ART"][0]).strip()
                if "\xa9nam" in audio.tags and audio.tags["\xa9nam"]:
                    title = title or str(audio.tags["\xa9nam"][0]).strip()
                if "\xa9day" in audio.tags and audio.tags["\xa9day"]:
                    year = coerce_year(audio.tags["\xa9day"][0])

        # fallback filename if missing
        if not artist or not title:
            fa, ft = parse_filename(path.name)
            artist = artist or fa
            title  = title  or ft

        # strip parentheses content from title for search
        title = re.sub(r"\s*\([^)]*\)\s*", " ", title or "").strip()

    except MutagenError:
        fa, ft = parse_filename(path.name)
        artist, title = fa, ft

    return artist or "", title or "", year or ""

# ===================== Artwork R/W =====================
def read_embedded_art(path: Path) -> Optional[bytes]:
    try:
        ext = path.suffix.lower()
        if ext == ".mp3":
            try: tags = ID3(path)
            except ID3NoHeaderError: return None
            apics = tags.getall("APIC")
            return bytes(apics[0].data) if apics else None
        elif ext == ".flac":
            audio = FLAC(path)
            return bytes(audio.pictures[0].data) if audio.pictures else None
        elif ext in (".m4a", ".mp4", ".alac"):
            audio = MP4(path)
            covr = audio.tags.get("covr")
            if covr and len(covr) > 0:
                return bytes(covr[0])
    except MutagenError:
        return None
    return None

def remove_all_art(path: Path) -> None:
    try:
        ext = path.suffix.lower()
        if ext == ".mp3":
            try: tags = ID3(path)
            except ID3NoHeaderError: return
            tags.delall("APIC"); tags.save(path)
        elif ext == ".flac":
            audio = FLAC(path); audio.clear_pictures(); audio.save()
        elif ext in (".m4a", ".mp4", ".alac"):
            audio = MP4(path)
            if "covr" in audio.tags:
                audio.tags["covr"] = []
                audio.save()
    except MutagenError as e:
        print(warn(f"   [WARN] Failed clearing existing art: {e}"))

def write_single_cover(path: Path, img_bytes: bytes, mime: str = "image/jpeg") -> bool:
    if not PIL_AVAILABLE:
        return False
    try:
        ext = path.suffix.lower()
        if ext == ".mp3":
            try: tags = ID3(path)
            except ID3NoHeaderError: tags = ID3()
            tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=img_bytes))
            tags.save(path); return True
        elif ext == ".flac":
            audio = FLAC(path)
            w,h = image_size_from_bytes(img_bytes)
            pic = Picture(); pic.type=3; pic.mime=mime; pic.desc="Cover"; pic.width=w; pic.height=h; pic.depth=24; pic.data=img_bytes
            audio.add_picture(pic); audio.save(); return True
        elif ext in (".m4a", ".mp4", ".alac"):
            audio = MP4(path)
            fmt = MP4Cover.FORMAT_JPEG
            if mime.lower() == "image/png":
                fmt = MP4Cover.FORMAT_PNG
            audio["covr"] = [MP4Cover(img_bytes, imageformat=fmt)]
            audio.save(); return True
    except MutagenError as e:
        print(warn(f"   [WARN] Failed writing art: {e}"))
        return False
    return False

# ===================== Discogs Search for Artwork =====================
def choose_best_image(images: List[Dict[str, Any]], min_size: int) -> Optional[str]:
    if not images: return None
    ranked = []
    for img in images:
        w, h = int(img.get("width",0)), int(img.get("height",0))
        uri = img.get("uri") or img.get("resource_url")
        if not uri: continue
        # prefer primary and bigger area
        score = (w*h) + (1_000_000 if img.get("type") == "primary" else 0)
        ranked.append((score, w, h, uri))
    if not ranked: return None
    ranked.sort(reverse=True)
    # pick the first that meets min size; else the biggest
    for _, w, h, uri in ranked:
        if max(w,h) >= min_size:
            return uri
    return ranked[0][3]

def download_image(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=base_headers(), params=auth_params({}), timeout=30)
        r.raise_for_status()
        return r.content
    except requests.RequestException:
        return None

def rank_results_for_art(results: List[Dict[str, Any]], artist: str, title: str, want_year: str) -> Optional[Dict[str, Any]]:
    """Prefer strong title/artist match and a year matching the tag year (if present)."""
    artist_n, title_n = normalize(artist), normalize(title)
    best, best_score = None, -1.0
    for res in results:
        res_title = res.get("title","")
        # Split "Artist - Title"
        if " - " in res_title:
            r_artist, r_title = res_title.split(" - ", 1)
        else:
            r_artist, r_title = "", res_title
        a_score = 1.0 if artist_n and (artist_n in normalize(r_artist) or normalize(r_artist) in artist_n) else title_similarity(artist_n, r_artist)
        t_score = title_similarity(title_n, normalize(r_title))
        y_bonus = 0.0
        ry = coerce_year(res.get("year", ""))
        if want_year and ry and want_year == ry:
            y_bonus = 0.35  # big bonus for exact year match
        # master results get slight boost (often better art context)
        type_bonus = 0.15 if res.get("type") == "master" else 0.0
        score = 0.5*a_score + 0.5*t_score + y_bonus + type_bonus
        if score > best_score:
            best, best_score = res, score
    if best and best_score >= 0.35:
        best["_match_score"] = round(best_score, 3)
        return best
    return None

def discogs_find_art(artist: str, title: str, want_year: str, delay: float, min_art: int) -> Tuple[Optional[str], str]:
    """
    Return (image_url, source_kind) where source_kind in {'release','master','none'}
    """
    sess = requests.Session()
    queries = []
    if artist and title:
        queries.append({"artist": artist, "track": title})
        queries.append({"artist": artist, "title": title})
    queries.append({"q": f"{artist} {title}".strip()})

    for base in queries:
        try:
            data = fetch_json(DISCOGS_SEARCH, params={**base, "type":"release", "per_page": "10", "sort": "relevance"}, delay=delay, session=sess)
        except RetryableDiscogsError:
            raise
        except requests.RequestException as e:
            raise RetryableDiscogsError(f"Search failed: {e}")

        results = data.get("results", [])
        if VERBOSE: print(dim(f"      [net] search results: {len(results)}"))
        if not results:
            time.sleep(delay); continue

        ranked = rank_results_for_art(results, artist, title, want_year)
        if not ranked:
            time.sleep(delay); continue

        # Try release details first (better for concrete cover art)
        res_url = ranked.get("resource_url")
        if res_url:
            try:
                rel = fetch_json(res_url, delay=0.0, session=sess)
                img_url = choose_best_image(rel.get("images", []), min_size=min_art)
                if img_url:
                    return img_url, "release"
                # Fall back to master if linked
                master_id = ranked.get("master_id") or rel.get("master_id")
                if master_id:
                    m = fetch_json(f"https://api.discogs.com/masters/{int(master_id)}", delay=0.0, session=sess)
                    img_url = choose_best_image(m.get("images", []), min_size=min_art)
                    if img_url:
                        return img_url, "master"
            except RetryableDiscogsError:
                raise
            except Exception:
                pass

        time.sleep(delay)

    return None, "none"

# ===================== Scan + Process =====================
def find_target_files(root: Path, recursive: bool) -> List[Path]:
    exts = {e.lower() for e in ART_EXTS}
    candidates = root.rglob("*") if recursive else root.glob("*")
    seen = set()
    out: List[Path] = []
    for p in candidates:
        if p.is_file() and p.suffix.lower() in exts:
            key = str(p.resolve()).lower()
            if key not in seen:
                seen.add(key); out.append(p)
    return sorted(out)

def needs_artwork(path: Path, min_art: int, placeholder_md5: Optional[str]) -> Tuple[bool, str]:
    if not PIL_AVAILABLE:
        return (False, "pillow_missing")
    emb = read_embedded_art(path)
    if emb is None:
        return (True, "missing")
    try:
        w,h = image_size_from_bytes(emb)
    except Exception:
        w,h = (0,0)
    if max(w,h) < min_art:
        return (True, f"too_small ({w}x{h})")
    if placeholder_md5 and md5_bytes(emb) == placeholder_md5:
        return (True, "placeholder")
    return (False, "ok")

def process_file(path: Path, args, placeholder_md5: Optional[str]) -> str:
    artist, title, year = read_artist_title_year(path)
    print(f"    {info('Check:')} {artist} - {title}" + (f" [{year}]" if year else ""))

    # Decide if we need to touch it
    need, reason = needs_artwork(path, args.min_art, placeholder_md5)
    if not need:
        print(f"      {dim('skip')} existing art OK")
        return "kept"

    # Lookup art
    try:
        img_url, src = discogs_find_art(artist, title, year, args.delay, args.min_art)
    except RetryableDiscogsError as e:
        print(warn(f"      [RETRY] {e}"))
        return "retry"

    if not img_url:
        print(err("      [MISS] No suitable artwork found"))
        return "miss"

    img_bytes = download_image(img_url)
    if not img_bytes:
        print(err("      [MISS] Failed to download artwork"))
        return "miss"

    # Embed
    mime = "image/png" if img_url.lower().endswith(".png") else "image/jpeg"
    remove_all_art(path)
    if write_single_cover(path, img_bytes, mime=mime):
        try:
            w,h = image_size_from_bytes(img_bytes)
            print(f"      {ok('✓')} added {w}x{h} ({src})")
        except Exception:
            print(f"      {ok('✓')} added ({src})")
        return "updated"

    print(err("      [ERROR] Failed to write cover"))
    return "error"

# ===================== Main =====================
def main():
    ap = argparse.ArgumentParser(description="Discogs Artwork Updater — updates missing/placeholder/small art using Artist/Title/Year tags.")
    ap.add_argument("folder", help="Folder to scan")
    ap.add_argument("-r","--recursive", action="store_true", help="Scan subfolders")
    ap.add_argument("--min-art", type=int, default=MIN_ART_SIZE_DEFAULT, help="Minimum artwork size in px (default 500)")
    ap.add_argument("--delay", type=float, default=0.6, help="Base delay between Discogs calls (seconds)")
    ap.add_argument("--placeholder", default=PLACEHOLDER_FILENAME, help="Placeholder image filename to detect (default placeholder.jpg)")
    ap.add_argument("--token", help="Discogs personal access token (optional; overrides env)")
    ap.add_argument("--verbose", action="store_true", help="Verbose HTTP logging")
    ap.add_argument("--no-color", action="store_true", help="Disable colored output")
    args = ap.parse_args()

    global DISCOGS_TOKEN, VERBOSE, USE_COLOR
    if args.token:
        DISCOGS_TOKEN = args.token
    VERBOSE = args.verbose
    if args.no_color:
        USE_COLOR = False

    if not (DISCOGS_KEY and DISCOGS_SECRET) and not DISCOGS_TOKEN:
        print(err("ERROR: Provide Discogs credentials. Either:"))
        print("  - keep the hardcoded key/secret, or")
        print("  - set DISCOGS_KEY/DISCOGS_SECRET env vars, or")
        print("  - pass a personal token via --token / set DISCOGS_TOKEN")
        sys.exit(1)

    root = Path(args.folder)
    if not root.exists() or not root.is_dir():
        print(err(f"ERROR: Folder '{root}' not found or not a directory."))
        sys.exit(1)

    # Placeholder fingerprint (optional)
    placeholder_md5 = None
    if args.placeholder:
        p = (Path(__file__).parent / args.placeholder)
        if p.exists():
            try:
                placeholder_md5 = md5_bytes(p.read_bytes())
                print(dim(f"Loaded placeholder '{args.placeholder}' MD5: {placeholder_md5}"))
            except Exception as e:
                print(warn(f"[WARN] Could not read placeholder file: {e}"))

    files = find_target_files(root, args.recursive)
    if not files:
        print(warn("No supported audio files found (mp3, flac, m4a/mp4/alac)."))
        sys.exit(0)

    print(info(f"Scanning {len(files)} file(s)...\n"))

    stats = {"updated":0, "kept":0, "miss":0, "retry":0, "error":0}
    for i, f in enumerate(files, 1):
        print(f"{info('['+str(i)+'/'+str(len(files))+']')} {f.name}")
        try:
            res = process_file(f, args, placeholder_md5)
            stats[res] = stats.get(res, 0) + 1
        except RetryableDiscogsError as e:
            print(warn(f"   [RETRY] {e}"))
            stats["retry"] += 1
        except Exception as e:
            print(err(f"   [ERROR] {e}"))
            stats["error"] += 1

    print("\n" + info("Summary"))
    print(f"  {ok('updated')} : {stats['updated']}")
    print(f"  {dim('kept')}    : {stats['kept']}")
    print(f"  {warn('miss')}    : {stats['miss']}")
    print(f"  {warn('retry')}   : {stats['retry']}")
    print(f"  {err('error')}   : {stats['error']}")

if __name__ == "__main__":
    main()
