#!/usr/bin/env python3
import argparse, csv, hashlib, io, re, sys, time, os
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

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

# Pillow for image size/format (art updates use it)
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    Image = None
    PIL_AVAILABLE = False
    print("[INFO] Pillow not installed; album-art size checks/embeds will be partially disabled.")

# ===================== CONFIG =====================
# Discogs credentials (hardcoded but overridable via env)
DISCOGS_KEY    = os.getenv("DISCOGS_KEY",    "xACNKfxTAAJOoYlrUPWT")
DISCOGS_SECRET = os.getenv("DISCOGS_SECRET", "OWMNTnnqQgCHPlIJHIHYTmOqCuVEAHWJ")
# Optional Personal Token (env or --token). Not required for this flow.
DISCOGS_TOKEN  = os.getenv("DISCOGS_TOKEN", "")

DISCOGS_SEARCH = "https://api.discogs.com/database/search"
USER_AGENT     = "Discogs-Tag-Art-Fixer/3.7 (+https://github.com/AlexEneas/Discogs-Tag-Art-Fixer)"
DISCOGS_ACCEPT = "application/vnd.discogs.v2.discogs+json"

PLACEHOLDER_FILENAME = "placeholder.jpg"
MIN_ART_SIZE = 500
RETRY_MAX_ROUNDS = 3
VERBOSE = False  # set from --verbose
# ==================================================

# --- mutagen (tags) ---
try:
    from mutagen import MutagenError, File as MutagenFile
    from mutagen.id3 import ID3, APIC, TDRC, TYER, TXXX, TPUB, ID3NoHeaderError
    from mutagen.flac import FLAC, Picture
    from mutagen.mp4 import MP4, MP4Cover, MP4FreeForm
    from mutagen.oggvorbis import OggVorbis
    from mutagen.oggopus import OggOpus
    from mutagen.aiff import AIFF
    from mutagen.wave import WAVE
    from mutagen.asf import ASF
except ImportError:
    print(err("ERROR: mutagen is required. Install with:  python -m pip install mutagen"))
    sys.exit(1)

MIX_RE = re.compile(r"\(([^)]+)\)", flags=re.IGNORECASE)

ALL_AUDIO_EXTS = {
    ".mp3", ".flac", ".m4a", ".mp4", ".alac", ".aac",
    ".wav", ".aif", ".aiff",
    ".ogg", ".oga", ".opus",
    ".wma"
}

# ---------------------- HTTP helpers ----------------------
def base_headers() -> dict:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": DISCOGS_ACCEPT,
    }
    if DISCOGS_KEY and DISCOGS_SECRET:
        h["Authorization"] = f"Discogs key={DISCOGS_KEY}, secret={DISCOGS_SECRET}"
    return h

def auth_params(base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = dict(base or {})
    # Token not strictly required, but supported
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

def handle_429(backoff_seconds: float):
    if VERBOSE:
        print(warn(f"      [limit] 429 Too Many Requests, backing off {backoff_seconds:.1f}s"))
    time.sleep(backoff_seconds)

def fetch_json(url: str, params: Optional[Dict[str, Any]] = None, delay: float = 0.0, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    sess = session or requests.Session()
    headers = base_headers()
    q = auth_params(params or {})
    if VERBOSE:
        preview = {k: q.get(k) for k in ("artist","title","track","q","page","per_page") if k in q}
        print(dim(f"      [net] GET {url}  params={preview}"))
    r = sess.get(url, headers=headers, params=q, timeout=25)
    if r.status_code == 429:
        handle_429(2.0)
        raise RetryableDiscogsError("Rate limited (429)")
    if r.status_code in (401,403):
        # fallback without Authorization header, query params only
        if VERBOSE:
            print(warn("      [net] auth fallback → query params only"))
        hdr2 = {"User-Agent": USER_AGENT, "Accept": DISCOGS_ACCEPT}
        r = sess.get(url, headers=hdr2, params=q, timeout=25)
    r.raise_for_status()
    debug_rate(r)
    data = r.json()
    smart_throttle(r, delay)
    return data

# ---------------------- util ----------------------
def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def image_size_from_bytes(img_bytes: bytes) -> Tuple[int, int]:
    if not PIL_AVAILABLE:
        return (0, 0)
    im = Image.open(io.BytesIO(img_bytes))
    return im.width, im.height

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
    """Return clean 'YYYY' from things like '2025//2025', '2019\\2019', '1999-03-01', or ints."""
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value) if 1900 <= value <= 2100 else ""
    s = str(value)
    s = s.replace("\\", "/")
    parts = s.split("/")
    if parts and len(parts[0]) == 4 and parts[0].isdigit():
        y0 = int(parts[0])
        return str(y0) if 1900 <= y0 <= 2100 else ""
    m = re.search(r'(?<!\d)(19\d{2}|20\d{2}|2100)(?!\d)', s)
    if not m:
        return ""
    y = int(m.group(0))
    return str(y) if 1900 <= y <= 2100 else ""

# ---------------------- filename fallback ----------------------
def parse_filename(name: str) -> Tuple[str, str, Optional[str]]:
    stem = Path(name).stem
    parts = stem.split(" - ", 1)
    if len(parts) != 2:
        return ("", stem.strip(), None)
    artist, right = parts[0].strip(), parts[1].strip()
    mix = None
    paren = MIX_RE.findall(right)
    if paren:
        mix = paren[-1].strip()
        right = MIX_RE.sub("", right).strip()
    title = re.sub(r"\s+", " ", right).strip()
    return (artist, title, mix)

# ---------------------- read artist/title from tags ----------------------
def get_artist_title_from_tags(path: Path) -> Tuple[str, str, Optional[str]]:
    artist, title, mix = "", "", None
    try:
        audio = MutagenFile(path)
        if audio is None:
            raise MutagenError("Unrecognized format")

        # MP3 (ID3)
        if isinstance(audio, ID3) or path.suffix.lower() == ".mp3":
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = None
            if tags:
                if "TPE1" in tags and tags["TPE1"].text:
                    artist = str(tags["TPE1"].text[0]).strip()
                if "TIT2" in tags and tags["TIT2"].text:
                    title = str(tags["TIT2"].text[0]).strip()

        # FLAC / Ogg Vorbis / Opus
        if isinstance(audio, (FLAC, OggVorbis, OggOpus)):
            for k in ("artist","ARTIST"):
                if k in audio and audio[k]:
                    artist = artist or audio[k][0].strip()
            for k in ("title","TITLE"):
                if k in audio and audio[k]:
                    title = title or audio[k][0].strip()

        # MP4/M4A/ALAC
        if isinstance(audio, MP4) or path.suffix.lower() in (".m4a",".mp4",".alac"):
            if audio.tags:
                if "\xa9ART" in audio.tags and audio.tags["\xa9ART"]:
                    artist = artist or str(audio.tags["\xa9ART"][0]).strip()
                if "\xa9nam" in audio.tags and audio.tags["\xa9nam"]:
                    title = title or str(audio.tags["\xa9nam"][0]).strip()

        # WMA (ASF)
        if isinstance(audio, ASF) or path.suffix.lower() == ".wma":
            if audio.tags:
                if "Author" in audio.tags and audio.tags["Author"]:
                    artist = artist or str(audio.tags["Author"][0].value).strip()
                if "Title" in audio.tags and audio.tags["Title"]:
                    title = title or str(audio.tags["Title"][0].value).strip()

        # WAV/AIFF, try ID3
        if isinstance(audio, (WAVE, AIFF)) or path.suffix.lower() in (".wav",".aif",".aiff"):
            try:
                tags = ID3(path)
                if "TPE1" in tags and tags["TPE1"].text:
                    artist = artist or str(tags["TPE1"].text[0]).strip()
                if "TIT2" in tags and tags["TIT2"].text:
                    title = title or str(tags["TIT2"].text[0]).strip()
            except ID3NoHeaderError:
                pass

    except MutagenError:
        pass

    if not artist or not title:
        fa, ft, fm = parse_filename(path.name)
        artist = artist or fa
        title  = title  or ft
        mix    = mix or fm

    if title and mix is None:
        paren = MIX_RE.findall(title)
        if paren: mix = paren[-1].strip()

    title_for_search = re.sub(r"\s*\([^)]*\)\s*", " ", title or "").strip()
    return artist or "", title_for_search or "", mix

# ---------------------- Discogs search + ranking ----------------------
class RetryableDiscogsError(Exception): ...

def rank_results(results: List[Dict[str, Any]], artist: str, title: str, mix: Optional[str]) -> Optional[Dict[str, Any]]:
    artist_n, title_n, mix_n = normalize(artist), normalize(title), normalize(mix) if mix else None
    best, best_score = None, -1.0
    for res in results:
        res_title, res_year, res_type = res.get("title",""), res.get("year"), res.get("type")
        if " - " in res_title:
            r_artist, r_title = res_title.split(" - ", 1)
        else:
            r_artist, r_title = "", res_title
        r_artist_n, r_title_n = normalize(r_artist), normalize(r_title)
        artist_score = 1.0 if artist_n and (artist_n in r_artist_n or r_artist_n in artist_n) else title_similarity(artist_n, r_artist_n)
        title_score  = title_similarity(title_n, r_title_n)
        mix_bonus    = 0.15 if (mix_n and mix_n not in {"original mix"} and mix_n in normalize(res_title)) else 0.0
        # Slightly increase bias for masters to prefer canonical context
        type_bonus   = 0.35 if res_type == "master" else 0.0
        year_bonus   = 0.1 if isinstance(res_year, int) and (1900 <= res_year <= 2100) else 0.0
        score = 0.45*artist_score + 0.45*title_score + mix_bonus + type_bonus + year_bonus
        if score > best_score:
            best_score, best = score, res
    if best and best_score >= 0.35:
        best["_match_score"] = round(best_score, 3)
        return best
    return None

def discogs_search(artist: str, title: str, mix: Optional[str], delay: float) -> Optional[Dict[str, Any]]:
    sess = requests.Session()
    queries: List[Dict[str, str]] = []
    if artist and title:
        q1 = {"artist": artist, "track": title}
        if mix and mix.lower() != "original mix": q1["q"] = mix
        queries.append(q1)
        queries.append({"artist": artist, "title": title})
    queries.append({"q": f"{artist} {title}".strip()})

    for base in queries:
        params = {
            **base,
            "type": "release",
            "sort": "relevance",
            "per_page": "10",
        }
        try:
            data = fetch_json(DISCOGS_SEARCH, params=params, delay=delay, session=sess)
        except RetryableDiscogsError:
            raise
        except requests.RequestException as e:
            raise RetryableDiscogsError(f"Request failed: {e}")

        results = data.get("results", [])
        if VERBOSE:
            print(dim(f"      [net] search results: {len(results)}"))
        if not results:
            time.sleep(delay); continue

        ranked = rank_results(results, artist, title, mix)
        if ranked:
            if VERBOSE:
                print(dim(f"      [rank] chose title={ranked.get('title')} score={ranked.get('_match_score')}"))
            return ranked
    return None

# ---------------------- Master + Versions helpers ----------------------
def fetch_release_details(resource_url_or_id: str) -> Optional[Dict[str, Any]]:
    """Accepts full resource_url or numeric release id."""
    url = resource_url_or_id
    if isinstance(resource_url_or_id, int) or str(resource_url_or_id).isdigit():
        url = f"https://api.discogs.com/releases/{resource_url_or_id}"
    try:
        return fetch_json(url, delay=0.0)
    except RetryableDiscogsError:
        raise
    except requests.RequestException as e:
        raise RetryableDiscogsError(f"Details request failed: {e}")

def fetch_master_year(master_id: Optional[int]) -> str:
    if not master_id:
        return ""
    url = f"https://api.discogs.com/masters/{master_id}"
    try:
        details = fetch_json(url, delay=0.0)
        return coerce_year(details.get("year", ""))
    except RetryableDiscogsError:
        raise
    except Exception:
        return ""

def fetch_master_versions(master_id: int, max_pages: int = 5) -> List[Dict[str, Any]]:
    """Return a list of version objects (paginated)."""
    versions: List[Dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        url = f"https://api.discogs.com/masters/{master_id}/versions"
        data = fetch_json(url, params={"page": page, "per_page": 100}, delay=0.0)
        versions.extend(data.get("versions", []))
        pg = data.get("pagination", {})
        pages = int(pg.get("pages", 1) or 1)
        if page >= pages:
            break
        page += 1
    return versions

def pick_earliest_version(versions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Choose the earliest version by year (ties, prefer Vinyl, then any)."""
    best = None
    best_year = 9999
    for v in versions:
        y = coerce_year(v.get("year", ""))
        if not y:
            continue
        yi = int(y)
        # Prefer smaller year, tie breaker vinyl formats
        if yi < best_year:
            best = v; best_year = yi
        elif yi == best_year and best is not None:
            fmt = (v.get("format", "") or "").lower()
            best_fmt = (best.get("format", "") or "").lower()
            if "vinyl" in fmt and "vinyl" not in best_fmt:
                best = v
    return best

def get_original_release_info(master_id: int, min_img_size: int) -> Tuple[str, str, int]:
    """
    From a master, find the earliest version and return:
    (labels_string, best_image_url, release_id)
    """
    try:
        versions = fetch_master_versions(master_id)
    except RetryableDiscogsError:
        raise
    except Exception:
        versions = []

    if not versions:
        return ("", "", 0)

    earliest = pick_earliest_version(versions)
    if not earliest:
        return ("", "", 0)

    release_id = int(earliest.get("id", 0) or 0)
    labels_str = earliest.get("label", "") or ""  # versions often include label text

    img_url = ""
    if release_id:
        # fetch full release to get robust labels and images
        try:
            rel = fetch_release_details(release_id)
            if rel:
                # labels (authoritative)
                labs = rel.get("labels") or rel.get("label") or []
                if isinstance(labs, list):
                    labels_str = ", ".join(sorted({(lab.get("name") or "").strip() for lab in labs if lab.get("name")}))
                # images
                img_url = choose_best_image(rel.get("images", []), min_size=min_img_size) or ""
        except RetryableDiscogsError:
            raise
        except Exception:
            pass

    return (labels_str, img_url, release_id)

# ---------------------- Images ----------------------
def choose_best_image(images: List[Dict[str, Any]], min_size: int = MIN_ART_SIZE) -> Optional[str]:
    if not images: return None
    scored = []
    for img in images:
        w, h = int(img.get("width",0)), int(img.get("height",0))
        uri   = img.get("uri") or img.get("resource_url")
        if not uri: continue
        scored.append(((w*h) + (1_000_000 if img.get("type")=="primary" else 0), w, h, uri))
    if not scored: return None
    scored.sort(reverse=True)
    for _, w, h, uri in scored:
        if max(w,h) >= min_size: return uri
    return scored[0][3]

def download_image(url: str) -> Optional[bytes]:
    headers = base_headers()
    try:
        r = requests.get(url, headers=headers, params=auth_params({}), timeout=30)
        r.raise_for_status()
        return r.content
    except requests.RequestException:
        return None

# ---------------------- art read/write (MP3/FLAC/MP4-M4A) ----------------------
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
                c0 = covr[0]
                return bytes(c0)
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
        print(warn(f"   [WARN] Failed to remove existing art: {e}"))

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
            audio.save()
            return True
    except MutagenError as e:
        print(warn(f"   [WARN] Failed to write art: {e}"))
        return False
    return False

# ---------------------- tag writing (ALL formats) ----------------------
def write_year_label_tags(path: Path, year: Optional[str], label: Optional[str]) -> Tuple[bool, str]:
    y   = coerce_year(year)
    lbl = (label.strip() if label else "")
    changed = False
    ext = path.suffix.lower()

    if ext in (".mp3", ".wav", ".aif", ".aiff"):
        try:
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = ID3()
            if y:
                tags.delall("TDRC"); tags.add(TDRC(encoding=3, text=y))
                tags.delall("TYER"); tags.add(TYER(encoding=3, text=y))
                changed = True
            if lbl:
                tags.delall("TPUB"); tags.add(TPUB(encoding=3, text=lbl))
                # also store in TXXX:LABEL for compatibility
                tags.delall("TXXX")
                tags.add(TXXX(encoding=3, desc="LABEL", text=lbl))
                changed = True
            if changed: tags.save(path)
            return changed, "ok" if changed else "unchanged"
        except MutagenError as e:
            return False, f"write_failed: {e}"

    if ext in (".flac", ".ogg", ".oga", ".opus"):
        try:
            audio = MutagenFile(path)
            if audio is None: return False, "unsupported_format"
            if y:
                audio["DATE"] = [y]
                audio["YEAR"] = [y]
                changed = True
            if lbl:
                audio["LABEL"] = [lbl]
                audio["PUBLISHER"] = [lbl]
                changed = True
            if changed: audio.save()
            return changed, "ok" if changed else "unchanged"
        except MutagenError as e:
            return False, f"write_failed: {e}"

    if ext in (".m4a", ".mp4", ".alac", ".aac"):
        try:
            audio = MP4(path)
            if y:
                audio["\xa9day"] = [y]
                changed = True
            if lbl:
                audio["----:com.apple.iTunes:LABEL"] = [MP4FreeForm(lbl.encode("utf-8"))]
                changed = True
            if changed: audio.save()
            return changed, "ok" if changed else "unchanged"
        except MutagenError as e:
            return False, f"write_failed: {e}"

    if ext == ".wma":
        try:
            audio = ASF(path)
            if y:
                audio.tags["WM/Year"] = [y]
                changed = True
            if lbl:
                audio.tags["WM/Publisher"] = [lbl]
                changed = True
            if changed: audio.save()
            return changed, "ok" if changed else "unchanged"
        except MutagenError as e:
            return False, f"write_failed: {e}"

    return False, "unsupported_format"

# ---------------------- discovery ----------------------
def find_audio_files(root: Path, recursive: bool) -> List[Path]:
    # Dedup and case insensitive suffix filtering (avoids double counting)
    exts = {e.lower() for e in ALL_AUDIO_EXTS}
    candidates = root.rglob("*") if recursive else root.glob("*")
    seen = set()
    out: List[Path] = []
    for p in candidates:
        if not p.is_file():
            continue
        if p.suffix.lower() in exts:
            key = str(p.resolve()).lower()
            if key not in seen:
                seen.add(key)
                out.append(p)
    return sorted(out)

# ---------------------- playlist parsing ----------------------
def parse_m3u_playlist(pl_path: Path) -> List[Path]:
    """
    Parse an .m3u or .m3u8 playlist into a list of audio file Paths.
    Handles absolute and relative paths, ignores comments and URLs,
    and filters to existing audio files only.
    """
    try:
        text = pl_path.read_text(encoding="utf-8", errors="ignore")
    except UnicodeDecodeError:
        text = pl_path.read_text(errors="ignore")

    lines = text.splitlines()
    base = pl_path.parent
    exts = {e.lower() for e in ALL_AUDIO_EXTS}
    seen = set()
    out: List[Path] = []

    url_re = re.compile(r"^(https?|ftp)://", re.IGNORECASE)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            # comment or EXTINF metadata
            continue
        if url_re.match(line):
            # streaming URL, skip
            continue

        # Strip quotes if any
        line_clean = line.strip('"').strip("'")
        p = Path(line_clean)
        if not p.is_absolute():
            p = (base / p).resolve()

        if not p.exists() or not p.is_file():
            continue
        if p.suffix.lower() not in exts:
            continue

        key = str(p).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)

    return out

# ---------------------- per file processing ----------------------
def process_one_file(f: Path, args, placeholder_md5: Optional[str]) -> Dict[str, Any]:
    artist, title, mix = get_artist_title_from_tags(f)
    row: Dict[str, Any] = {
        "file": str(f), "artist": artist, "title": title, "mix": mix or "",
        "year": "", "label": "", "discogs_url": "", "match_confidence": "",
        "tag_status": "unchanged", "art_status": "unchanged", "art_source_url": "", "notes": ""
    }
    print(f"    {info('Lookup:')} {artist} - {title}" + (f" ({mix})" if mix else ""))

    # Search (may raise RetryableDiscogsError)
    res = discogs_search(artist, title, mix, args.delay)
    if not res:
        print(f"      {err('[MISS]')} No confident Discogs match")
        row["notes"] = "no_confident_match"
        return row

    row["match_confidence"] = res.get("_match_score", "")
    url = res.get("uri") or ""
    row["discogs_url"] = f"https://www.discogs.com{url}" if url and url.startswith(("/", "release", "master")) else url

    # Fetch release details for labels/images context
    details = None
    try:
        details = fetch_release_details(res.get("resource_url", ""))
    except RetryableDiscogsError:
        raise

    # Determine master id from result/details
    master_id = None
    if res.get("type") == "master":
        master_id = res.get("id") or (details.get("id") if details else None)
    else:
        master_id = res.get("master_id") or (details.get("master_id") if details else None)

    # Canonical (master) year
    year = ""
    if master_id:
        try:
            year = fetch_master_year(int(master_id))
        except RetryableDiscogsError:
            raise
    if not year:
        # Fallback to release year only if master not available
        year = coerce_year(details.get("year", res.get("year", "")) if details else res.get("year", ""))

    # ORIGINAL release label plus art from earliest version
    orig_label, orig_img_url, orig_release_id = "", "", 0
    if master_id:
        try:
            orig_label, orig_img_url, orig_release_id = get_original_release_info(int(master_id), args.min_art)
        except RetryableDiscogsError:
            raise

    # If for some reason no original label, fall back to matched release labels
    labels = orig_label
    if not labels and details:
        labs = details.get("labels") or details.get("label") or []
        if isinstance(labs, list):
            labels = ", ".join(sorted({(lab.get("name") or "").strip() for lab in labs if lab.get("name")}))

    # Also keep matched release images as emergency fallback
    img_url_release = choose_best_image(details.get("images", []), min_size=args.min_art) if details else ""

    # Final decisions
    row["year"] = year
    row["label"] = labels
    print(f"      {ok('✓')} Match score={row['match_confidence']}  year={year or '-'}  label={labels or '-'}")

    # Write tags
    try:
        changed, note = write_year_label_tags(f, year, labels)
        row["tag_status"] = "updated" if changed else note
    except Exception as e:
        row["tag_status"] = f"failed: {e}"

    # Artwork update
    art_status, art_src = "unchanged", ""
    art_supported_exts = (".mp3", ".flac", ".m4a", ".mp4", ".alac")
    if not args.no_art and f.suffix.lower() in art_supported_exts and PIL_AVAILABLE:
        try:
            embedded = read_embedded_art(f)
        except Exception:
            embedded = None

        need_art, reason = False, ""
        if embedded is None:
            need_art, reason = True, "missing"
        else:
            try: w,h = image_size_from_bytes(embedded)
            except Exception: w,h = (0,0)
            if max(w,h) < int(args.min_art):
                need_art, reason = True, f"too small ({w}x{h})"
            else:
                if placeholder_md5 and md5_bytes(embedded) == placeholder_md5:
                    need_art, reason = True, "placeholder"

        # Prefer original art, fallback to matched release art
        if need_art:
            candidate_img = orig_img_url or img_url_release or ""
            if candidate_img:
                img_bytes = download_image(candidate_img)
                if img_bytes:
                    remove_all_art(f)
                    mime = "image/png" if candidate_img.lower().endswith(".png") else "image/jpeg"
                    if write_single_cover(f, img_bytes, mime=mime):
                        try:
                            w2,h2 = image_size_from_bytes(img_bytes)
                            if candidate_img == orig_img_url:
                                art_status = f"downloaded ({w2}x{h2}) from original release due to {reason}"
                            else:
                                art_status = f"downloaded ({w2}x{h2}) due to {reason}"
                        except Exception:
                            if candidate_img == orig_img_url:
                                art_status = f"downloaded from original release due to {reason}"
                            else:
                                art_status = f"downloaded due to {reason}"
                        art_src = candidate_img
                    else:
                        art_status, art_src = "write_failed", candidate_img
                else:
                    art_status, art_src = "download_failed", candidate_img
            else:
                art_status = "no_image_available"
        else:
            art_status = "kept_existing"
    elif not PIL_AVAILABLE and f.suffix.lower() in art_supported_exts and not args.no_art:
        art_status = "skipped_no_pillow"

    row["art_status"], row["art_source_url"] = art_status, art_src

    tag_out = ok(row['tag_status']) if row['tag_status'].startswith("updated") else (warn(row['tag_status']) if "unchanged" not in row['tag_status'] else dim(row['tag_status']))
    art_out = ok(row['art_status']) if "downloaded" in row['art_status'] else (warn(row['art_status']) if "no_image" in row['art_status'] or "failed" in row['art_status'] else dim(row['art_status']))
    print(f"      {info('[TAGS]')} {tag_out}   {info('[ART]')} {art_out}")
    return row

# ---------------------- main program with retries ----------------------
def main():
    ap = argparse.ArgumentParser(description="Discogs master year tagging, original label pickup, and art fixer (MP3, FLAC, MP4, M4A).")
    ap.add_argument("folder", help="Folder to scan, or .m3u/.m3u8 playlist file")
    ap.add_argument("-o","--out", default="discogs_results.csv", help="Output CSV path")
    ap.add_argument("-r","--recursive", action="store_true", help="Scan subfolders (directory mode only)")
    ap.add_argument("--delay", type=float, default=0.6, help="Base delay between Discogs calls (seconds)")
    ap.add_argument("--min-art", type=int, default=MIN_ART_SIZE, help="Minimum art size (px)")
    ap.add_argument("--no-art", action="store_true", help="Do not modify or insert album art")
    ap.add_argument("--token", help="Discogs personal access token (overrides env)")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging for Discogs requests")
    ap.add_argument("--no-color", action="store_true", help="Disable colored output")
    args = ap.parse_args()

    global DISCOGS_TOKEN, VERBOSE, USE_COLOR
    if args.token:
        DISCOGS_TOKEN = args.token
    VERBOSE = args.verbose
    if args.no_color:
        USE_COLOR = False  # disables coloring helpers

    if not (DISCOGS_KEY and DISCOGS_SECRET) and not DISCOGS_TOKEN:
        print(err("ERROR: Provide Discogs credentials. Either:"))
        print("  - set DISCOGS_KEY and DISCOGS_SECRET env vars (recommended), or")
        print("  - keep the hardcoded key and secret, or")
        print("  - pass a personal token via --token / set DISCOGS_TOKEN")
        sys.exit(1)

    # placeholder.jpg MD5 (optional)
    placeholder_md5 = None
    p_path = Path(__file__).with_name(PLACEHOLDER_FILENAME)
    if p_path.exists():
        try:
            placeholder_md5 = md5_bytes(p_path.read_bytes())
            print(dim(f"Loaded placeholder '{PLACEHOLDER_FILENAME}' MD5: {placeholder_md5}"))
        except Exception as e:
            print(warn(f"[WARN] Could not read {PLACEHOLDER_FILENAME}: {e}"))
    else:
        print(dim("[INFO] placeholder.jpg not found, placeholder matching disabled."))

    target = Path(args.folder)
    if not target.exists():
        print(err(f"ERROR: Path '{target}' not found."))
        sys.exit(1)

    # Decide between directory scan and playlist scan
    files: List[Path] = []
    if target.is_dir():
        files = find_audio_files(target, args.recursive)
    elif target.is_file() and target.suffix.lower() in (".m3u", ".m3u8"):
        print(info(f"Reading playlist file: {target}"))
        files = parse_m3u_playlist(target)
    else:
        print(err(f"ERROR: '{target}' is not a directory and not an .m3u/.m3u8 playlist."))
        sys.exit(1)

    if not files:
        print(warn("No audio files found."))
        sys.exit(0)

    print(info(f"Scanning {len(files)} file(s)...\n"))

    rows: List[Dict[str, Any]] = []
    retry_queue: List[Path] = []

    # Main pass
    for idx, f in enumerate(files, 1):
        print(f"{info('['+str(idx)+'/'+str(len(files))+']')} {f.name}")
        try:
            row = process_one_file(f, args, placeholder_md5)
            rows.append(row)
        except RetryableDiscogsError as e:
            print(warn(f"   [RETRY] {e}; will retry later"))
            retry_queue.append(f)
        except Exception as e:
            print(err(f"   [ERROR] Unexpected: {e}"))
            rows.append({
                "file": str(f), "artist": "", "title": "", "mix": "",
                "year": "", "label": "", "discogs_url": "", "match_confidence": "",
                "tag_status": "unchanged", "art_status": "unchanged", "art_source_url": "",
                "notes": f"error: {e}"
            })

    # Retry waves
    for round_idx in range(1, RETRY_MAX_ROUNDS+1):
        if not retry_queue: break
        backoff = args.delay * (2**round_idx) + 1.0
        print(info(f"\n== Retry round {round_idx} ({len(retry_queue)} items), sleeping {backoff:.1f}s before retry =="))
        time.sleep(backoff)
        current = retry_queue; retry_queue = []
        for f in current:
            print(info(f"[retry {round_idx}] ") + f.name)
            try:
                row = process_one_file(f, args, placeholder_md5)
                rows.append(row)
            except RetryableDiscogsError as e:
                print(warn(f"   [RETRY-KEEP] {e}"))
                retry_queue.append(f)
            except Exception as e:
                print(err(f"   [ERROR] Unexpected during retry: {e}"))
                rows.append({
                    "file": str(f), "artist": "", "title": "", "mix": "",
                    "year": "", "label": "", "discogs_url": "", "match_confidence": "",
                    "tag_status": "unchanged", "art_status": "unchanged", "art_source_url": "",
                    "notes": f"retry_error: {e}"
                })

    if retry_queue:
        print(warn(f"\n[WARN] {len(retry_queue)} file(s) still pending due to rate limits or errors; you can re run to process them."))

    out_path = Path(args.out)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "file","artist","title","mix","year","label",
                "discogs_url","match_confidence","tag_status",
                "art_status","art_source_url","notes"
            ]
        )
        writer.writeheader(); writer.writerows(rows)

    print(ok(f"\nDone. Wrote {len(rows)} rows to: {out_path.resolve()}"))
    print(dim("CSV columns: file, artist, title, mix, year, label, discogs_url, match_confidence, tag_status, art_status, art_source_url, notes"))

# Exception type (kept near bottom)
class RetryableDiscogsError(Exception): ...
# ---------------------------------------------------

if __name__ == "__main__":
    main()
