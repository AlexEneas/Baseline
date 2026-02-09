#!/usr/bin/env python3
import argparse, hashlib, io, os, re, sys, time, json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import threading
import queue

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

# ===================== Examples / ? help =====================
def print_examples_and_exit() -> None:
    print("""
UpdateArt command examples
==========================

Update artwork in a folder:
  python UpdateArt.py update "D:\\Music"

Update artwork in a folder, including subfolders:
  python UpdateArt.py update "D:\\Music" --recursive

Update artwork using an existing playlist:
  python UpdateArt.py update --playlist "D:\\Playlists\\needs_artwork.m3u8"

Scan a folder and create a playlist of tracks missing artwork:
  python UpdateArt.py scan "D:\\Music"

Scan recursively and write relative paths:
  python UpdateArt.py scan "D:\\Music" --recursive --relative

Resume is automatic. To force a fresh run:
  python UpdateArt.py update "D:\\Music" --reset-progress

Disable the artwork preview window:
  python UpdateArt.py update "D:\\Music" --no-preview

Need full option details:
  python UpdateArt.py --help
""")
    sys.exit(0)

# ===================== Imaging =====================
try:
    from PIL import Image
    PIL_AVAILABLE = True
except Exception:
    Image = None
    PIL_AVAILABLE = False
    print("[INFO] Pillow not installed, album-art size checks and preview will be limited.")
    print("       Install with: python -m pip install pillow")

def image_size_from_bytes(img_bytes: bytes) -> Tuple[int, int]:
    if not PIL_AVAILABLE:
        return (0, 0)
    im = Image.open(io.BytesIO(img_bytes))
    return im.width, im.height

def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="ignore")).hexdigest()

# ===================== Config =====================
DISCOGS_KEY    = os.getenv("DISCOGS_KEY", "")
DISCOGS_SECRET = os.getenv("DISCOGS_SECRET", "")
DISCOGS_TOKEN  = os.getenv("DISCOGS_TOKEN", "")  # optional

USER_AGENT     = os.getenv("DISCOGS_USER_AGENT", "Baseline (Music library maintenance for DJs)")
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
    if not VERBOSE:
        return
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

class RetryableDiscogsError(Exception):
    pass

def fetch_json(url: str, params: Optional[Dict[str, Any]] = None, delay: float = 0.0, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    sess = session or requests.Session()
    r = sess.get(url, headers=base_headers(), params=auth_params(params or {}), timeout=25)
    if r.status_code == 429:
        if VERBOSE:
            print(warn("      [limit] 429 Too Many Requests, backoff 2s"))
        time.sleep(2.0)
        raise RetryableDiscogsError("Rate limited (429)")
    if r.status_code in (401, 403):
        if VERBOSE:
            print(warn("      [net] auth fallback, query params only"))
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
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

def coerce_year(value) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value) if 1900 <= value <= 2100 else ""
    s = str(value).replace("\\", "/")
    parts = s.split("/")
    if parts and len(parts[0]) == 4 and parts[0].isdigit():
        y0 = int(parts[0])
        return str(y0) if 1900 <= y0 <= 2100 else ""
    m = re.search(r"(?<!\d)(19\d{2}|20\d{2}|2100)(?!\d)", s)
    if not m:
        return ""
    y = int(m.group(0))
    return str(y) if 1900 <= y <= 2100 else ""

# ===================== Tag reading (Artist/Title/Year) =====================
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
        if audio is None:
            raise MutagenError("Unrecognized format")
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
                if tags.get("TDRC") and tags["TDRC"].text:
                    year = coerce_year(tags["TDRC"].text[0])
                elif tags.get("TYER") and tags["TYER"].text:
                    year = coerce_year(tags["TYER"].text[0])

        elif ext == ".flac":
            if "artist" in audio and audio["artist"]:
                artist = artist or audio["artist"][0].strip()
            if "title" in audio and audio["title"]:
                title = title or audio["title"][0].strip()
            if "date" in audio and audio["date"]:
                year = coerce_year(audio["date"][0])
            elif "year" in audio and audio["year"]:
                year = coerce_year(audio["year"][0])

        elif ext in (".m4a", ".mp4", ".alac"):
            if audio.tags:
                if "\xa9ART" in audio.tags and audio.tags["\xa9ART"]:
                    artist = artist or str(audio.tags["\xa9ART"][0]).strip()
                if "\xa9nam" in audio.tags and audio.tags["\xa9nam"]:
                    title = title or str(audio.tags["\xa9nam"][0]).strip()
                if "\xa9day" in audio.tags and audio.tags["\xa9day"]:
                    year = coerce_year(audio.tags["\xa9day"][0])

        if not artist or not title:
            fa, ft = parse_filename(path.name)
            artist = artist or fa
            title  = title  or ft

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
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                return None
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
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                return
            tags.delall("APIC")
            tags.save(path)
        elif ext == ".flac":
            audio = FLAC(path)
            audio.clear_pictures()
            audio.save()
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
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = ID3()
            tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=img_bytes))
            tags.save(path)
            return True
        elif ext == ".flac":
            audio = FLAC(path)
            w, h = image_size_from_bytes(img_bytes)
            pic = Picture()
            pic.type = 3
            pic.mime = mime
            pic.desc = "Cover"
            pic.width = w
            pic.height = h
            pic.depth = 24
            pic.data = img_bytes
            audio.add_picture(pic)
            audio.save()
            return True
        elif ext in (".m4a", ".mp4", ".alac"):
            audio = MP4(path)
            fmt = MP4Cover.FORMAT_JPEG
            if mime.lower() == "image/png":
                fmt = MP4Cover.FORMAT_PNG
            audio["covr"] = [MP4Cover(img_bytes, imageformat=fmt)]
            audio.save()
            return True
    except MutagenError as e:
        print(warn(f"   [WARN] Failed writing art: {e}"))
        return False
    return False

# ===================== Persistent Preview Window =====================
class CoverPreview:
    def __init__(self, enabled: bool = True, title: str = "Cover Preview"):
        self.enabled = enabled
        self.title = title
        self._q: "queue.Queue[Tuple[str, Optional[bytes]]]" = queue.Queue()
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if not self.enabled:
            return
        if not PIL_AVAILABLE:
            print(warn("      [WARN] Preview disabled, Pillow not available"))
            self.enabled = False
            return

        try:
            import tkinter as tk  # noqa
            from PIL import ImageTk  # noqa
        except Exception as e:
            print(warn(f"      [WARN] Preview disabled, Tkinter not available: {e}"))
            self.enabled = False
            return

        def ui_loop():
            import tkinter as tk
            from PIL import ImageTk

            root = tk.Tk()
            root.title(self.title)
            root.geometry("720x760")
            root.resizable(True, True)

            header = tk.Label(root, text="No artwork yet", anchor="w", justify="left")
            header.pack(fill="x", padx=10, pady=(10, 5))

            img_label = tk.Label(root)
            img_label.pack(expand=True, fill="both", padx=10, pady=10)

            footer = tk.Label(root, text="Answer y or n in the terminal.", anchor="w", justify="left")
            footer.pack(fill="x", padx=10, pady=(0, 10))

            state: Dict[str, Any] = {"photo": None}

            def set_image(caption: str, data: Optional[bytes]):
                header.config(text=caption)
                if not data:
                    img_label.config(image="")
                    state["photo"] = None
                    return
                try:
                    im = Image.open(io.BytesIO(data))
                    w = img_label.winfo_width() or 700
                    h = img_label.winfo_height() or 650
                    im.thumbnail((w, h))
                    photo = ImageTk.PhotoImage(im)
                    img_label.config(image=photo)
                    state["photo"] = photo
                except Exception:
                    img_label.config(image="")
                    state["photo"] = None

            def poll_queue():
                if self._stop.is_set():
                    try:
                        root.destroy()
                    except Exception:
                        pass
                    return

                try:
                    while True:
                        caption, data = self._q.get_nowait()
                        set_image(caption, data)
                except queue.Empty:
                    pass

                root.after(100, poll_queue)

            self._ready.set()
            poll_queue()
            root.mainloop()

        self._thread = threading.Thread(target=ui_loop, daemon=True)
        self._thread.start()
        self._ready.wait(timeout=2.0)

    def show(self, caption: str, image_bytes: Optional[bytes]):
        if not self.enabled:
            return
        self._q.put((caption, image_bytes))

    def close(self):
        if not self.enabled:
            return
        self._stop.set()

def prompt_yes_no(question: str, default: str = "n") -> bool:
    default = (default or "n").lower().strip()
    while True:
        ans = input(f"{question} (y/n) ").strip().lower()
        if not ans:
            ans = default
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print("Please type y or n.")

# ===================== Discogs Search for Artwork =====================
def choose_best_image(images: List[Dict[str, Any]], min_size: int) -> Optional[str]:
    if not images:
        return None
    ranked = []
    for img in images:
        w, h = int(img.get("width", 0)), int(img.get("height", 0))
        uri = img.get("uri") or img.get("resource_url")
        if not uri:
            continue
        score = (w * h) + (1_000_000 if img.get("type") == "primary" else 0)
        ranked.append((score, w, h, uri))
    if not ranked:
        return None
    ranked.sort(reverse=True)
    for _, w, h, uri in ranked:
        if max(w, h) >= min_size:
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
    artist_n, title_n = normalize(artist), normalize(title)
    best, best_score = None, -1.0
    for res in results:
        res_title = res.get("title", "")
        if " - " in res_title:
            r_artist, r_title = res_title.split(" - ", 1)
        else:
            r_artist, r_title = "", res_title

        a_score = 1.0 if artist_n and (artist_n in normalize(r_artist) or normalize(r_artist) in artist_n) else title_similarity(artist_n, r_artist)
        t_score = title_similarity(title_n, normalize(r_title))

        y_bonus = 0.0
        ry = coerce_year(res.get("year", ""))
        if want_year and ry and want_year == ry:
            y_bonus = 0.35

        type_bonus = 0.15 if res.get("type") == "master" else 0.0
        score = 0.5 * a_score + 0.5 * t_score + y_bonus + type_bonus

        if score > best_score:
            best, best_score = res, score

    if best and best_score >= 0.35:
        best["_match_score"] = round(best_score, 3)
        return best
    return None

def discogs_find_art(artist: str, title: str, want_year: str, delay: float, min_art: int) -> Tuple[Optional[str], str]:
    sess = requests.Session()
    queries = []
    if artist and title:
        queries.append({"artist": artist, "track": title})
        queries.append({"artist": artist, "title": title})
    queries.append({"q": f"{artist} {title}".strip()})

    for base in queries:
        try:
            data = fetch_json(
                DISCOGS_SEARCH,
                params={**base, "type": "release", "per_page": "10", "sort": "relevance"},
                delay=delay,
                session=sess
            )
        except RetryableDiscogsError:
            raise
        except requests.RequestException as e:
            raise RetryableDiscogsError(f"Search failed: {e}")

        results = data.get("results", [])
        if VERBOSE:
            print(dim(f"      [net] search results: {len(results)}"))
        if not results:
            time.sleep(delay)
            continue

        ranked = rank_results_for_art(results, artist, title, want_year)
        if not ranked:
            time.sleep(delay)
            continue

        res_url = ranked.get("resource_url")
        if res_url:
            try:
                rel = fetch_json(res_url, delay=0.0, session=sess)
                img_url = choose_best_image(rel.get("images", []), min_size=min_art)
                if img_url:
                    return img_url, "release"

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

# ===================== Target list building =====================
def find_target_files(root: Path, recursive: bool) -> List[Path]:
    exts = {e.lower() for e in ART_EXTS}
    candidates = root.rglob("*") if recursive else root.glob("*")
    seen = set()
    out: List[Path] = []
    for p in candidates:
        if p.is_file() and p.suffix.lower() in exts:
            key = str(p.resolve()).lower()
            if key not in seen:
                seen.add(key)
                out.append(p)
    return sorted(out)

def parse_m3u8(playlist_path: Path) -> List[Path]:
    exts = {e.lower() for e in ART_EXTS}
    base_dir = playlist_path.parent

    try:
        raw = playlist_path.read_text(encoding="utf-8-sig", errors="ignore")
    except Exception:
        raw = playlist_path.read_text(encoding="utf-8", errors="ignore")

    paths: List[Path] = []
    seen = set()

    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        p = Path(line)
        if not p.is_absolute():
            p = (base_dir / p).resolve()

        if not p.exists() or not p.is_file():
            continue
        if p.suffix.lower() not in exts:
            continue

        key = str(p).lower()
        if key not in seen:
            seen.add(key)
            paths.append(p)

    return sorted(paths)

def build_list_fingerprint(files: List[Path]) -> str:
    joined = "\n".join(str(p.resolve()) for p in files)
    return md5_text(joined)

# ===================== Progress store =====================
def default_progress_file(folder: Optional[Path], playlist: Optional[Path]) -> Path:
    if playlist is not None:
        return playlist.with_suffix(playlist.suffix + ".updateart.progress.json")
    if folder is not None:
        return folder / ".updateart.progress.json"
    return Path(".updateart.progress.json")

def load_progress(progress_path: Path) -> Optional[Dict[str, Any]]:
    if not progress_path.exists():
        return None
    try:
        return json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        return None

def save_progress(progress_path: Path, data: Dict[str, Any]) -> None:
    try:
        progress_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        print(warn(f"[WARN] Could not save progress file: {e}"))

def reset_progress(progress_path: Path) -> None:
    try:
        if progress_path.exists():
            progress_path.unlink()
            print(dim(f"Deleted progress file: {progress_path}"))
    except Exception as e:
        print(warn(f"[WARN] Could not delete progress file: {e}"))

# ===================== Decision logic =====================
def needs_artwork(path: Path, min_art: int, placeholder_md5: Optional[str]) -> Tuple[bool, str]:
    if not PIL_AVAILABLE:
        return (False, "pillow_missing")
    emb = read_embedded_art(path)
    if emb is None:
        return (True, "missing")
    if placeholder_md5 and md5_bytes(emb) == placeholder_md5:
        return (True, "placeholder")
    try:
        w, h = image_size_from_bytes(emb)
    except Exception:
        w, h = (0, 0)
    if max(w, h) < min_art:
        return (True, f"too_small ({w}x{h})")
    return (False, "ok")

def should_auto_update(reason: str) -> bool:
    return reason in ("missing", "placeholder")

# ===================== File processing =====================
def process_file_interactive(path: Path, args, placeholder_md5: Optional[str], preview: CoverPreview) -> str:
    artist, title, year = read_artist_title_year(path)
    print(f"    {info('Track:')} {artist} - {title}" + (f" [{year}]" if year else ""))

    emb = read_embedded_art(path)

    caption = f"{path.name}"
    if emb:
        caption = f"{path.name} | Embedded art"
        if PIL_AVAILABLE:
            try:
                w, h = image_size_from_bytes(emb)
                if w and h:
                    caption = f"{path.name} | Embedded art {w}x{h}"
            except Exception:
                pass
    else:
        caption = f"{path.name} | No embedded art"

    preview.show(caption, emb)

    need, reason = needs_artwork(path, args.min_art, placeholder_md5)

    if need:
        print(f"      {warn('Status:')} would update automatically because {reason}")
    else:
        print(f"      {dim('Status:')} looks OK (auto would skip)")

    if need and should_auto_update(reason):
        print(f"      {info('Auto:')} updating without prompt ({reason})")
        do_update = True
    else:
        do_update = prompt_yes_no("      Update artwork from Discogs?", default="n")

    if not do_update:
        print(f"      {dim('skip')} kept existing artwork")
        return "kept"

    try:
        img_url, src = discogs_find_art(artist, title, year, args.delay, args.min_art)
    except RetryableDiscogsError as e:
        print(warn(f"      [RETRY] {e}"))
        return "retry"

    if not img_url:
        # Optional fallback: embed a local 'white label' image when Discogs has no suitable match.
        if (not getattr(args, "no_fallback_art", False)) and getattr(args, "fallback_art", None):
            fb_path = Path(__file__).parent / str(args.fallback_art)
            if fb_path.exists():
                try:
                    fb_bytes = fb_path.read_bytes()
                    fb_mime = "image/png" if fb_path.suffix.lower() == ".png" else "image/jpeg"
                    remove_all_art(path)
                    if write_single_cover(path, fb_bytes, mime=fb_mime):
                        try:
                            w, h = image_size_from_bytes(fb_bytes)
                            if w and h:
                                preview.show(f"{path.name} | Fallback art {w}x{h}", fb_bytes)
                            else:
                                preview.show(f"{path.name} | Fallback art", fb_bytes)
                        except Exception:
                            preview.show(f"{path.name} | Fallback art", fb_bytes)
                        print(ok("      ✓ embedded fallback artwork (white label)"))
                        return "fallback"
                except Exception as e:
                    print(warn(f"      [WARN] Fallback artwork failed: {e}"))
        print(err("      [MISS] No suitable artwork found"))
        return "miss"

    img_bytes = download_image(img_url)
    if not img_bytes:
        print(err("      [MISS] Failed to download artwork"))
        return "miss"

    mime = "image/png" if img_url.lower().endswith(".png") else "image/jpeg"

    remove_all_art(path)
    if write_single_cover(path, img_bytes, mime=mime):
        try:
            w, h = image_size_from_bytes(img_bytes)
            print(f"      {ok('✓')} updated to {w}x{h} ({src})")
        except Exception:
            print(f"      {ok('✓')} updated ({src})")

        preview.show(f"{path.name} | Updated art", img_bytes)
        return "updated"

    print(err("      [ERROR] Failed to write cover"))
    return "error"

# ===================== Scan mode: create playlist =====================
def write_m3u8(playlist_out: Path, tracks: List[Path], make_relative_to: Optional[Path] = None) -> None:
    playlist_out.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = ["#EXTM3U"]
    for t in tracks:
        p = t
        if make_relative_to is not None:
            try:
                p = Path(os.path.relpath(t, start=make_relative_to))
            except Exception:
                p = t
        # Use forward slashes in m3u8 for compatibility
        lines.append(str(p).replace("\\", "/"))
    playlist_out.write_text("\n".join(lines) + "\n", encoding="utf-8")

def run_scan(folder: Path, recursive: bool, min_art: int, placeholder_md5: Optional[str], out_path: Path, relative: bool) -> int:
    files = find_target_files(folder, recursive)
    if not files:
        print(warn("No supported audio files found (mp3, flac, m4a/mp4/alac)."))
        return 0

    print(info(f"Scanning {len(files)} file(s) for missing or placeholder artwork..."))

    matches: List[Path] = []
    for i, f in enumerate(files, 1):
        need, reason = needs_artwork(f, min_art=min_art, placeholder_md5=placeholder_md5)
        if need and reason in ("missing", "placeholder"):
            matches.append(f)
            print(f"  {info('['+str(i)+'/'+str(len(files))+']')} {f.name}  {warn('match')} ({reason})")
        else:
            if VERBOSE:
                print(f"  {dim('['+str(i)+'/'+str(len(files))+']')} {f.name}  skip ({reason})")

    if not matches:
        print(ok("No files found with missing or placeholder artwork."))
        return 0

    make_rel_to = out_path.parent if relative else None
    write_m3u8(out_path, matches, make_relative_to=make_rel_to)
    print(ok(f"Wrote playlist with {len(matches)} track(s): {out_path}"))
    return len(matches)

# ===================== Update mode runner =====================
def run_update(args) -> int:
    global DISCOGS_TOKEN, VERBOSE, USE_COLOR
    if args.token:
        DISCOGS_TOKEN = args.token
    VERBOSE = args.verbose
    if args.no_color:
        USE_COLOR = False

    if not (DISCOGS_KEY and DISCOGS_SECRET) and not DISCOGS_TOKEN:
        print(err("ERROR: Provide Discogs credentials. Either:"))
        print("  - keep the hardcoded key/secret, or")
        print("  - set DISCOGS_KEY and DISCOGS_SECRET env vars, or")
        print("  - pass a personal token via --token or set DISCOGS_TOKEN")
        return 2

    playlist_path: Optional[Path] = Path(args.playlist).expanduser().resolve() if args.playlist else None
    folder_path: Optional[Path] = Path(args.folder).expanduser().resolve() if args.folder else None

    if playlist_path:
        if not playlist_path.exists() or not playlist_path.is_file():
            print(err(f"ERROR: Playlist '{playlist_path}' not found or not a file."))
            return 2
    else:
        if folder_path is None:
            print(err("ERROR: Provide a folder, or use --playlist."))
            return 2
        if not folder_path.exists() or not folder_path.is_dir():
            print(err(f"ERROR: Folder '{folder_path}' not found or not a directory."))
            return 2

    placeholder_md5 = None
    if args.placeholder:
        p = (Path(__file__).parent / args.placeholder)
        if p.exists():
            try:
                placeholder_md5 = md5_bytes(p.read_bytes())
                print(dim(f"Loaded placeholder '{args.placeholder}' MD5: {placeholder_md5}"))
            except Exception as e:
                print(warn(f"[WARN] Could not read placeholder file: {e}"))

    mode = "playlist" if playlist_path else "folder"
    if playlist_path:
        files = parse_m3u8(playlist_path)
    else:
        files = find_target_files(folder_path, args.recursive)

    if not files:
        print(warn("No supported audio files found."))
        return 0

    fingerprint = build_list_fingerprint(files)

    progress_path = Path(args.progress_file).expanduser().resolve() if args.progress_file else default_progress_file(folder_path, playlist_path)

    if args.reset_progress:
        reset_progress(progress_path)

    start_index = 0
    if not args.no_resume and not args.reset_progress:
        saved = load_progress(progress_path)
        if saved and saved.get("fingerprint") == fingerprint and saved.get("mode") == mode:
            start_index = int(saved.get("next_index", 0))
            if start_index < 0:
                start_index = 0
            if start_index > len(files):
                start_index = 0
            print(info(f"Resuming: starting at item {start_index + 1} of {len(files)}"))
        elif saved:
            print(warn("Saved progress does not match this target list, starting from the beginning."))
            print(dim(f"Progress file: {progress_path}"))

    preview = CoverPreview(enabled=(not args.no_preview), title="Cover Preview")
    preview.start()

    print(info(f"Target mode: {mode}"))
    if playlist_path:
        print(dim(f"Playlist: {playlist_path}"))
    else:
        print(dim(f"Folder: {folder_path}"))
    print(info(f"Processing {len(files)} file(s)...\n"))

    stats = {"updated": 0, "kept": 0, "miss": 0, "retry": 0, "error": 0}

    try:
        for i in range(start_index, len(files)):
            f = files[i]
            print(f"{info('[' + str(i + 1) + '/' + str(len(files)) + ']')} {f.name}")
            try:
                res = process_file_interactive(f, args, placeholder_md5, preview)
                stats[res] = stats.get(res, 0) + 1
            except RetryableDiscogsError as e:
                print(warn(f"   [RETRY] {e}"))
                stats["retry"] += 1
            except KeyboardInterrupt:
                print(warn("\nInterrupted by user. Saving progress and stopping."))
                save_progress(progress_path, {
                    "mode": mode,
                    "fingerprint": fingerprint,
                    "next_index": i,
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                })
                break
            except Exception as e:
                print(err(f"   [ERROR] {e}"))
                stats["error"] += 1

            save_progress(progress_path, {
                "mode": mode,
                "fingerprint": fingerprint,
                "next_index": i + 1,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
    finally:
        preview.close()

    print("\n" + info("Summary"))
    print(f"  {ok('updated')} : {stats['updated']}")
    print(f"  {dim('kept')}    : {stats['kept']}")
    print(f"  {warn('miss')}    : {stats['miss']}")
    print(f"  {warn('retry')}   : {stats['retry']}")
    print(f"  {err('error')}   : {stats['error']}")

    print(dim(f"\nProgress file: {progress_path}"))
    return 0

# ===================== Main (subcommands) =====================
def main():
    # Friendly help shortcut:
    #   python UpdateArt.py ?
    #   python UpdateArt.py help
    if len(sys.argv) > 1 and sys.argv[1] in ("?", "help"):
        print_examples_and_exit()

    ap = argparse.ArgumentParser(description="UpdateArt: interactive Discogs cover updater, plus scan mode to build a playlist of tracks missing artwork.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # UPDATE
    up = sub.add_parser("update", help="Update artwork from Discogs (folder or playlist), with resume support.")
    up.add_argument("folder", nargs="?", help="Folder to scan (omit if using --playlist)")
    up.add_argument("--playlist", help="Path to an .m3u8 file. If set, only tracks in the playlist are processed.")
    up.add_argument("-r", "--recursive", action="store_true", help="Scan subfolders (folder mode only)")
    up.add_argument("--min-art", type=int, default=MIN_ART_SIZE_DEFAULT, help="Minimum artwork size in px (default 500)")
    up.add_argument("--delay", type=float, default=0.6, help="Base delay between Discogs calls (seconds)")
    up.add_argument("--placeholder", default=PLACEHOLDER_FILENAME, help="Placeholder image filename to detect (default placeholder.jpg)")

    up.add_argument("--fallback-art", default="white_label.jpg",
                help="If Discogs artwork is not found, embed this local image instead (relative to Discogs folder). Default: white_label.jpg")
    up.add_argument("--no-fallback-art", action="store_true",
                help="Disable fallback artwork embedding when Discogs has no match")
    up.add_argument("--token", help="Discogs personal access token (optional, overrides env)")
    up.add_argument("--verbose", action="store_true", help="Verbose HTTP logging")
    up.add_argument("--no-color", action="store_true", help="Disable colored output")
    up.add_argument("--no-preview", action="store_true", help="Disable the preview window")

    up.add_argument("--progress-file", help="Custom progress file path (JSON). Defaults beside the folder or playlist.")
    up.add_argument("--no-resume", action="store_true", help="Ignore saved progress and start from the beginning.")
    up.add_argument("--reset-progress", action="store_true", help="Delete saved progress and start from the beginning.")

    # SCAN
    sc = sub.add_parser("scan", help="Scan a folder and create an .m3u8 playlist of files with missing or placeholder artwork.")
    sc.add_argument("folder", help="Folder to scan")
    sc.add_argument("-r", "--recursive", action="store_true", help="Scan subfolders")
    sc.add_argument("--min-art", type=int, default=MIN_ART_SIZE_DEFAULT, help="Minimum artwork size in px (used for status reporting only)")
    sc.add_argument("--placeholder", default=PLACEHOLDER_FILENAME, help="Placeholder image filename to detect (default placeholder.jpg)")
    sc.add_argument("--out", help="Output .m3u8 path. Default: <folder>\\needs_artwork.m3u8")
    sc.add_argument("--relative", action="store_true", help="Write playlist paths relative to the playlist file location.")
    sc.add_argument("--verbose", action="store_true", help="Verbose logging")

    args = ap.parse_args()

    global VERBOSE
    VERBOSE = getattr(args, "verbose", False)

    if args.cmd == "update":
        raise SystemExit(run_update(args))

    if args.cmd == "scan":
        folder = Path(args.folder).expanduser().resolve()
        if not folder.exists() or not folder.is_dir():
            print(err(f"ERROR: Folder '{folder}' not found or not a directory."))
            raise SystemExit(2)

        placeholder_md5 = None
        if args.placeholder:
            p = (Path(__file__).parent / args.placeholder)
            if p.exists():
                try:
                    placeholder_md5 = md5_bytes(p.read_bytes())
                    print(dim(f"Loaded placeholder '{args.placeholder}' MD5: {placeholder_md5}"))
                except Exception as e:
                    print(warn(f"[WARN] Could not read placeholder file: {e}"))

        out_path = Path(args.out).expanduser().resolve() if args.out else (folder / "needs_artwork.m3u8")
        count = run_scan(folder, args.recursive, args.min_art, placeholder_md5, out_path, args.relative)
        raise SystemExit(0 if count >= 0 else 1)

if __name__ == "__main__":
    main()
