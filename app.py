#!/usr/bin/env python3
"""
Baseline
Music library maintenance for DJs

Tkinter GUI wrapper that runs the tools in this folder.

Highlights:
- Discogs tab uses checkboxes for Year, Record Label, Artwork, then runs a single update pass.
- Filename Check tab suggests filename fixes based on tags, no renaming is performed.
- Bridges scripts that use input() so prompts like y/n work inside the GUI.
"""

import sys
import threading
import queue
import traceback
import builtins
import re
import os
import json
from dataclasses import dataclass
from pathlib import Path
import importlib.util
from datetime import datetime

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from tkinter import font as tkfont

try:
    from PIL import Image, ImageTk
except Exception:
    Image = None
    ImageTk = None


ROOT = Path(__file__).resolve().parent

if getattr(sys, "frozen", False):
    APP_ROOT = Path(sys.executable).resolve().parent
else:
    APP_ROOT = ROOT

SETTINGS_DIR = APP_ROOT / "data"
SETTINGS_PATH = SETTINGS_DIR / "baseline_settings.json"

if os.name == "nt":
    _legacy_settings_base = Path(os.getenv("APPDATA") or (Path.home() / "AppData" / "Roaming"))
else:
    _legacy_settings_base = Path.home() / ".config"

LEGACY_SETTINGS_PATHS = [
    ROOT / "settings" / "baseline_settings.json",
    _legacy_settings_base / "Baseline" / "baseline_settings.json",
]

LOGS_ROOT = Path.home() / "Documents" / "Baseline" / "Logs"

UI_COLORS = {
    "bg": "#F2EEE6",
    "panel": "#FBF8F2",
    "ink": "#1E293B",
    "muted": "#5B6475",
    "accent": "#0F766E",
    "accent_active": "#0A5F59",
    "line": "#D7D1C5",
    "field": "#FFFFFF",
}


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_module(module_path: Path, module_name: str):
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


class Logger:
    def __init__(self, q: "queue.Queue[str]"):
        self.q = q

    def write(self, s: str):
        if s:
            self.q.put(s)

    def flush(self):
        return


@dataclass
class InputRequest:
    prompt: str
    event: threading.Event
    result: str = ""



class Settings:
    def __init__(self):
        _safe_mkdir(SETTINGS_DIR)
        discogs_placeholder = str((ROOT / "Discogs" / "placeholder.jpg").resolve()) if (ROOT / "Discogs" / "placeholder.jpg").exists() else ""
        discogs_fallback = str((ROOT / "Discogs" / "white_label.jpg").resolve()) if (ROOT / "Discogs" / "white_label.jpg").exists() else ""
        self.data = {
            "discogs": {
                "consumer_key": "",
                "consumer_secret": "",
                "user_agent": "Baseline (Music library maintenance for DJs)",
                "min_art_size": 500,
                "auto_accept_resize_small_art": False,
                "placeholder_image": discogs_placeholder,
                "fallback_image": discogs_fallback,
                "format_priority": ["Vinyl", "CD", "Digital"],
            },
            "mik": {
                "db_path": "",
            },
        }
        self.load()

    @staticmethod
    def _load_json_file(path: Path) -> dict:
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}

    def load(self):
        loaded: dict = {}
        if SETTINGS_PATH.exists():
            loaded = self._load_json_file(SETTINGS_PATH)
        else:
            for legacy_path in LEGACY_SETTINGS_PATHS:
                if legacy_path == SETTINGS_PATH:
                    continue
                if not legacy_path.exists():
                    continue
                # Migrate old settings into the new app-root data location.
                loaded = self._load_json_file(legacy_path)
                if loaded:
                    self._merge(loaded)
                    self.save()
                    return

        if loaded:
            self._merge(loaded)
        else:
            self.save()

    def _merge(self, loaded: dict):
        for k, v in loaded.items():
            if isinstance(v, dict) and isinstance(self.data.get(k), dict):
                self.data[k].update(v)
            else:
                self.data[k] = v

    def save(self):
        _safe_mkdir(SETTINGS_DIR)
        SETTINGS_PATH.write_text(json.dumps(self.data, indent=2), encoding="utf-8")


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.settings = Settings()

        self._body_font = self._pick_font(("Bahnschrift", "Segoe UI", "Calibri", "Arial"))
        self._title_font = self._pick_font(("Bahnschrift SemiBold", "Segoe UI Semibold", "Trebuchet MS", "Arial"))
        self._mono_font = self._pick_font(("Cascadia Mono", "Consolas", "Courier New", "Courier"))
        self._configure_theme()

        self.title("Baseline Music Suite")
        self.geometry("1220x860")
        self.minsize(1040, 720)

        self.log_q: "queue.Queue[str]" = queue.Queue()
        self.input_request_q: "queue.Queue[InputRequest]" = queue.Queue()
        self.worker: threading.Thread | None = None

        self.run_id = None
        self.run_dir: Path | None = None
        self.run_log_fp = None
        self.run_summary_fp = None

        self._preview_photo = None
        self._preview_signature: tuple[int, bytes, bytes] | None = None
        self._preview_caption = tk.StringVar(value="No preview yet")
        self._interactive_controls: list[tk.Misc] = []
        self._mik_db_cache: Path | None = None
        self._mik_db_cache_ready = False

        self._build_ui()
        self._pump_logs()
        self._pump_input_requests()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _pick_font(self, candidates: tuple[str, ...]) -> str:
        try:
            families = {str(f) for f in self.tk.call("font", "families")}
        except Exception:
            families = set()
        for name in candidates:
            if name in families:
                return name
        return "TkDefaultFont"

    def _configure_theme(self):
        self.option_add("*tearOff", False)
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        self.configure(bg=UI_COLORS["bg"])
        style.configure(".", font=(self._body_font, 10))
        style.configure("TFrame", background=UI_COLORS["bg"])
        style.configure("Header.TFrame", background=UI_COLORS["bg"])
        style.configure("TLabel", background=UI_COLORS["bg"], foreground=UI_COLORS["ink"], font=(self._body_font, 10))
        style.configure("Title.TLabel", background=UI_COLORS["bg"], foreground=UI_COLORS["ink"], font=(self._title_font, 24, "bold"))
        style.configure("Subtitle.TLabel", background=UI_COLORS["bg"], foreground=UI_COLORS["muted"], font=(self._body_font, 10))
        style.configure("Status.TLabel", background=UI_COLORS["bg"], foreground=UI_COLORS["accent"], font=(self._body_font, 10, "bold"))

        style.configure("TNotebook", background=UI_COLORS["bg"], borderwidth=0)
        style.configure("TNotebook.Tab", padding=(14, 8), font=(self._body_font, 10, "bold"))
        style.map(
            "TNotebook.Tab",
            background=[("selected", UI_COLORS["panel"]), ("!selected", "#E5DED1")],
            foreground=[("selected", UI_COLORS["accent"]), ("!selected", UI_COLORS["ink"])],
        )

        style.configure("TLabelframe", background=UI_COLORS["panel"], borderwidth=1, relief="solid")
        style.configure("TLabelframe.Label", background=UI_COLORS["panel"], foreground=UI_COLORS["ink"], font=(self._body_font, 10, "bold"))
        style.configure("TCheckbutton", background=UI_COLORS["bg"], foreground=UI_COLORS["ink"])
        style.map("TCheckbutton", foreground=[("disabled", UI_COLORS["muted"])])
        style.configure("TEntry", fieldbackground=UI_COLORS["field"])
        style.configure("TButton", padding=(10, 6), font=(self._body_font, 10))
        style.configure("Primary.TButton", padding=(12, 7), font=(self._body_font, 10, "bold"), foreground="white", background=UI_COLORS["accent"], borderwidth=0)
        style.map(
            "Primary.TButton",
            background=[("active", UI_COLORS["accent_active"]), ("!disabled", UI_COLORS["accent"]), ("disabled", "#AEB6BE")],
            foreground=[("disabled", "#F8F9FA"), ("!disabled", "white")],
        )

    def _on_close(self):
        if self.worker and self.worker.is_alive():
            messagebox.showwarning("Baseline is busy", "A task is running. Please wait for it to finish before closing.")
            return
        self.destroy()

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = ttk.Frame(self, padding=(14, 12, 14, 10), style="Header.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        ttk.Label(header, text="Baseline", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(header, text="Music library maintenance for DJs", style="Subtitle.TLabel").grid(row=1, column=0, sticky="w", pady=(3, 0))

        # Busy indicator (spinning bar) so it never looks like Baseline is hanging
        header.columnconfigure(1, weight=0)
        self.busy_text = tk.StringVar(value="Ready")
        self.busy_bar = ttk.Progressbar(header, mode="indeterminate", length=140)
        self.busy_label = ttk.Label(header, textvariable=self.busy_text, style="Status.TLabel")
        self.busy_bar.grid(row=0, column=1, sticky="e", padx=(10, 0))
        self.busy_label.grid(row=1, column=1, sticky="e", padx=(10, 0))
        self.busy_bar.grid_remove()


        self.nb = ttk.Notebook(self)
        self.nb.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))

        self.tab_discogs = ttk.Frame(self.nb, padding=10)
        self.tab_filename = ttk.Frame(self.nb, padding=10)
        self.tab_mik = ttk.Frame(self.nb, padding=10)
        self.tab_rekordbox = ttk.Frame(self.nb, padding=10)
        self.tab_settings = ttk.Frame(self.nb, padding=10)

        self.nb.add(self.tab_discogs, text="Discogs")
        self.nb.add(self.tab_filename, text="Filename Check")
        self.nb.add(self.tab_mik, text="Mixed In Key")
        self.nb.add(self.tab_rekordbox, text="Rekordbox XML")
        self.nb.add(self.tab_settings, text="Settings")

        self._build_discogs_tab()
        self._build_filename_tab()
        self._build_mik_tab()
        self._build_rekordbox_tab()
        self._build_settings_tab()

        log_frame = ttk.LabelFrame(self, text="Run Log", padding=10)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        self.rowconfigure(2, weight=1)

        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.txt_log = tk.Text(
            log_frame,
            height=12,
            wrap="word",
            bg=UI_COLORS["field"],
            fg=UI_COLORS["ink"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=UI_COLORS["line"],
            insertbackground=UI_COLORS["ink"],
            padx=8,
            pady=8,
            font=(self._mono_font, 10),
        )
        self.txt_log.grid(row=0, column=0, sticky="nsew")
        yscroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_log.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.txt_log.configure(yscrollcommand=yscroll.set)

        btns = ttk.Frame(self, padding=(10, 0, 10, 10))
        btns.grid(row=3, column=0, sticky="ew")
        btns.columnconfigure(0, weight=1)

        self.btn_open_last_run = ttk.Button(btns, text="Open Last Run Folder", command=self._open_last_run)
        self.btn_open_last_run.grid(row=0, column=1, sticky="e")
        self.btn_clear_log = ttk.Button(btns, text="Clear Log", command=self._clear_log)
        self.btn_clear_log.grid(row=0, column=2, sticky="e", padx=(8, 0))
        self.btn_quit = ttk.Button(btns, text="Quit", command=self._on_close)
        self.btn_quit.grid(row=0, column=3, sticky="e", padx=(8, 0))

        self._interactive_controls = self._collect_interactive_controls()

    def _collect_interactive_controls(self) -> list[tk.Misc]:
        controls: list[tk.Misc] = []
        skip = {self.txt_log, self.busy_bar, self.btn_quit}

        def walk(parent: tk.Misc):
            for child in parent.winfo_children():
                if child in skip:
                    continue
                if isinstance(child, (ttk.Button, ttk.Entry, ttk.Checkbutton, ttk.Notebook)):
                    controls.append(child)
                walk(child)

        walk(self)
        return controls

    def _set_controls_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for widget in self._interactive_controls:
            if not widget.winfo_exists():
                continue
            try:
                widget.configure(state=state)
            except Exception:
                try:
                    if isinstance(widget, ttk.Notebook):
                        if enabled:
                            widget.state(["!disabled"])
                        else:
                            widget.state(["disabled"])
                except Exception:
                    pass

    def _clear_log(self):
        self.txt_log.delete("1.0", "end")

    def _append_log(self, s: str, flush: bool = True):
        self.txt_log.insert("end", s)
        self.txt_log.see("end")
        if self.run_log_fp:
            try:
                self.run_log_fp.write(s)
                if flush:
                    self.run_log_fp.flush()
            except Exception:
                pass

    def _pump_logs(self):
        chunks: list[str] = []
        try:
            for _ in range(800):
                chunks.append(self.log_q.get_nowait())
        except queue.Empty:
            pass

        if chunks:
            self._append_log("".join(chunks), flush=True)
        self.after(70, self._pump_logs)

    def _pump_input_requests(self):
        try:
            while True:
                req = self.input_request_q.get_nowait()
                req.result = self._handle_input_prompt(req.prompt)
                req.event.set()
        except queue.Empty:
            pass
        self.after(100, self._pump_input_requests)

    def _handle_input_prompt(self, prompt: str) -> str:
        prompt = (prompt or "").strip()
        prompt_lc = prompt.lower()

        yn = False
        if re.search(r"\b\(?\s*y\s*/\s*n\s*\)?\b", prompt_lc):
            yn = True
        if "please type y or n" in prompt_lc:
            yn = True

        if yn:
            msg = prompt if prompt else "Please choose Yes or No"
            ok = messagebox.askyesno("Input required", msg, parent=self)
            return "y" if ok else "n"

        if not prompt:
            prompt = "Enter a value:"
        val = simpledialog.askstring("Input required", prompt, parent=self)
        return val if val is not None else ""

    def _clear_preview(self):
        self._preview_caption.set("No preview yet")
        self.lbl_preview_img.configure(image="")
        self._preview_photo = None
        self._preview_signature = None

    def _render_preview(self, caption: str, image_bytes: bytes | None):
        self._preview_caption.set(caption or "Artwork preview")

        if Image is None or ImageTk is None:
            self._preview_caption.set((caption or "Artwork preview") + "\n\nPillow not installed. Run: pip install pillow")
            self.lbl_preview_img.configure(image="")
            self._preview_photo = None
            return

        if not image_bytes:
            self.lbl_preview_img.configure(image="")
            self._preview_photo = None
            self._preview_signature = None
            return

        try:
            if len(image_bytes) <= 64:
                signature = (len(image_bytes), image_bytes, image_bytes)
            else:
                signature = (len(image_bytes), image_bytes[:32], image_bytes[-32:])
            if signature == self._preview_signature:
                return

            import io
            im = Image.open(io.BytesIO(image_bytes))
            im.thumbnail((520, 560))
            photo = ImageTk.PhotoImage(im)
            self.lbl_preview_img.configure(image=photo)
            self._preview_photo = photo
            self._preview_signature = signature
        except Exception as e:
            self._preview_caption.set((caption or "Artwork preview") + f"\n\nCould not render image: {e}")
            self.lbl_preview_img.configure(image="")
            self._preview_photo = None
            self._preview_signature = None

    
    def _set_busy(self, is_busy: bool, label: str = ""):
        # Called on the UI thread only (use self.after from worker threads)
        if is_busy:
            self.busy_text.set(label or "Working...")
            try:
                self.busy_bar.grid()
                self.busy_bar.start(12)
            except Exception:
                pass
            self._set_controls_enabled(False)
        else:
            self.busy_text.set("Ready")
            try:
                self.busy_bar.stop()
                self.busy_bar.grid_remove()
            except Exception:
                pass
            self._set_controls_enabled(True)

    def _ensure_not_running(self) -> bool:
        if self.worker and self.worker.is_alive():
            messagebox.showwarning("Busy", "A task is already running. Please wait for it to finish.")
            return False
        return True

    def _start_run(self, title: str):
        _safe_mkdir(LOGS_ROOT)
        self.run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.run_dir = LOGS_ROOT / self.run_id
        _safe_mkdir(self.run_dir)
        self.run_log_fp = open(self.run_dir / "baseline.log", "w", encoding="utf-8")
        self.run_summary_fp = open(self.run_dir / "summary.txt", "w", encoding="utf-8")
        self.run_summary_fp.write("Baseline\nMusic library maintenance for DJs\n\n")
        self.run_summary_fp.write(f"Run ID: {self.run_id}\n")
        self.run_summary_fp.write(f"Task: {title}\n")
        self.run_summary_fp.write(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        self.run_summary_fp.flush()

    def _end_run(self, ok: bool = True):
        if self.run_summary_fp:
            self.run_summary_fp.write(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            self.run_summary_fp.write("Run completed successfully.\n" if ok else "Run completed with errors.\n")
            self.run_summary_fp.flush()
            self.run_summary_fp.close()
            self.run_summary_fp = None
        if self.run_log_fp:
            try:
                self.run_log_fp.close()
            except Exception:
                pass
            self.run_log_fp = None

    def _open_last_run(self):
        if not self.run_dir or not self.run_dir.exists():
            messagebox.showinfo("No run yet", "No run folder exists yet in this session.")
            return
        self._open_path(self.run_dir)

    def _open_path(self, path: Path):
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                import subprocess
                subprocess.call(["open", str(path)])
            else:
                import subprocess
                subprocess.call(["xdg-open", str(path)])
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _run_tool(self, script_rel: str, argv: list[str], title: str, patch_cover_preview: bool = False):
        if not self._ensure_not_running():
            return

        script_path = ROOT / script_rel
        if not script_path.exists():
            messagebox.showerror("Missing script", f"Could not find: {script_path}")
            return

        try:
            self._start_run(title)
        except Exception as e:
            messagebox.showerror("Run failed", f"Could not start run logs.\n\n{e}")
            return

        self.after(0, lambda: self._set_busy(True, title))
        self.log_q.put(f"\n== {title} ==\n")
        self.log_q.put(f"Script: {script_path}\nArgs: {argv}\n\n")

        def bridged_input(prompt: str = "") -> str:
            if prompt:
                self.log_q.put(prompt + ("\n" if not prompt.endswith("\n") else ""))
            req = InputRequest(prompt=prompt, event=threading.Event())
            self.input_request_q.put(req)
            req.event.wait()
            return req.result

        def worker():
            old_stdout, old_stderr = sys.stdout, sys.stderr
            old_argv = sys.argv
            old_input = builtins.input

            sys.stdout = Logger(self.log_q)  # type: ignore[assignment]
            sys.stderr = Logger(self.log_q)  # type: ignore[assignment]
            builtins.input = bridged_input

            ok = True
            try:
                mod = load_module(script_path, script_path.stem + "_mod")

                if patch_cover_preview and hasattr(mod, "CoverPreview"):
                    app_ref = self

                    class CoverPreviewBridge:
                        def __init__(self, enabled: bool = True, title: str = "Cover Preview"):
                            self.enabled = enabled
                            self.title = title

                        def start(self):
                            return

                        def show(self, caption, data):
                            if not self.enabled:
                                return
                            cap = str(caption) if caption is not None else "Artwork preview"
                            img = data if isinstance(data, (bytes, bytearray)) else None
                            app_ref.after(0, lambda: app_ref._render_preview(cap, img))

                        def close(self):
                            return

                    mod.CoverPreview = CoverPreviewBridge  # type: ignore[attr-defined]

                if not hasattr(mod, "main"):
                    self.log_q.put("ERROR: Tool does not expose main().\n")
                    ok = False
                else:
                    sys.argv = [str(script_path)] + argv
                    rc = 0
                    try:
                        result = mod.main()
                        rc = int(result) if result is not None else 0
                    except SystemExit as e:
                        code = getattr(e, "code", 0)
                        if code is None:
                            rc = 0
                        elif isinstance(code, int):
                            rc = code
                        else:
                            try:
                                rc = int(code)
                            except Exception:
                                rc = 1
                    except Exception:
                        raise

                    if rc != 0:
                        ok = False
                    self.log_q.put(f"\nDone. Exit code: {rc}\n")

            except Exception:
                ok = False
                self.log_q.put("\nERROR:\n")
                self.log_q.put(traceback.format_exc() + "\n")

            finally:
                try:
                    self.after(0, lambda: self._set_busy(False))
                except Exception:
                    pass

                builtins.input = old_input
                sys.argv = old_argv
                sys.stdout, sys.stderr = old_stdout, old_stderr
                self.after(0, lambda: self._end_run(ok))

        self.worker = threading.Thread(target=worker, daemon=False)
        self.worker.start()

    # Discogs tab
    def _build_discogs_tab(self):
        f = self.tab_discogs
        f.columnconfigure(1, weight=1)
        f.rowconfigure(3, weight=1)

        self.discogs_folder = tk.StringVar()
        self.discogs_recursive = tk.BooleanVar(value=True)

        # checkboxes
        self.discogs_do_year = tk.BooleanVar(value=True)
        self.discogs_do_label = tk.BooleanVar(value=True)
        self.discogs_do_art = tk.BooleanVar(value=True)

        row = 0
        ttk.Label(f, text="Music folder:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.discogs_folder).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_discogs_folder).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Checkbutton(f, text="Recursive", variable=self.discogs_recursive).grid(row=row, column=0, sticky="w", pady=(6,10))

        opts = ttk.LabelFrame(f, text="Allow Baseline to update", padding=10)
        opts.grid(row=row, column=1, columnspan=2, sticky="ew", pady=(0,10))
        ttk.Checkbutton(opts, text="Year", variable=self.discogs_do_year).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(opts, text="Record label", variable=self.discogs_do_label).grid(row=0, column=1, sticky="w", padx=(16,0))
        ttk.Checkbutton(opts, text="Artwork", variable=self.discogs_do_art).grid(row=0, column=2, sticky="w", padx=(16,0))

        actions = ttk.Frame(f)
        actions.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(0,10))
        actions.columnconfigure(0, weight=1)
        ttk.Button(actions, text="Run Discogs update", style="Primary.TButton", command=self._discogs_run_update).grid(row=0, column=0, sticky="w")
        ttk.Button(actions, text="Run artwork scan", command=self._discogs_run_art_scan).grid(row=0, column=1, sticky="w", padx=(12,0))
        ttk.Button(actions, text="Open Discogs Settings", command=lambda: self.nb.select(self.tab_settings)).grid(row=0, column=2, sticky="w", padx=(12,0))

        # Preview panel
        split = ttk.Frame(f)
        split.grid(row=3, column=0, columnspan=3, sticky="nsew")
        split.columnconfigure(0, weight=1)
        split.rowconfigure(0, weight=1)

        preview = ttk.LabelFrame(split, text="Artwork preview", padding=10)
        preview.grid(row=0, column=0, sticky="nsew")
        preview.columnconfigure(0, weight=1)
        preview.rowconfigure(1, weight=1)

        ttk.Label(preview, textvariable=self._preview_caption, wraplength=820, justify="left").grid(row=0, column=0, sticky="ew", pady=(0,8))
        self.lbl_preview_img = ttk.Label(preview)
        self.lbl_preview_img.grid(row=1, column=0, sticky="nsew")

    def _browse_discogs_folder(self):
        p = filedialog.askdirectory(title="Select music folder")
        if p:
            self.discogs_folder.set(p)

    def _discogs_common_args(self):
        folder = self.discogs_folder.get().strip()
        if not folder:
            messagebox.showerror("Missing folder", "Please select a music folder.")
            return None
        args = [folder]
        if self.discogs_recursive.get():
            args.append("-r")
        return args

    def _discogs_run_update(self):
        args = self._discogs_common_args()
        if not args:
            return
        do_year = self.discogs_do_year.get()
        do_label = self.discogs_do_label.get()
        do_art = self.discogs_do_art.get()

        if not (do_year or do_label or do_art):
            messagebox.showwarning("Nothing selected", "Tick at least one of Year, Record label, or Artwork.")
            return

        self._clear_preview()
        discogs_key = (self.settings.data.get("discogs", {}) or {}).get("consumer_key", "").strip()
        discogs_secret = (self.settings.data.get("discogs", {}) or {}).get("consumer_secret", "").strip()
        discogs_user_agent = (self.settings.data.get("discogs", {}) or {}).get("user_agent", "Baseline (Music library maintenance for DJs)").strip() or "Baseline (Music library maintenance for DJs)"
        min_art = int((self.settings.data.get("discogs", {}) or {}).get("min_art_size", 500))

        # Provide credentials to Discogs scripts via environment variables
        if discogs_key and discogs_secret:
            os.environ["DISCOGS_KEY"] = discogs_key
            os.environ["DISCOGS_SECRET"] = discogs_secret
        os.environ["DISCOGS_USER_AGENT"] = discogs_user_agent

        # If Year or Label are selected, use discogs_years_labels_art.py (it updates year/label, and can also do art).
        # If only Artwork is selected, use UpdateArt.py which is artwork-specific and already includes y/n workflow.
        if do_year or do_label:
            argv = args + ["--min-art", str(min_art)]
            if not do_art:
                argv.append("--no-art")
            # Note: format priority Vinyl > CD > Digital is implemented inside the Discogs script, we will update that next.
            self._run_tool("Discogs/discogs_years_labels_art.py", argv, "Discogs update (year/label/artwork)")
        else:
            # Artwork only
            argv = ["update"] + args
            # UpdateArt.py uses its own placeholder logic, we will connect settings in the next pass.
            # Auto accept small art is implemented in the Discogs script later, UpdateArt prompts per file currently.
            self._run_tool("Discogs/UpdateArt.py", argv, "Discogs artwork update", patch_cover_preview=True)

    def _discogs_run_art_scan(self):
        args = self._discogs_common_args()
        if not args:
            return
        self._clear_preview()
        argv = ["scan"] + args
        self._run_tool("Discogs/UpdateArt.py", argv, "Artwork scan", patch_cover_preview=True)

    # Filename tab
    def _build_filename_tab(self):
        f = self.tab_filename
        f.columnconfigure(1, weight=1)

        self.fn_folder = tk.StringVar()
        self.fn_recursive = tk.BooleanVar(value=True)
        self.fn_out_csv = tk.StringVar(value="filename_suggestions.csv")
        self.fn_out_m3u8 = tk.BooleanVar(value=True)

        row = 0
        ttk.Label(f, text="Music folder:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.fn_folder).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_fn_folder).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Checkbutton(f, text="Recursive", variable=self.fn_recursive).grid(row=row, column=0, sticky="w", pady=(6,10))

        row += 1
        ttk.Label(f, text="Output CSV:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.fn_out_csv).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_fn_csv).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Checkbutton(f, text="Also create review playlist (.m3u8)", variable=self.fn_out_m3u8).grid(row=row, column=0, sticky="w", pady=(6,10))

        row += 1
        ttk.Button(f, text="Run filename check (suggestions only)", style="Primary.TButton", command=self._run_filename_check).grid(row=row, column=0, sticky="w")

        # Apply renames from an edited CSV (second step)
        row += 1
        ttk.Separator(f, orient="horizontal").grid(row=row, column=0, columnspan=3, sticky="ew", pady=(14, 10))

        self.fn_apply_csv = tk.StringVar()
        self.fn_do_rename = tk.BooleanVar(value=False)
        self.fn_update_mik_paths = tk.BooleanVar(value=True)

        row += 1
        ttk.Label(f, text="Edited CSV to apply:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.fn_apply_csv).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_fn_apply_csv).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Checkbutton(
            f,
            text="Actually rename files (dangerous, double-check your CSV first)",
            variable=self.fn_do_rename
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(6, 6))

        # Optional: keep Mixed In Key DB in sync after renaming
        mik_db = self._detect_mik_db()
        if mik_db:
            ttk.Checkbutton(
                f,
                text="Update Mixed In Key database paths after renaming",
                variable=self.fn_update_mik_paths
            ).grid(row=row+1, column=0, columnspan=3, sticky="w", pady=(0, 10))
            row += 1
        else:
            ttk.Label(
                f,
                text="Mixed In Key not detected, DB path sync is unavailable.",
            ).grid(row=row+1, column=0, columnspan=3, sticky="w", pady=(0, 10))
            row += 1

        row += 1
        ttk.Button(f, text="Apply renames from CSV", style="Primary.TButton", command=self._apply_filename_renames).grid(row=row, column=0, sticky="w")



        ttk.Label(
            f,
            text="Baseline will suggest filenames using: Artist feat./pres. in artist field, dash separator, and remix in parentheses at end. No renaming is performed.",
            wraplength=920
        ).grid(row=row+1, column=0, columnspan=3, sticky="w", pady=(12,0))

    def _browse_fn_folder(self):
        p = filedialog.askdirectory(title="Select music folder")
        if p:
            self.fn_folder.set(p)

    def _browse_fn_csv(self):
        p = filedialog.asksaveasfilename(
            title="Select output CSV",
            defaultextension=".csv",
            filetypes=[("CSV","*.csv"), ("All files","*.*")]
        )
        if p:
            self.fn_out_csv.set(p)

    def _browse_fn_apply_csv(self):
        p = filedialog.askopenfilename(
            title="Select edited filename suggestions CSV",
            filetypes=[("CSV","*.csv"), ("All files","*.*")]
        )
        if p:
            self.fn_apply_csv.set(p)


    def _run_filename_check(self):
        folder = self.fn_folder.get().strip()
        if not folder:
            messagebox.showerror("Missing folder", "Please select a music folder.")
            return

        argv = [folder, "--out", self.fn_out_csv.get().strip() or "filename_suggestions.csv"]
        if self.fn_recursive.get():
            argv.append("--recursive")
        if self.fn_out_m3u8.get():
            argv.append("--m3u8")

        self._run_tool("Filename/filename_check.py", argv, "Filename check (suggestions only)")


    def _apply_filename_renames(self):
        csv_path = self.fn_apply_csv.get().strip()
        if not csv_path:
            messagebox.showerror("Missing CSV", "Please select the edited CSV you want to apply.")
            return

        argv = [csv_path]
        if self.fn_do_rename.get():
            argv.append("--apply")
        else:
            argv.append("--dry-run")

        mik_db = self._detect_mik_db()
        if self.fn_do_rename.get() and mik_db and getattr(self, "fn_update_mik_paths", None) and self.fn_update_mik_paths.get():
            argv += ["--update-mik", "--mik-db", str(mik_db)]

        self._run_tool("Filename/filename_apply_renames.py", argv, "Filename rename (apply from CSV)")

    # Mixed In Key tab
    def _build_mik_tab(self):
        f = self.tab_mik
        f.columnconfigure(1, weight=1)

        self.mik_db_path = tk.StringVar()
        self.mik_report_path = tk.StringVar(value="report.csv")
        self.mik_dry_run = tk.BooleanVar(value=True)
        self.mik_apply = tk.BooleanVar(value=False)

        row = 0
        ttk.Label(f, text="MIK database:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.mik_db_path).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_mik_db).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        # attempt auto-detect
        autod = self._detect_mik_db()
        if autod and not self.mik_db_path.get():
            self.mik_db_path.set(str(autod))
            self.settings.data["mik"]["db_path"] = str(autod)
            self.settings.save()

        row += 1
        ttk.Label(f, text="Report file:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.mik_report_path).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_mik_report).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        opts = ttk.Frame(f)
        opts.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(6, 10))
        ttk.Checkbutton(
            opts, text="Dry-run (no changes)", variable=self.mik_dry_run, command=self._sync_mik_mode
        ).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(opts, text="Apply changes", variable=self.mik_apply, command=self._sync_mik_mode).grid(
            row=0, column=1, sticky="w", padx=(16, 0)
        )

        row += 1
        actions = ttk.LabelFrame(f, text="Actions", padding=10)
        actions.grid(row=row, column=0, columnspan=3, sticky="ew")
        actions.columnconfigure(0, weight=1)

        ttk.Button(actions, text="Prune missing files from DB", style="Primary.TButton", command=self._mik_prune_missing).grid(
            row=0, column=0, sticky="ew", pady=4
        )
        ttk.Button(actions, text="Sync tags from files into DB", command=self._mik_sync_tags).grid(
            row=1, column=0, sticky="ew", pady=4
        )
        ttk.Button(actions, text="Sync artwork from files into DB", command=self._mik_sync_artwork).grid(
            row=2, column=0, sticky="ew", pady=4
        )

    def _detect_mik_db(self, refresh: bool = False) -> Path | None:
        if self._mik_db_cache_ready and not refresh:
            return self._mik_db_cache

        found: Path | None = None
        # Known default path for MIK 11
        candidate = Path(os.path.expandvars(r"%LOCALAPPDATA%\Mixed In Key\Mixed In Key\11.0\MIKStore.db"))
        if candidate.exists():
            found = candidate
        else:
            # Scan for other versions
            base = Path(os.path.expandvars(r"%LOCALAPPDATA%\Mixed In Key\Mixed In Key"))
            if base.exists():
                cands = list(base.glob("**/MIKStore.db"))
                cands = [c for c in cands if c.is_file()]
                if cands:
                    # pick newest by modified time
                    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                    found = cands[0]

            # manual path from settings
            if found is None:
                p = (self.settings.data.get("mik", {}) or {}).get("db_path", "")
                if p:
                    pp = Path(p)
                    if pp.exists():
                        found = pp

        self._mik_db_cache = found
        self._mik_db_cache_ready = True
        return found

    def _browse_mik_db(self):
        p = filedialog.askopenfilename(
            title="Select Mixed In Key database",
            filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")],
        )
        if p:
            self.mik_db_path.set(p)
            self.settings.data["mik"]["db_path"] = p
            self.settings.save()
            self._mik_db_cache = Path(p)
            self._mik_db_cache_ready = True

    def _browse_mik_report(self):
        p = filedialog.asksaveasfilename(
            title="Select report file",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("Text", "*.txt"), ("All files", "*.*")],
        )
        if p:
            self.mik_report_path.set(p)

    def _sync_mik_mode(self):
        if self.mik_apply.get():
            self.mik_dry_run.set(False)
        if self.mik_dry_run.get():
            self.mik_apply.set(False)
        if (not self.mik_apply.get()) and (not self.mik_dry_run.get()):
            self.mik_dry_run.set(True)

    def _mik_common_args(self) -> list[str] | None:
        db = self.mik_db_path.get().strip()
        if not db:
            messagebox.showerror("Missing database", "Please select your Mixed In Key database first.")
            return None
        dbp = Path(db)
        if dbp.exists():
            self._mik_db_cache = dbp
            self._mik_db_cache_ready = True
        report = self.mik_report_path.get().strip()

        args = [db]
        if self.mik_apply.get():
            args.append("--apply")
        else:
            args.append("--dry-run")
        if report:
            args += ["--report", report]
        return args

    def _mik_prune_missing(self):
        args = self._mik_common_args()
        if not args:
            return
        self._run_tool("MixedinKey/mik_prune_missing.py", args, "Prune missing files")

    def _mik_sync_tags(self):
        args = self._mik_common_args()
        if not args:
            return
        self._run_tool("MixedinKey/mik_sync_tags_from_files.py", args, "Sync tags from files")

    def _mik_sync_artwork(self):
        args = self._mik_common_args()
        if not args:
            return
        self._run_tool("MixedinKey/mik_sync_artwork.py", args, "Sync artwork from files")


    # Rekordbox XML tab

    def _build_rekordbox_tab(self):
        f = self.tab_rekordbox
        f.columnconfigure(1, weight=1)

        self.rb_xml_path = tk.StringVar()
        self.rb_music_root = tk.StringVar()
        self.rb_mik_csv = tk.StringVar()
        self.rb_outdir = tk.StringVar(value=str((ROOT / "baseline_work" / "rekordbox_reports").resolve()))

        row = 0
        ttk.Label(f, text="Rekordbox XML:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.rb_xml_path).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_rb_xml).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Label(f, text="Music root (optional, for relink):").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.rb_music_root).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_rb_root).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Label(f, text="MIK CSV (optional, compare):").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.rb_mik_csv).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_rb_mik).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        ttk.Label(f, text="Output folder:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.rb_outdir).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=self._browse_rb_outdir).grid(row=row, column=2, sticky="ew", padx=(8,0), pady=4)

        row += 1
        btns = ttk.Frame(f)
        btns.grid(row=row, column=0, columnspan=3, sticky="w", pady=(10,4))
        ttk.Button(btns, text="Run full analysis", style="Primary.TButton", command=self._run_rekordbox_analysis).pack(side="left")

        ttk.Label(f, text="This runs: overview, data quality, duplicates, playlists, missing files, artwork scan, and MIK compare (if provided).").grid(
            row=row+1, column=0, columnspan=3, sticky="w", pady=(6,0)
        )

    def _browse_rb_xml(self):
        p = filedialog.askopenfilename(
            title="Select Rekordbox XML export",
            filetypes=[("Rekordbox XML", "*.xml"), ("All files", "*.*")]
        )
        if p:
            self.rb_xml_path.set(p)

    def _browse_rb_root(self):
        p = filedialog.askdirectory(title="Select music root folder")
        if p:
            self.rb_music_root.set(p)

    def _browse_rb_mik(self):
        p = filedialog.askopenfilename(
            title="Select Mixed In Key CSV export",
            filetypes=[("CSV", "*.csv"), ("All files", "*.*")]
        )
        if p:
            self.rb_mik_csv.set(p)

    def _browse_rb_outdir(self):
        p = filedialog.askdirectory(title="Select output folder")
        if p:
            self.rb_outdir.set(p)

    def _run_rekordbox_analysis(self):
        xml = self.rb_xml_path.get().strip()
        if not xml:
            messagebox.showerror("Missing XML", "Please select a Rekordbox XML export first.")
            return

        outdir = self.rb_outdir.get().strip()
        args = [xml]
        if outdir:
            args += ["--outdir", outdir]

        root = self.rb_music_root.get().strip()
        if root:
            args += ["--music-root", root]

        mik = self.rb_mik_csv.get().strip()
        if mik:
            args += ["--mik-csv", mik]

        # Use Baseline settings so placeholder artwork can be detected
        settings_fp = SETTINGS_PATH
        if not settings_fp.exists():
            for legacy_path in LEGACY_SETTINGS_PATHS:
                if legacy_path.exists():
                    settings_fp = legacy_path
                    break
        if settings_fp.exists():
            args += ["--settings", str(settings_fp)]

        self._run_tool("Rekordbox/rekordbox_analyse.py", args, "Rekordbox XML analysis")

    # Settings tab

    def _build_settings_tab(self):
        f = self.tab_settings
        f.columnconfigure(1, weight=1)

        row = 0
        ttk.Label(f, text="Settings file:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Label(f, text=str(SETTINGS_PATH)).grid(row=row, column=1, sticky="w", pady=4)

        row += 1
        ttk.Label(f, text="Logs folder:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Label(f, text=str(LOGS_ROOT)).grid(row=row, column=1, sticky="w", pady=4)

        row += 1
        top_actions = ttk.Frame(f)
        top_actions.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(2, 10))
        ttk.Button(top_actions, text="Save settings", style="Primary.TButton", command=self._save_settings).grid(row=0, column=0, sticky="w")
        ttk.Label(top_actions, text=f"Saves to: {SETTINGS_PATH}").grid(row=0, column=1, sticky="w", padx=(12, 0))

        row += 1
        sep = ttk.Separator(f, orient="horizontal")
        sep.grid(row=row, column=0, columnspan=3, sticky="ew", pady=12)

        row += 1
        ttk.Label(f, text="Discogs Settings", font=("Segoe UI", 12, "bold")).grid(row=row, column=0, sticky="w", pady=(0, 8))

        discogs = self.settings.data.get("discogs", {}) or {}

        # Credentials
        self.set_discogs_key = tk.StringVar(value=str(discogs.get("consumer_key", "")))
        self.set_discogs_secret = tk.StringVar(value=str(discogs.get("consumer_secret", "")))
        self.set_discogs_user_agent = tk.StringVar(value=str(discogs.get("user_agent", "Baseline (Music library maintenance for DJs)")))

        row += 1
        ttk.Label(f, text="Consumer key:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.set_discogs_key).grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        ttk.Label(f, text="Consumer secret:").grid(row=row, column=0, sticky="w", pady=4)
        self._discogs_secret_entry = ttk.Entry(f, textvariable=self.set_discogs_secret, show="•")
        self._discogs_secret_entry.grid(row=row, column=1, sticky="ew", pady=4)
        self.set_discogs_show_secret = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            f,
            text="Show",
            variable=self.set_discogs_show_secret,
            command=self._toggle_discogs_secret_visibility
        ).grid(row=row, column=2, sticky="w", padx=(8, 0), pady=4)

        row += 1
        ttk.Label(f, text="User-Agent:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.set_discogs_user_agent).grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        help_txt = "How to get keys: Discogs website, Settings, Developers, Create an application, then copy your consumer key and secret."
        ttk.Label(f, text=help_txt, wraplength=720).grid(row=row, column=0, columnspan=3, sticky="w", pady=(0, 8))

        row += 1
        ttk.Button(f, text="Test Discogs connection", command=self._test_discogs_connection).grid(row=row, column=0, sticky="w", pady=(0, 10))

        row += 1
        sep2 = ttk.Separator(f, orient="horizontal")
        sep2.grid(row=row, column=0, columnspan=3, sticky="ew", pady=12)

        # Artwork rules
        self.set_discogs_min_art = tk.StringVar(value=str(int(discogs.get("min_art_size", 500))))
        self.set_discogs_auto_small = tk.BooleanVar(value=bool(discogs.get("auto_accept_resize_small_art", False)))
        self.set_discogs_placeholder = tk.StringVar(value=str(discogs.get("placeholder_image", "")))
        self.set_discogs_fallback = tk.StringVar(value=str(discogs.get("fallback_image", "")))

        row += 1
        ttk.Label(f, text="Minimum artwork size (px):").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.set_discogs_min_art, width=10).grid(row=row, column=1, sticky="w", pady=4)

        row += 1
        ttk.Checkbutton(
            f,
            text="Auto accept and resize Discogs artwork below minimum (not default)",
            variable=self.set_discogs_auto_small
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=4)

        row += 1
        ttk.Label(f, text="Placeholder image:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.set_discogs_placeholder).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=lambda: self._pick_image(self.set_discogs_placeholder)).grid(row=row, column=2, sticky="ew", padx=(8, 0), pady=4)

        row += 1
        ttk.Label(f, text="Fallback image:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(f, textvariable=self.set_discogs_fallback).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(f, text="Browse", command=lambda: self._pick_image(self.set_discogs_fallback)).grid(row=row, column=2, sticky="ew", padx=(8, 0), pady=4)

    def _toggle_discogs_secret_visibility(self):
        if getattr(self, "_discogs_secret_entry", None) is None:
            return
        self._discogs_secret_entry.configure(show="" if self.set_discogs_show_secret.get() else "•")

    def _test_discogs_connection(self):
        key = self.set_discogs_key.get().strip()
        secret = self.set_discogs_secret.get().strip()
        ua = self.set_discogs_user_agent.get().strip() or "Baseline (Music library maintenance for DJs)"
        if not key or not secret:
            messagebox.showerror("Discogs keys required", "Please enter your Discogs consumer key and consumer secret first.")
            return
        try:
            import requests
            url = "https://api.discogs.com/database/search"
            params = {"q": "test", "per_page": 1, "key": key, "secret": secret}
            headers = {"User-Agent": ua}
            r = requests.get(url, params=params, headers=headers, timeout=15)
            if r.status_code == 200:
                messagebox.showinfo("Discogs OK", "Discogs connection successful. Click Save settings to store your keys.")
            else:
                messagebox.showerror("Discogs failed", f"Discogs request failed. HTTP {r.status_code}\\n\\n{r.text[:500]}")
        except Exception as e:
            messagebox.showerror("Discogs failed", f"Discogs connection failed.\\n\\n{e}")

    def _pick_image(self, var: tk.StringVar):
        p = filedialog.askopenfilename(
            title="Select image",
            filetypes=[("Images", "*.jpg *.jpeg *.png *.webp"), ("All files", "*.*")]
        )
        if p:
            var.set(p)

    def _save_settings(self):
        try:
            d = self.settings.data.get("discogs", {}) or {}
            d["consumer_key"] = self.set_discogs_key.get().strip()
            d["consumer_secret"] = self.set_discogs_secret.get().strip()
            d["user_agent"] = self.set_discogs_user_agent.get().strip() or "Baseline (Music library maintenance for DJs)"
            d["auto_accept_resize_small_art"] = bool(self.set_discogs_auto_small.get())
            d["placeholder_image"] = self.set_discogs_placeholder.get().strip()
            d["fallback_image"] = self.set_discogs_fallback.get().strip()

            min_art_raw = (self.set_discogs_min_art.get() or "").strip()
            if not min_art_raw:
                min_art_val = 500
            else:
                min_art_val = int(min_art_raw)
            if min_art_val <= 0:
                raise ValueError("Minimum artwork size must be a positive integer.")
            d["min_art_size"] = min_art_val

            self.settings.data["discogs"] = d
            self.settings.save()
            messagebox.showinfo("Saved", f"Settings saved.\n\nFile:\n{SETTINGS_PATH}")
        except Exception as e:
            messagebox.showerror("Error", str(e))


def main():

    app = App()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
