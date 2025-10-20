#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oracle module for Database Pulse with column filtering that also applies to Email Report.

- Two connection modes per instance:
  1) TNS Alias / full descriptor (requires Oracle Client for name resolution)  [mode="TNS"]
  2) Thin (no client): host/port + Service Name or SID                          [mode="THIN"]
- Client library path set once on toolbar (used to init thick mode).
- Columns (kept compatible with your v18 layout order request):
  S.No, DB_name, Environment, Host, DB Version, Startup time, Status, Inst_status,
  Sessions, WorstTS%, TS Online, DB Size, LastFull/inc, LastArc, MS, LastChecked,
  Check Status, Error

- Adds:
  * Filter… button + Clear Filter
  * Filters hide/show rows (Treeview detach/reattach) AND email uses only visible rows.
  * Email columns + display columns customizers kept.
  * Config persisted to config/oracle_config.json (next to the script / exe).
"""

import base64
import json
import os
import re
import sys
import time
import locale
import threading
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter import font as tkfont

APP_NAME = "Database Pulse"
APP_VERSION = "Database Pulse v1.0"

GOOD = "✅"
BAD = "❌"
DEFAULT_INTERVAL_SEC = 300  # 5 minutes

# ---------------- Paths / config ----------------
def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

CONFIG_DIR = (_base_dir() / "config")
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "oracle_config.json"


# ---------------- Password helpers ----------------
def _win_protect(data: bytes) -> str:
    try:
        import ctypes
        import ctypes.wintypes as wt

        class DATA_BLOB(ctypes.Structure):
            _fields_ = [("cbData", wt.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

        CryptProtectData = ctypes.windll.crypt32.CryptProtectData
        blob_in = DATA_BLOB(len(data), ctypes.cast(ctypes.create_string_buffer(data), ctypes.POINTER(ctypes.c_char)))
        blob_out = DATA_BLOB()
        if not CryptProtectData(ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)):
            raise OSError("CryptProtectData failed")
        try:
            encrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)
        return base64.b64encode(encrypted).decode("ascii")
    except Exception:
        return base64.b64encode(data).decode("ascii")

def _win_unprotect(s: str) -> bytes:
    raw = base64.b64decode(s.encode("ascii"))
    try:
        import ctypes
        import ctypes.wintypes as wt

        class DATA_BLOB(ctypes.Structure):
            _fields_ = [("cbData", wt.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

        CryptUnprotectData = ctypes.windll.crypt32.CryptUnprotectData
        blob_in = DATA_BLOB(len(raw), ctypes.cast(ctypes.create_string_buffer(raw), ctypes.POINTER(ctypes.c_char)))
        blob_out = DATA_BLOB()
        if not CryptUnprotectData(ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)):
            raise OSError("CryptUnprotectData failed")
        try:
            decrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)
        return decrypted
    except Exception:
        return raw

def _encrypt_password(plain: Optional[str]) -> Optional[str]:
    if not plain:
        return None
    if sys.platform.startswith("win"):
        return _win_protect(plain.encode("utf-8"))
    return base64.b64encode(plain.encode("utf-8")).decode("ascii")

def _decrypt_password(enc: Optional[str]) -> Optional[str]:
    if not enc:
        return None
    try:
        if sys.platform.startswith("win"):
            return _win_unprotect(enc).decode("utf-8")
        return base64.b64decode(enc.encode("ascii")).decode("utf-8")
    except Exception:
        return None


# ---------------- Oracle driver ----------------
_oracle_init_done = False
def _ensure_oracle_client(lib_dir: Optional[str]) -> None:
    """Initialize thick client once if lib_dir is provided."""
    global _oracle_init_done
    if _oracle_init_done or not lib_dir:
        return
    try:
        import oracledb
        oracledb.init_oracle_client(lib_dir=lib_dir)
        _oracle_init_done = True
    except Exception as e:
        raise RuntimeError(f"Failed to init Oracle Client: {e}")

def _oracle_connect_tns(user: str, password: str, tns: str, lib_dir: Optional[str]) -> "oracledb.Connection":
    import oracledb
    if lib_dir:
        _ensure_oracle_client(lib_dir)
    return oracledb.connect(user=user, password=password, dsn=tns, encoding="UTF-8")

def _oracle_connect_thin(user: str, password: str, host: str, port: int,
                         use_service: bool, svc_or_sid: str) -> "oracledb.Connection":
    import oracledb
    # thin mode requires no init_oracle_client
    if use_service:
        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))" \
              f"(CONNECT_DATA=(SERVICE_NAME={svc_or_sid})))"
    else:
        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))" \
              f"(CONNECT_DATA=(SID={svc_or_sid})))"
    return oracledb.connect(user=user, password=password, dsn=dsn, encoding="UTF-8")


# ---------------- Data model ----------------
@dataclass
class DbTarget:
    name: str             # DB_name (display)
    environment: str      # NON-PROD/PROD
    mode: str             # "TNS" or "THIN"
    # Common
    user: str
    password_enc: Optional[str] = None
    host: str = ""        # used in THIN
    port: int = 1521
    use_service_name: bool = True
    service_or_sid: str = ""
    tns_alias: str = ""   # used in TNS
    client_lib_dir: Optional[str] = None
    # meta
    last_host_display: str = ""

@dataclass
class DbHealth:
    version: str = "-"
    startup_time: str = "-"
    status: str = "-"
    inst_status: str = "-"
    sessions: str = "-"  # "cur/limit"
    worst_ts_pct: Optional[float] = None
    ts_online: Tuple[int, int] = (0, 0)  # online/total
    db_size_gb: Optional[float] = None
    last_full_inc: Optional[str] = None
    last_arch: Optional[str] = None
    elapsed_ms: int = 0
    ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    error: str = ""


# ---------------- Queries ----------------
Q_VERSION = [
    ("select version_full from v$instance", "version_full"),
    ("select version from v$instance", "version"),
]

Q_STARTUP = "select to_char(startup_time,'YYYY-MM-DD HH24:MI:SS') from v$instance"

Q_STATUS = "select status, instance_status from v$instance"

Q_SESSIONS = """
select
  (select count(*) from v$session where username is not null) as cur,
  (select value from v$parameter where name = 'sessions') as lim
from dual
"""

# Prefer v$tablespace_usage_metrics if available; else compute
Q_WORST_TS_METRICS = """
select max(used_percent) from dba_tablespace_usage_metrics
"""

Q_WORST_TS_FALLBACK = """
with ts as (
  select df.tablespace_name,
         sum(df.bytes) as total_bytes,
         nvl(sum(fs.bytes),0) as free_bytes
  from dba_data_files df
  left join dba_free_space fs on fs.tablespace_name = df.tablespace_name
  group by df.tablespace_name
)
select max( round( ( (total_bytes - free_bytes) / nullif(total_bytes,0) ) * 100, 1) ) from ts
"""

Q_TS_ONLINE = """
select count(*) total,
       sum(case when upper(status)='ONLINE' then 1 else 0 end) tonline
from dba_tablespaces
"""

Q_DB_SIZE_GB = """
select round( sum(bytes)/1024/1024/1024, 2) from dba_data_files
"""

# Last datafile backup (full/inc) across all DBs (controlfile-based)
Q_LAST_FULL_INC = """
select to_char(max(b.completion_time),'YYYY-MM-DD HH24:MI:SS')
from v$backup_set_details b
join v$backup_set s on s.set_stamp = b.set_stamp and s.set_count = b.set_count
where b.input_type like 'DB%'  -- datafile / full or incremental
"""

Q_LAST_ARCH = """
select to_char(max(completion_time),'YYYY-MM-DD HH24:MI:SS')
from v$backup_archivelog
"""

# ---------------- Helpers ----------------
def logical_columns() -> List[str]:
    return [
        "S.No","DB_name","Environment","Host","DB Version","Startup time","Status","Inst_status",
        "Sessions","WorstTS%","TS Online","DB Size","LastFull/inc","LastArc","MS","LastChecked","Check Status","Error"
    ]

def default_config() -> Dict[str, Any]:
    cols = logical_columns()
    return {
        "interval_sec": DEFAULT_INTERVAL_SEC,
        "client_lib_dir": "",  # Oracle Client (optional for TNS)
        "email": {"server": "", "port": 25, "from_addr": "", "to_addrs": "", "subject": "Oracle Health Report"},
        "dbs": [],
        "last_health": {},
        "auto_run": False,
        "column_order": cols[:],
        "visible_columns": cols[:],
        "email_columns": cols[:],
        "column_widths": {},
        # Filtering
        "active_filter": [],  # list[(col, op, val)]
    }

def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            base = default_config()
            # merge shallow
            for k, v in cfg.items():
                if k == "email":
                    base["email"].update(v or {})
                else:
                    base[k] = v
            base.setdefault("column_widths", {})
            base.setdefault("active_filter", [])
            return base
        except Exception:
            pass
    return default_config()

def save_config(cfg: Dict[str, Any]):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

def _serialize_db(t: DbTarget) -> Dict[str, Any]:
    return {
        "name": t.name, "environment": t.environment, "mode": t.mode, "user": t.user,
        "password_enc": t.password_enc, "host": t.host, "port": t.port, "use_service_name": t.use_service_name,
        "service_or_sid": t.service_or_sid, "tns_alias": t.tns_alias, "client_lib_dir": t.client_lib_dir,
        "last_host_display": t.last_host_display,
    }

def _hydrate_db(d: Dict[str, Any]) -> DbTarget:
    return DbTarget(
        name=d.get("name",""), environment=d.get("environment","NON-PROD"), mode=d.get("mode","TNS"),
        user=d.get("user",""), password_enc=d.get("password_enc"), host=d.get("host",""),
        port=int(d.get("port",1521)), use_service_name=bool(d.get("use_service_name", True)),
        service_or_sid=d.get("service_or_sid",""), tns_alias=d.get("tns_alias",""),
        client_lib_dir=d.get("client_lib_dir") or None, last_host_display=d.get("last_host_display","")
    )


# ---------------- UI ----------------
class OracleMonitorApp(ttk.Frame):
    LOGICAL_COLUMNS = tuple(logical_columns())
    STATUS_COLUMNS = {"DB Version","Startup time","Status","Inst_status","Sessions",
                      "WorstTS%","TS Online","DB Size","LastFull/inc","LastArc","MS","LastChecked","Check Status","Error"}

    def __init__(self, master):
        super().__init__(master)
        self.cfg = load_config()
        self.interval_sec = int(self.cfg.get("interval_sec", DEFAULT_INTERVAL_SEC))
        self.client_lib_dir = self.cfg.get("client_lib_dir","")
        self.email_cfg = dict(self.cfg.get("email", {}))
        self.dbs: List[DbTarget] = [_hydrate_db(d) for d in self.cfg.get("dbs", [])]
        self.last_health: Dict[str, Dict[str, Any]] = self.cfg.get("last_health", {})
        self._auto_flag = False

        # Filtering state
        self._active_filter: List[Tuple[str,str,str]] = [tuple(x) for x in self.cfg.get("active_filter", [])]
        self._detached: set[str] = set()  # hidden rows

        self._build_ui()
        self._refresh_table_from_dbs()
        self._load_last_health_into_rows()

        if self.cfg.get("auto_run", False):
            self.auto_var.set(True)
            self._start_auto()

    # ---------- Build UI ----------
    def _build_ui(self):
        self.grid_rowconfigure(0, weight=0)
        self.grid_rowconfigure(1, weight=0)
        self.grid_rowconfigure(2, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self._font = tkfont.nametofont("TkDefaultFont")
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("OR.Treeview.Heading", background="#cfe8ff", foreground="#000",
                        font=(self._font.actual("family"), self._font.actual("size"), "bold"))
        style.map("OR.Treeview.Heading", background=[("active","#b7dbff")])
        style.configure("OR.Treeview", rowheight=22)

        # Toolbar (row 0)
        t1 = ttk.Frame(self)
        t1.grid(row=0, column=0, sticky="ew", padx=8, pady=(8,3))

        self.interval_var = tk.IntVar(value=self.interval_sec)
        ttk.Label(t1, text="Interval (sec):").pack(side=tk.LEFT)
        ttk.Spinbox(t1, from_=30, to=3600, textvariable=self.interval_var, width=8).pack(side=tk.LEFT, padx=(4,10))
        self.auto_var = tk.BooleanVar(value=self.cfg.get("auto_run", False))
        ttk.Checkbutton(t1, text="Auto-run", variable=self.auto_var, command=self._toggle_auto).pack(side=tk.LEFT, padx=(0,8))
        ttk.Button(t1, text="Run All", command=self.run_all_once).pack(side=tk.LEFT)
        ttk.Button(t1, text="Run Selected", command=self.run_selected_once).pack(side=tk.LEFT, padx=(6,10))
        ttk.Button(t1, text="Clear All", command=self._clear_all_rows).pack(side=tk.LEFT, padx=(6,4))
        ttk.Button(t1, text="Clear Selected", command=self._clear_selected_row).pack(side=tk.LEFT, padx=(4,10))

        ttk.Button(t1, text="Add DB", command=self._add_dialog).pack(side=tk.LEFT, padx=(10,0))
        ttk.Button(t1, text="Edit DB", command=self._edit_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Remove DB", command=self._remove_selected).pack(side=tk.LEFT)

        ttk.Button(t1, text="Import Config", command=self._import_json).pack(side=tk.LEFT, padx=(10,0))
        ttk.Button(t1, text="Export Config", command=self._export_json).pack(side=tk.LEFT)

        ttk.Separator(t1, orient="vertical").pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(t1, text="Customize Columns", command=self._customize_columns).pack(side=tk.LEFT, padx=(0,6))
        ttk.Button(t1, text="Select Columns", command=self._select_columns_dialog).pack(side=tk.LEFT, padx=(0,10))

        ttk.Separator(t1, orient="vertical").pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(t1, text="Filter…", command=self._open_filter_dialog).pack(side=tk.LEFT, padx=(0,6))
        ttk.Button(t1, text="Clear Filter", command=self._clear_filter).pack(side=tk.LEFT, padx=(0,10))

        ttk.Separator(t1, orient="vertical").pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Label(t1, text="Client Lib dir:").pack(side=tk.LEFT)
        self.client_dir_var = tk.StringVar(value=self.client_lib_dir)
        ttk.Entry(t1, textvariable=self.client_dir_var, width=28).pack(side=tk.LEFT, padx=(4,4))
        ttk.Button(t1, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT)

        # Email bar (row 1)
        t2 = ttk.Frame(self)
        t2.grid(row=1, column=0, sticky="ew", padx=8, pady=(0,6))
        ttk.Label(t2, text="SMTP/Exchange:").pack(side=tk.LEFT)
        self.smtp_server_var = tk.StringVar(value=self.email_cfg.get("server",""))
        self.smtp_port_var = tk.IntVar(value=int(self.email_cfg.get("port",25)))
        ttk.Entry(t2, textvariable=self.smtp_server_var, width=22).pack(side=tk.LEFT, padx=(4, 2))
        ttk.Entry(t2, textvariable=self.smtp_port_var, width=6).pack(side=tk.LEFT, padx=(2, 6))
        ttk.Label(t2, text="From:").pack(side=tk.LEFT)
        self.from_var = tk.StringVar(value=self.email_cfg.get("from_addr",""))
        ttk.Entry(t2, textvariable=self.from_var, width=24).pack(side=tk.LEFT, padx=(4, 6))
        ttk.Label(t2, text="To (comma):").pack(side=tk.LEFT)
        self.to_var = tk.StringVar(value=self.email_cfg.get("to_addrs",""))
        ttk.Entry(t2, textvariable=self.to_var, width=32).pack(side=tk.LEFT, padx=(4, 6))
        ttk.Button(t2, text="Save Mail", command=self._save_mail_settings).pack(side=tk.LEFT, padx=(6,0))
        ttk.Button(t2, text="Email Columns", command=self._select_email_columns_dialog).pack(side=tk.LEFT, padx=(6,0))
        ttk.Button(t2, text="Email Report", command=self._email_report).pack(side=tk.LEFT, padx=(6,0))

        # Table (row 2)
        frame = ttk.Frame(self)
        frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=8)
        self.tree = ttk.Treeview(frame, columns=self.LOGICAL_COLUMNS, show="headings",
                                 height=20, style="OR.Treeview")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=xsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)

        for col in self.LOGICAL_COLUMNS:
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c, False))
            self.tree.column(col, width=120, stretch=True, anchor="w")

        # Apply saved display columns
        order = [c for c in self.cfg.get("column_order", list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
        if not order or order[0] != "S.No":
            order = ["S.No"] + [c for c in self.LOGICAL_COLUMNS if c != "S.No"]
        visible = [c for c in self.cfg.get("visible_columns", order) if c in self.LOGICAL_COLUMNS]
        if not visible or visible[0] != "S.No":
            visible = ["S.No"] + [c for c in visible if c != "S.No"]
        display = [c for c in order if c in visible]
        self.tree["displaycolumns"] = display

        # Persisted widths
        for col, w in self.cfg.get("column_widths", {}).items():
            try:
                self.tree.column(col, width=int(w))
            except Exception:
                pass

        # Context menu
        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="Copy Cell", command=self._copy_cell)
        self.menu.add_separator()
        self.menu.add_command(label="Copy DB Name", command=lambda: self._copy_by_col("DB_name"))
        self.menu.add_command(label="Copy Host", command=lambda: self._copy_by_col("Host"))
        self.menu.add_command(label="Copy Error", command=lambda: self._copy_by_col("Error"))
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<ButtonRelease-1>", lambda e: self._persist_column_layout())

        # Bottom status
        bottom = ttk.Frame(self)
        bottom.grid(row=3, column=0, sticky="ew", padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottom, textvariable=self.status_var).pack(side=tk.LEFT)

    # ---------- Helpers ----------
    def _pick_client_dir(self):
        d = filedialog.askdirectory(title="Select Oracle Client lib dir (optional for TNS)")
        if d:
            self.client_dir_var.set(d)
            self.client_lib_dir = d
            self.cfg["client_lib_dir"] = d
            save_config(self.cfg)

    def _toggle_auto(self):
        if self.auto_var.get():
            self._start_auto()
        else:
            self._stop_auto()
        self.cfg["auto_run"] = self.auto_var.get()
        save_config(self.cfg)

    def _start_auto(self):
        if getattr(self, "_auto_flag", False):
            return
        self._auto_flag = True
        self.after(200, self._loop)

    def _stop_auto(self):
        self._auto_flag = False

    def _loop(self):
        if not self.auto_var.get():
            return
        self._checks_async(self.dbs)
        self.after(self.interval_var.get() * 1000, self._loop)

    def _persist_column_layout(self):
        widths = {col: self.tree.column(col, option="width") for col in self.LOGICAL_COLUMNS}
        self.cfg["column_widths"] = widths
        visible = list(self.tree["displaycolumns"])
        full = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
        new_full, seen = [], set()
        for c in visible:
            if c not in seen:
                new_full.append(c); seen.add(c)
        for c in full:
            if c not in seen and c in self.LOGICAL_COLUMNS:
                new_full.append(c); seen.add(c)
        self.cfg["column_order"] = new_full
        self.cfg["visible_columns"] = visible
        save_config(self.cfg)

    def _autosize_columns(self):
        pad = 24
        visible = list(self.tree["displaycolumns"])
        font = tkfont.nametofont("TkDefaultFont")
        for col in visible:
            header_w = font.measure(col)
            max_w = header_w
            for iid in self.tree.get_children(""):
                vals = self.tree.item(iid)["values"]
                try:
                    idx = self.LOGICAL_COLUMNS.index(col)
                    txt = str(vals[idx]) if idx < len(vals) else ""
                    tw = font.measure(txt)
                    max_w = max(max_w, tw)
                except Exception:
                    pass
            new_w = max(max_w + pad, 90)
            cur = self.tree.column(col, option="width")
            if cur < new_w:
                self.tree.column(col, width=new_w)

    # Context menu
    def _show_context_menu(self, event):
        iid = self.tree.identify_row(event.y)
        cid = self.tree.identify_column(event.x)
        if iid:
            self.tree.selection_set(iid)
            self._context_row = iid
            self._context_col = cid
            self.menu.tk_popup(event.x_root, event.y_root)

    def _copy_cell(self):
        row = getattr(self, "_context_row", None)
        colid = getattr(self, "_context_col", None)
        if not row or not colid:
            return
        col_index = int(colid.replace("#","")) - 1
        vals = self.tree.item(row)["values"]
        text = str(vals[col_index]) if col_index < len(vals) else ""
        self.clipboard_clear(); self.clipboard_append(text)

    def _copy_by_col(self, colname: str):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]
        vals = self.tree.item(iid)["values"]
        idx = self.LOGICAL_COLUMNS.index(colname)
        text = str(vals[idx]) if idx < len(vals) else ""
        self.clipboard_clear(); self.clipboard_append(text)

    # Sorting
    def _parse_sessions(self, s: str) -> Tuple[int,int]:
        try:
            a,b = str(s).split("/"); return int(a), int(b)
        except Exception: return (0,0)

    def _parse_pct(self, s: str) -> float:
        try:
            return float(str(s).replace("%","").split()[0])
        except Exception: return -1.0

    def _parse_datecell(self, s: str) -> float:
        t = str(s).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d"):
            try:
                return datetime.strptime(t, fmt).timestamp()
            except Exception: pass
        return float("-inf")

    def _status_rank(self, s:str) -> int:
        return 1 if str(s).strip().startswith(GOOD) else 0

    def _generic_key(self, col: str, s: str):
        if col=="S.No":
            try: return (int(s),)
            except: return (0,)
        if col in ("Status","Inst_status"):
            return (self._status_rank(s), s)
        if col=="Sessions":
            a,b = self._parse_sessions(s); return (a/(b or 1.0), a, b)
        if col in ("WorstTS%","DB Size"):
            return (self._parse_pct(s),)
        if col in ("Startup time","LastFull/inc","LastArc","LastChecked"):
            return (self._parse_datecell(s),)
        return (str(s).lower(),)

    def _sort_by_column(self, col: str, descending: bool):
        rows = [(self._generic_key(col, self.tree.set(k, col)), k) for k in self.tree.get_children("")]
        rows.sort(reverse=descending, key=lambda x: x[0])
        for idx, (_, k) in enumerate(rows):
            self.tree.move(k, "", idx)
        self._renumber()
        self.tree.heading(col, command=lambda c=col: self._sort_by_column(c, not descending))

    def _renumber(self):
        for i, iid in enumerate(self.tree.get_children(""), start=1):
            vals = list(self.tree.item(iid)["values"])
            if vals:
                vals[0] = i
                self.tree.item(iid, values=vals)

    # ---------- Table refresh / persistence ----------
    def _refresh_table_from_dbs(self):
        for i in self.tree.get_children(""):
            self.tree.delete(i)
        for idx, t in enumerate(self.dbs, start=1):
            host_disp = t.tns_alias if t.mode=="TNS" else (t.host or "-")
            values = ["-"] * len(self.LOGICAL_COLUMNS)
            values[0] = idx
            values[1] = t.name
            values[2] = t.environment
            values[3] = host_disp
            self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber()
        self._autosize_columns()
        # after rebuild, re-apply filter (detaches rows)
        if self._active_filter:
            self._apply_filter_now()

    def _load_last_health_into_rows(self):
        for t in self.dbs:
            h = self.last_health.get(t.name)
            if not h: continue
            self._apply_persisted_row(t.name, h)
        self._autosize_columns()
        if self._active_filter:
            self._apply_filter_now()

    def _apply_persisted_row(self, name: str, h: Dict[str, Any]):
        vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
        def mark(ok: bool)->str: return GOOD if ok else BAD

        # Sessions
        sessions = f"{h.get('cur_sess',0)}/{h.get('lim_sess',0)}"
        try:
            cur = int(h.get("cur_sess",0)); lim = int(h.get("lim_sess",0) or 1)
            ok_sess = (cur/lim) < 0.95
        except Exception:
            ok_sess = False
        # WorstTS%
        worst = float(h.get("worst_ts_pct",-1.0)) if h.get("worst_ts_pct") is not None else -1.0
        ok_ts = (worst >= 0 and worst < 90.0)

        colidx = {c:i for i,c in enumerate(self.LOGICAL_COLUMNS)}
        vals[colidx["DB Version"]] = h.get("version","-")
        vals[colidx["Startup time"]] = h.get("startup_time","-")
        vals[colidx["Status"]] = f"{mark(str(h.get('status','')).upper()=='OPEN')} {h.get('status','-')}"
        vals[colidx["Inst_status"]] = f"{mark(str(h.get('inst_status','')).upper().startswith('OPEN'))} {h.get('inst_status','-')}"
        vals[colidx["Sessions"]] = (f"{'✅' if ok_sess else '❌'} {sessions}")
        vals[colidx["WorstTS%"]] = (f"{'✅' if ok_ts else '❌'} {worst:.1f}%" if worst>=0 else f"{BAD} -")
        ts_on = h.get("ts_online", [0,0]); vals[colidx["TS Online"]] = f"{'✅' if ts_on and ts_on[0]==ts_on[1] and ts_on[1]>0 else '❌'} {ts_on[0]}/{ts_on[1]}"
        size_gb = h.get("db_size_gb"); vals[colidx["DB Size"]] = (f"{size_gb:.2f} GB" if size_gb is not None else "-")
        vals[colidx["LastFull/inc"]] = h.get("last_full_inc","-")
        vals[colidx["LastArc"]] = h.get("last_arch","-")
        vals[colidx["MS"]] = str(h.get("elapsed_ms",0))
        vals[colidx["LastChecked"]] = h.get("ts","-")
        vals[colidx["Check Status"]] = "Complete"
        vals[colidx["Error"]] = h.get("error","")
        self.tree.item(name, values=vals)

    def _persist_all(self):
        self.cfg["interval_sec"] = self.interval_var.get()
        self.cfg["client_lib_dir"] = self.client_dir_var.get().strip()
        self.cfg["dbs"] = [_serialize_db(t) for t in self.dbs]
        self.cfg["last_health"] = self.last_health
        self.cfg["auto_run"] = self.auto_var.get()
        self.cfg["active_filter"] = list(self._active_filter)
        save_config(self.cfg)

    # ---------- Config import/export & email ----------
    def _import_json(self):
        p = filedialog.askopenfilename(title="Import config (.json)", filetypes=[["JSON","*.json"]])
        if not p: return
        try:
            with open(p,"r",encoding="utf-8") as f:
                cfg = json.load(f)
            self.cfg.update(cfg)
            if "email" in cfg:
                self.email_cfg.update(cfg["email"] or {})
                self.cfg["email"] = self.email_cfg
            self.interval_var.set(int(self.cfg.get("interval_sec", DEFAULT_INTERVAL_SEC)))
            self.client_dir_var.set(self.cfg.get("client_lib_dir",""))
            self.auto_var.set(bool(self.cfg.get("auto_run", False)))
            self._active_filter = [tuple(x) for x in self.cfg.get("active_filter", [])]
            self.dbs = [_hydrate_db(d) for d in self.cfg.get("dbs", [])]
            self.last_health = self.cfg.get("last_health", {})
            order = [c for c in self.cfg.get("column_order", list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
            if order and order[0]!="S.No":
                order = ["S.No"] + [c for c in order if c!="S.No"]
            visible = [c for c in self.cfg.get("visible_columns", order) if c in self.LOGICAL_COLUMNS]
            if visible and visible[0]!="S.No":
                visible = ["S.No"] + [c for c in visible if c!="S.No"]
            self.tree["displaycolumns"] = visible or list(self.LOGICAL_COLUMNS)
            if "column_widths" in self.cfg:
                for col,w in self.cfg["column_widths"].items():
                    try: self.tree.column(col, width=int(w))
                    except: pass
            self._refresh_table_from_dbs(); self._load_last_health_into_rows()
            messagebox.showinfo(APP_NAME, "Imported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to import: {e}")

    def _export_json(self):
        p = filedialog.asksaveasfilename(title="Export config", defaultextension=".json", initialfile="oracle_config.json")
        if not p: return
        try:
            export = dict(self.cfg)
            export["interval_sec"] = self.interval_var.get()
            export["client_lib_dir"] = self.client_dir_var.get().strip()
            export["email"] = {
                "server": self.smtp_server_var.get().strip(),
                "port": int(self.smtp_port_var.get() or 25),
                "from_addr": self.from_var.get().strip(),
                "to_addrs": self.to_var.get().strip(),
                "subject": self.email_cfg.get("subject", "Oracle Health Report"),
            }
            export["dbs"] = [_serialize_db(t) for t in self.dbs]
            export["last_health"] = self.last_health
            export["auto_run"] = self.auto_var.get()
            export["column_order"] = list(self.cfg.get("column_order", self.LOGICAL_COLUMNS))
            export["visible_columns"] = list(self.tree["displaycolumns"])
            export["email_columns"] = list(self.cfg.get("email_columns", self.LOGICAL_COLUMNS))
            export["column_widths"] = {c: self.tree.column(c,"width") for c in self.LOGICAL_COLUMNS}
            export["active_filter"] = list(self._active_filter)
            with open(p,"w",encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo(APP_NAME, "Exported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to export: {e}")

    # Email
    def _save_mail_settings(self):
        self.email_cfg["server"] = self.smtp_server_var.get().strip()
        try:
            self.email_cfg["port"] = int(self.smtp_port_var.get() or 25)
        except Exception:
            self.email_cfg["port"] = 25
        self.email_cfg["from_addr"] = self.from_var.get().strip()
        self.email_cfg["to_addrs"] = self.to_var.get().strip()
        self.cfg["email"] = self.email_cfg
        save_config(self.cfg)
        messagebox.showinfo(APP_NAME, "Mail settings saved.")

    def _build_html(self, rows: List[List]) -> str:
        headers = [c for c in self.cfg.get("email_columns", list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
        if not headers:
            headers = list(self.LOGICAL_COLUMNS)

        def ok_style(text: str, col: str) -> str:
            t = str(text).strip()
            ok = None
            if col in ("Status","Inst_status","Sessions","WorstTS%","TS Online"):
                ok = t.startswith(GOOD)
            if ok is True:
                return "background-color:#e6ffe6;color:#064b00;font-weight:bold;"
            if ok is False:
                return "background-color:#ffe6e6;color:#7a0000;font-weight:bold;"
            return ""

        head = "<tr>" + "".join(f"<th style='padding:6px 10px;border-bottom:1px solid #ccc;text-align:left'>{h}</th>" for h in headers) + "</tr>"
        body_rows = []
        for r in rows:
            cells = []
            for col in headers:
                try:
                    idx = self.LOGICAL_COLUMNS.index(col)
                    val = r[idx]
                except Exception:
                    val = ""
                cells.append(f"<td style='padding:4px 8px;border-bottom:1px solid #eee;{ok_style(val,col)}'>{val}</td>")
            body_rows.append("<tr>" + "".join(cells) + "</tr>")
        table = "<table style='border-collapse:collapse;font-family:Segoe UI,Arial,sans-serif;font-size:12px'>" + head + "".join(body_rows) + "</table>"
        title = f"<h3>Oracle Health Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h3>"
        return "<html><body>" + title + table + "</body></html>"

    def _send_html_email(self, server: str, port: int, from_addr: str, to_addrs: List[str], subject: str, html: str):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        part = MIMEText(html, "html", "utf-8"); msg.attach(part)
        import smtplib
        with smtplib.SMTP(server, port, timeout=20) as s:
            s.sendmail(from_addr, to_addrs, msg.as_string())

    def _email_report(self):
        email = self.email_cfg
        server = self.smtp_server_var.get().strip() or email.get("server","")
        port = int(self.smtp_port_var.get() or email.get("port",25))
        from_addr = self.from_var.get().strip() or email.get("from_addr","")
        to_addrs = self.to_var.get().strip() or email.get("to_addrs","")
        subject = email.get("subject", "Oracle Health Report")
        if not (server and from_addr and to_addrs):
            messagebox.showerror(APP_NAME, "Set SMTP server, From, and To first.")
            return
        # IMPORTANT: only visible (filtered) rows
        rows = [self.tree.item(i)["values"] for i in self.tree.get_children("")]
        html = self._build_html(rows)
        try:
            self._send_html_email(server, port, from_addr, [x.strip() for x in to_addrs.split(",") if x.strip()], subject, html)
            messagebox.showinfo(APP_NAME, "Email report sent.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Email failed: {e}")

    # ---------- Filtering ----------
    def _row_passes(self, values: List[Any]) -> bool:
        if not self._active_filter: return True
        colidx = {c:i for i,c in enumerate(self.tree["columns"])}
        def cmp_text(op: str, hay: str, needle: str) -> bool:
            ht = (hay or "").strip(); nd = (needle or "").strip()
            if op=="contains": return nd.lower() in ht.lower()
            if op=="equals":   return ht.lower()==nd.lower()
            # numeric ops try to float
            try:
                nums_h = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", ht.replace("%",""))
                nums_n = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", nd.replace("%",""))
                hv = float(nums_h[0]); nv = float(nums_n[0])
            except Exception:
                return False
            if op==">":  return hv >  nv
            if op==">=": return hv >= nv
            if op=="<":  return hv <  nv
            if op=="<=": return hv <= nv
            if op=="!=": return hv != nv
            return False

        for col,op,val in self._active_filter:
            i = colidx.get(col); 
            if i is None or i>=len(values): return False
            if not cmp_text(op, str(values[i]), val): return False
        return True

    def _apply_filter_now(self):
        # Reattach/show rows that match; detach/hide those that don't
        for iid in list(self.tree.get_children("")):
            vals = self.tree.item(iid)["values"]
            if self._row_passes(vals):
                if iid in self._detached:
                    self.tree.move(iid, "", "end")
                    self._detached.discard(iid)
            else:
                self.tree.detach(iid)
                self._detached.add(iid)
        # Re-attach any previously detached that now match (in case of clearing filters)
        if not self._active_filter:
            for iid in list(self._detached):
                try:
                    self.tree.move(iid, "", "end")
                except Exception:
                    pass
            self._detached.clear()
        self._renumber()
        try: self._autosize_columns()
        except: pass
        # persist
        self.cfg["active_filter"] = list(self._active_filter); save_config(self.cfg)

    def _open_filter_dialog(self):
        dlg = tk.Toplevel(self)
        dlg.title("Filter Rows")
        dlg.resizable(False, False)

        cols = list(self.tree["columns"])
        pad = {"padx":6, "pady":4}
        ttk.Label(dlg, text="Column:").grid(row=0, column=0, sticky="e", **pad)
        col_var = tk.StringVar(value=cols[0] if cols else "")
        ttk.Combobox(dlg, textvariable=col_var, values=cols, state="readonly", width=28).grid(row=0, column=1, columnspan=3, sticky="w", **pad)

        ttk.Label(dlg, text="Operator:").grid(row=1, column=0, sticky="e", **pad)
        ops = ["contains","equals",">",">=","<","<=","!="]
        op_var = tk.StringVar(value="contains")
        ttk.Combobox(dlg, textvariable=op_var, values=ops, state="readonly", width=10).grid(row=1, column=1, sticky="w", **pad)

        ttk.Label(dlg, text="Value:").grid(row=1, column=2, sticky="e", **pad)
        val_var = tk.StringVar()
        ttk.Entry(dlg, textvariable=val_var, width=20).grid(row=1, column=3, sticky="w", **pad)

        ttk.Label(dlg, text="Active conditions (AND):").grid(row=2, column=0, columnspan=4, sticky="w", padx=6, pady=(8,2))
        box = tk.Listbox(dlg, height=8, width=64); box.grid(row=3, column=0, columnspan=4, sticky="we", padx=6)

        # preload existing
        for c,o,v in self._active_filter: box.insert("end", f"{c} {o} {v}")

        btns = ttk.Frame(dlg); btns.grid(row=4, column=0, columnspan=4, sticky="e", padx=6, pady=8)
        def add_cond():
            c=col_var.get().strip(); o=op_var.get().strip(); v=val_var.get()
            if not c or not o: return
            self._active_filter.append((c,o,v)); box.insert("end", f"{c} {o} {v}"); val_var.set("")
        def del_sel():
            sel=list(box.curselection()); sel.reverse()
            for i in sel: box.delete(i); del self._active_filter[i]
        def clear_all():
            box.delete(0,"end"); self._active_filter.clear()
        def apply_now():
            self._apply_filter_now(); dlg.destroy()
        ttk.Button(btns, text="Add", command=add_cond).pack(side=tk.LEFT, padx=(0,4))
        ttk.Button(btns, text="Delete", command=del_sel).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Clear All", command=clear_all).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Cancel", command=dlg.destroy).pack(side=tk.RIGHT, padx=(6,0))
        ttk.Button(btns, text="Apply", command=apply_now).pack(side=tk.RIGHT)

        dlg.transient(self); dlg.grab_set(); dlg.wait_window()

    def _clear_filter(self):
        self._active_filter.clear()
        self._apply_filter_now()

    # ---------- Run checks ----------
    def run_all_once(self):
        self._checks_async(self.dbs)

    def run_selected_once(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a DB row to run."); return
        name = sel[0]
        t = next((x for x in self.dbs if x.name==name), None)
        if not t:
            messagebox.showerror(APP_NAME, "Selected DB not found."); return
        self._checks_async([t])

    def _set_check_status(self, name: str, status: str):
        if name in self.tree.get_children("") or name in self._detached:
            # ensure we can set values even if detached
            if name in self._detached:
                self.tree.move(name, "", "end")
                self._detached.discard(name)
            vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
            idx = self.LOGICAL_COLUMNS.index("Check Status")
            if len(vals) <= idx:
                vals += [""] * (idx + 1 - len(vals))
            vals[idx] = status
            self.tree.item(name, values=vals)
            # After set, re-apply filter to possibly hide again
            if self._active_filter: self._apply_filter_now()

    def _checks_async(self, items: List[DbTarget]):
        for t in items:
            self._set_check_status(t.name, "In Progress")

        def job(target: DbTarget):
            h = self._check_one(target)
            self.after(0, lambda n=target.name, T=target, H=h: self._apply_result(n, T, H))

        for t in items:
            threading.Thread(target=job, args=(t,), daemon=True).start()

    def _check_one(self, t: DbTarget) -> DbHealth:
        t0 = time.time()
        h = DbHealth()
        try:
            import oracledb
            if t.mode == "TNS":
                conn = _oracle_connect_tns(t.user, _decrypt_password(t.password_enc) or "", t.tns_alias, self.client_lib_dir or t.client_lib_dir)
                host_disp = t.tns_alias
            else:
                conn = _oracle_connect_thin(t.user, _decrypt_password(t.password_enc) or "", t.host, int(t.port), t.use_service_name, t.service_or_sid)
                host_disp = t.host
            try:
                with conn.cursor() as cur:
                    # Version
                    ver = "-"
                    for sql, colname in Q_VERSION:
                        try:
                            cur.execute(sql)
                            row = cur.fetchone()
                            if row and row[0]:
                                ver = str(row[0]); break
                        except Exception:
                            continue
                    h.version = ver
                    # Startup
                    try:
                        cur.execute(Q_STARTUP); r = cur.fetchone()
                        if r and r[0]: h.startup_time = r[0]
                    except Exception: pass
                    # Status & Inst_status
                    try:
                        cur.execute(Q_STATUS); r = cur.fetchone()
                        if r and len(r)>=2:
                            h.status = str(r[0]); h.inst_status = str(r[1])
                    except Exception: pass
                    # Sessions
                    try:
                        cur.execute(Q_SESSIONS); r = cur.fetchone()
                        if r and len(r)>=2:
                            cur_s, lim_s = int(r[0] or 0), int(r[1] or 0)
                            h.sessions = f"{cur_s}/{lim_s}"
                            h.cur_sess, h.lim_sess = cur_s, lim_s
                    except Exception: pass
                    # Worst TS %
                    worst = None
                    try:
                        cur.execute(Q_WORST_TS_METRICS); r = cur.fetchone()
                        if r and r[0] is not None:
                            worst = float(r[0])
                    except Exception:
                        try:
                            cur.execute(Q_WORST_TS_FALLBACK); r = cur.fetchone()
                            if r and r[0] is not None:
                                worst = float(r[0])
                        except Exception: pass
                    h.worst_ts_pct = worst
                    # TS online
                    try:
                        cur.execute(Q_TS_ONLINE); r = cur.fetchone()
                        if r and len(r)>=2:
                            total = int(r[0] or 0); online = int(r[1] or 0)
                            h.ts_online = (online, total)
                    except Exception: pass
                    # DB size GB
                    try:
                        cur.execute(Q_DB_SIZE_GB); r = cur.fetchone()
                        if r and r[0] is not None:
                            h.db_size_gb = float(r[0])
                    except Exception: pass
                    # Backups
                    try:
                        cur.execute(Q_LAST_FULL_INC); r = cur.fetchone()
                        if r and r[0]: h.last_full_inc = str(r[0])
                    except Exception: pass
                    try:
                        cur.execute(Q_LAST_ARCH); r = cur.fetchone()
                        if r and r[0]: h.last_arch = str(r[0])
                    except Exception: pass
            finally:
                try: conn.close()
                except Exception: pass
            h.elapsed_ms = int((time.time() - t0) * 1000)
            h.ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # host shown in table
            t.last_host_display = host_disp
            return h
        except Exception as e:
            h.error = str(e)
            h.elapsed_ms = int((time.time() - t0) * 1000)
            h.ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return h

    def _apply_result(self, name: str, t: DbTarget, h: DbHealth):
        def mark(ok: bool)->str: return GOOD if ok else BAD
        vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
        colidx = {c:i for i,c in enumerate(self.LOGICAL_COLUMNS)}

        # Host display (tns alias or host)
        vals[colidx["Host"]] = t.last_host_display or vals[colidx["Host"]]

        # version
        vals[colidx["DB Version"]] = h.version or "-"
        # startup
        vals[colidx["Startup time"]] = h.startup_time or "-"
        # status
        vals[colidx["Status"]] = f"{mark(str(h.status).upper()=='OPEN')} {h.status or '-'}"
        vals[colidx["Inst_status"]] = f"{mark(str(h.inst_status).upper().startswith('OPEN'))} {h.inst_status or '-'}"
        # sessions (95% rule)
        try:
            cur = int(getattr(h,'cur_sess',0))
            lim = int(getattr(h,'lim_sess',0) or 1)
            ok_sess = (cur/lim) < 0.95 if lim>0 else False
            vals[colidx["Sessions"]] = f"{GOOD if ok_sess else BAD} {cur}/{lim}"
        except Exception:
            vals[colidx["Sessions"]] = f"{BAD} -"
        # worst ts
        if h.worst_ts_pct is not None and h.worst_ts_pct >= 0:
            vals[colidx["WorstTS%"]] = f"{GOOD if h.worst_ts_pct < 90.0 else BAD} {h.worst_ts_pct:.1f}%"
        else:
            vals[colidx["WorstTS%"]] = f"{BAD} -"
        # ts online
        online,total = h.ts_online
        ts_ok = (total>0 and online==total)
        vals[colidx["TS Online"]] = f"{GOOD if ts_ok else BAD} {online}/{total}"
        # size
        vals[colidx["DB Size"]] = (f"{h.db_size_gb:.2f} GB" if h.db_size_gb is not None else "-")
        # backups
        vals[colidx["LastFull/inc"]] = h.last_full_inc or "-"
        vals[colidx["LastArc"]] = h.last_arch or "-"
        # times
        vals[colidx["MS"]] = str(h.elapsed_ms or 0)
        vals[colidx["LastChecked"]] = h.ts
        vals[colidx["Check Status"]] = "Complete"
        vals[colidx["Error"]] = h.error or ""

        self.tree.item(name, values=vals)

        # Re-apply filter (keeps filtered view up to date)
        if self._active_filter:
            self._apply_filter_now()

        # persist health
        self.last_health[name] = {
            "version": h.version, "startup_time": h.startup_time,
            "status": h.status, "inst_status": h.inst_status,
            "cur_sess": getattr(h,'cur_sess',0), "lim_sess": getattr(h,'lim_sess',0),
            "worst_ts_pct": h.worst_ts_pct,
            "ts_online": list(h.ts_online),
            "db_size_gb": h.db_size_gb,
            "last_full_inc": h.last_full_inc or "-",
            "last_arch": h.last_arch or "-",
            "elapsed_ms": h.elapsed_ms, "ts": h.ts, "error": h.error or ""
        }
        self._persist_all()
        self._renumber()
        try: self._autosize_columns()
        except: pass

    def _clear_all_rows(self):
        for iid in list(self.tree.get_children("")) + list(self._detached):
            if iid in self._detached:
                self.tree.move(iid, "", "end")
                self._detached.discard(iid)
            vals = list(self.tree.item(iid)["values"])
            cleared = list(vals)
            for c in self.STATUS_COLUMNS:
                idx = self.LOGICAL_COLUMNS.index(c); cleared[idx] = "-"
            self.tree.item(iid, values=cleared)
        self.status_var.set("Cleared all rows (except S.No, DB_name, Environment, Host).")
        if self._active_filter: self._apply_filter_now()

    def _clear_selected_row(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row to clear."); return
        iid = sel[0]
        if iid in self._detached:
            self.tree.move(iid, "", "end")
            self._detached.discard(iid)
        vals = list(self.tree.item(iid)["values"])
        for c in self.STATUS_COLUMNS:
            vals[self.LOGICAL_COLUMNS.index(c)] = "-"
        self.tree.item(iid, values=vals)
        self.status_var.set(f"Cleared row: {iid}")
        if self._active_filter: self._apply_filter_now()

    # ---------- CRUD dialogs ----------
    def _add_dialog(self):
        DbEditor(self, on_save=self._add_db)

    def _edit_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a DB to edit."); return
        name = sel[0]
        t = next((x for x in self.dbs if x.name==name), None)
        if not t:
            messagebox.showerror(APP_NAME, "DB not found."); return
        DbEditor(self, target=t, on_save=self._update_db)

    def _remove_selected(self):
        sel = self.tree.selection()
        if not sel: return
        name = sel[0]
        self.dbs = [d for d in self.dbs if d.name!=name]
        try:
            self.tree.delete(name)
        except Exception:
            pass
        self._persist_all()
        self._renumber()

    def _add_db(self, t: DbTarget):
        if any(x.name==t.name for x in self.dbs):
            messagebox.showerror(APP_NAME, "A DB with this name already exists."); return
        self.dbs.append(t)
        self._persist_all()
        host_disp = t.tns_alias if t.mode=="TNS" else (t.host or "-")
        values = ["-"] * len(self.LOGICAL_COLUMNS)
        values[0] = len(self.dbs); values[1] = t.name; values[2] = t.environment; values[3] = host_disp
        self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber(); self._autosize_columns()
        if self._active_filter: self._apply_filter_now()

    def _update_db(self, t: DbTarget):
        found = False
        for i,cur in enumerate(self.dbs):
            if cur.name==t.name:
                self.dbs[i] = t; found=True; break
        if not found:
            self.dbs.append(t)
        self._persist_all()
        if t.name in self.tree.get_children("") or t.name in self._detached:
            if t.name in self._detached:
                self.tree.move(t.name, "", "end")
                self._detached.discard(t.name)
            vals = list(self.tree.item(t.name)["values"])
            vals[1] = t.name; vals[2] = t.environment; vals[3] = (t.tns_alias if t.mode=="TNS" else (t.host or "-"))
            self.tree.item(t.name, values=vals)
            if self._active_filter: self._apply_filter_now()
        self._autosize_columns()


# ---------- Editor ----------
class DbEditor(tk.Toplevel):
    def __init__(self, app: OracleMonitorApp, target: Optional[DbTarget]=None, on_save=None):
        super().__init__(app)
        self.app = app; self.on_save=on_save
        self.title("Add / Edit Oracle DB")
        self.resizable(False, False)

        self.name_var = tk.StringVar(value=target.name if target else "")
        self.env_var = tk.StringVar(value=target.environment if target else "NON-PROD")
        self.mode_var = tk.StringVar(value=target.mode if target else "TNS")
        self.user_var = tk.StringVar(value=target.user if target else "")
        self.pass_var = tk.StringVar(value=_decrypt_password(target.password_enc) if target and target.password_enc else "")
        self.tns_var = tk.StringVar(value=target.tns_alias if target else "")
        self.host_var = tk.StringVar(value=target.host if target else "")
        self.port_var = tk.IntVar(value=int(target.port if target else 1521))
        self.use_service_var = tk.BooleanVar(value=bool(target.use_service_name if target else True))
        self.svc_sid_var = tk.StringVar(value=target.service_or_sid if target else "")

        body = ttk.Frame(self, padding=10); body.pack(fill=tk.BOTH, expand=True)

        r=0
        ttk.Label(body, text="DB Name (display):").grid(row=r,column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(body, textvariable=self.name_var, width=30).grid(row=r,column=1, columnspan=3, sticky="w", padx=4, pady=4); r+=1

        ttk.Label(body, text="Environment:").grid(row=r,column=0, sticky="e", padx=4, pady=4)
        ttk.Combobox(body, textvariable=self.env_var, values=["NON-PROD","PROD"], state="readonly", width=14)\
            .grid(row=r,column=1, sticky="w", padx=4, pady=4); r+=1

        ttk.Label(body, text="Connection Type:").grid(row=r,column=0, sticky="e", padx=4, pady=4)
        ttk.Radiobutton(body, text="TNS Alias / Descriptor (Oracle Client)", variable=self.mode_var, value="TNS", command=self._refresh).grid(row=r,column=1, sticky="w")
        ttk.Radiobutton(body, text="Thin (Host/Port, no Client)", variable=self.mode_var, value="THIN", command=self._refresh).grid(row=r,column=2, sticky="w"); r+=1

        # TNS box
        self.tns_box = ttk.LabelFrame(body, text="TNS")
        self.tns_box.grid(row=r, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(self.tns_box, text="TNS Alias/Descriptor:").grid(row=0,column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.tns_box, textvariable=self.tns_var, width=34).grid(row=0,column=1, columnspan=3, sticky="w", padx=4, pady=4)

        # THIN box
        self.thin_box = ttk.LabelFrame(body, text="Thin (no client)")
        self.thin_box.grid(row=r+1, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(self.thin_box, text="Host:").grid(row=0,column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_box, textvariable=self.host_var, width=22).grid(row=0,column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.thin_box, text="Port:").grid(row=0,column=2, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_box, textvariable=self.port_var, width=8).grid(row=0,column=3, sticky="w", padx=4, pady=4)

        ttk.Label(self.thin_box, text="Connect Using:").grid(row=1,column=0, sticky="e", padx=4, pady=4)
        ttk.Combobox(self.thin_box, values=["Service Name","SID"], textvariable=self.use_service_var, state="readonly", width=14)\
            .grid(row=1,column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.thin_box, text="Service/SID:").grid(row=1,column=2, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_box, textvariable=self.svc_sid_var, width=18).grid(row=1,column=3, sticky="w", padx=4, pady=4)

        # Common auth
        r += 2
        auth_box = ttk.LabelFrame(body, text="Credentials")
        auth_box.grid(row=r, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(auth_box, text="User:").grid(row=0,column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(auth_box, textvariable=self.user_var, width=24).grid(row=0,column=1, sticky="w", padx=4, pady=4)
        ttk.Label(auth_box, text="Password:").grid(row=0,column=2, sticky="e", padx=4, pady=4)
        ttk.Entry(auth_box, textvariable=self.pass_var, width=18, show="*").grid(row=0,column=3, sticky="w", padx=4, pady=4)

        btns = ttk.Frame(self, padding=(10,6)); btns.pack(fill=tk.X)
        ttk.Button(btns, text="Test Connection", command=self._test_connection).pack(side=tk.LEFT)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side=tk.RIGHT, padx=(6,0))
        ttk.Button(btns, text="Save", command=self._save).pack(side=tk.RIGHT)

        self._refresh()
        self.grab_set(); self.transient(app)

    def _refresh(self):
        tns_mode = (self.mode_var.get()=="TNS")
        for ch in self.tns_box.winfo_children():
            ch.configure(state=("normal" if tns_mode else "disabled"))
        for ch in self.thin_box.winfo_children():
            ch.configure(state=("disabled" if tns_mode else "normal"))

    def _make_target(self) -> DbTarget:
        mode = self.mode_var.get()
        if mode=="TNS":
            return DbTarget(
                name=self.name_var.get().strip(), environment=self.env_var.get().strip() or "NON-PROD",
                mode="TNS", user=self.user_var.get().strip(),
                password_enc=_encrypt_password(self.pass_var.get() or ""),
                tns_alias=self.tns_var.get().strip(),
            )
        else:
            return DbTarget(
                name=self.name_var.get().strip(), environment=self.env_var.get().strip() or "NON-PROD",
                mode="THIN", user=self.user_var.get().strip(),
                password_enc=_encrypt_password(self.pass_var.get() or ""),
                host=self.host_var.get().strip(), port=int(self.port_var.get() or 1521),
                use_service_name=bool(self.use_service_var.get() in (True,"Service Name")),
                service_or_sid=self.svc_sid_var.get().strip(),
            )

    def _test_connection(self):
        try:
            t = self._make_target()
            if not t.name or not t.user:
                messagebox.showerror(APP_NAME, "Name and User required.")
                return
            import oracledb
            if t.mode=="TNS":
                conn = _oracle_connect_tns(t.user, _decrypt_password(t.password_enc) or "", t.tns_alias, self.app.client_dir_var.get().strip() or None)
            else:
                conn = _oracle_connect_thin(t.user, _decrypt_password(t.password_enc) or "", t.host, int(t.port), t.use_service_name, t.service_or_sid)
            try:
                with conn.cursor() as cur:
                    cur.execute("select 1 from dual")
                    _ = cur.fetchone()
                messagebox.showinfo(APP_NAME, f"Connection OK: {t.name}")
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Connection failed:\n{e}")

    def _save(self):
        try:
            t = self._make_target()
            if not t.name: messagebox.showerror(APP_NAME, "DB Name is required."); return
            if t.mode=="TNS" and not t.tns_alias:
                messagebox.showerror(APP_NAME, "Enter TNS Alias/Descriptor."); return
            if t.mode=="THIN" and (not t.host or not t.service_or_sid):
                messagebox.showerror(APP_NAME, "Enter Host and Service/SID."); return
            if self.on_save: self.on_save(t)
            self.destroy()
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Save failed: {e}")


# Keep compatibility alias if launcher expects OraclePlaceholder
class OraclePlaceholder(OracleMonitorApp):
    pass
