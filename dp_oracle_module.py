#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oracle module for Database Pulse.
Contains: config helpers, connection, checks, and MonitorApp + DbEditor.
"""
import base64
import json
import os
import smtplib
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont

# Optional DB driver
try:
    import oracledb  # pip install python-oracledb
except Exception:
    oracledb = None

APP_NAME = "Database Pulse"
APP_VERSION = "Database Pulse v1.0"

# -------- Paths / Config --------
def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

CONFIG_DIR = (_base_dir() / "config")
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "oracle_config.json"

ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
DEFAULT_INTERVAL_SEC = 300  # 5 minutes
GOOD = "✅"
BAD = "❌"

# -------- Password helpers (DPAPI on Windows; base64 elsewhere) --------
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

# -------- DSN helpers (thin/ezconnect) --------
def normalize_tns(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("@"):
        t = t[1:]
    return t

def ezconnect_service(host: str, port: str, service: str) -> str:
    host = (host or "").strip()
    port = (port or "1521").strip()
    service = (service or "").strip()
    return f"{host}:{port}/{service}"  # storage/display only

def ezconnect_sid(host: str, port: str, sid: str) -> str:
    host = (host or "").strip()
    port = (port or "1521").strip()
    sid = (sid or "").strip()
    return f"{host}:{port}/?sid={sid}"  # storage/display only

def parse_ezconnect(dsn: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    s = (dsn or "").strip()
    if s.startswith("//"):
        s = s[2:]
    host_port, _, rest = s.partition("/")
    host, _, port = host_port.partition(":")
    if rest.startswith("?sid="):
        return host, (port or "1521"), None, rest.split("=", 1)[1]
    return host, (port or "1521"), rest or None, None

# -------- Data classes --------
@dataclass
class DbTarget:
    name: str
    dsn: str
    user: Optional[str] = None
    password_enc: Optional[str] = None
    mode: str = "thin"  # "thin" (ezconnect) or "thick" (TNS via client)
    environment: str = "NON-PROD"

@dataclass
class DbHealth:
    status: str
    details: str
    version: str = ""
    inst_status: str = ""
    sessions_curr: int = 0
    sessions_limit: int = 0
    worst_ts_pct_used: Optional[float] = None
    host: str = ""
    elapsed_ms: int = 0
    last_full_inc_backup: Optional[datetime] = None
    last_arch_backup: Optional[datetime] = None
    startup_time: Optional[datetime] = None
    ts_online: Optional[int] = None
    ts_total: Optional[int] = None
    db_size_gb: Optional[float] = None
    error: str = ""
    ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# -------- Columns / Config --------
def logical_columns() -> List[str]:
    return [
        "S.No",
        "DB Name",
        "Environment",
        "Host",
        "DB Version",
        "Startup Time",
        "Status",
        "Inst_status",
        "Sessions",
        "WorstTS%",
        "TS Online",
        "DB Size",
        "LastFull/Inc",
        "LastArch",
        "Ms",
        "LastChecked",
        "Check status",
        "Error",
    ]

def default_config() -> Dict[str, Any]:
    cols = logical_columns()
    return {
        "interval_sec": 300,
        "targets": [],
        "client_lib_dir": os.environ.get("ORACLE_CLIENT_LIB_DIR", ""),
        "email": {
            "server": "",
            "port": 25,
            "from_addr": "",
            "to_addrs": "",
            "subject": "Oracle DB Health Report",
        },
        "last_health": {},
        "auto_run": False,
        "column_order": cols[:],
        "visible_columns": cols[:],
        "email_columns": cols[:],
        "column_widths": {},
    }

def _hydrate_target(d: Dict[str, Any]) -> DbTarget:
    return DbTarget(
        name=d.get("name", ""),
        dsn=(d.get("dsn", "") or "").strip(),
        user=d.get("user"),
        password_enc=d.get("password_enc"),
        mode=d.get("mode", "thin"),
        environment=d.get("environment", "NON-PROD"),
    )

def _serialize_target(t: DbTarget) -> Dict[str, Any]:
    return {
        "name": t.name,
        "dsn": t.dsn.strip(),
        "user": t.user,
        "password_enc": t.password_enc,
        "mode": t.mode,
        "environment": t.environment,
    }

def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            base = default_config()
            for k, v in cfg.items():
                if k == "email":
                    base["email"].update(v or {})
                else:
                    base[k] = v
            base.setdefault("last_health", {})
            base.setdefault("auto_run", False)
            base.setdefault("column_order", default_config()["column_order"])
            base.setdefault("visible_columns", default_config()["visible_columns"])
            base.setdefault("email_columns", default_config()["email_columns"])
            base.setdefault("column_widths", {})
            if base.get("targets"):
                base["targets"] = [_serialize_target(_hydrate_target(t)) for t in base["targets"]]
            return base
        except Exception:
            pass
    return default_config()

def save_config(cfg: Dict[str, Any]):
    out = dict(cfg)
    out["targets"] = [
        _serialize_target(_hydrate_target(t) if isinstance(t, dict) else t) for t in cfg.get("targets", [])
    ]
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)

# -------- DB Connection --------
def init_oracle_client_if_needed(cfg: Dict[str, Any]):
    if oracledb is None:
        return
    lib_dir = cfg.get("client_lib_dir") or os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
    if lib_dir:
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        except Exception:
            pass

def connect_target(target: DbTarget, cfg: Dict[str, Any]):
    if oracledb is None:
        raise RuntimeError("python-oracledb not installed. pip install python-oracledb")
    mode = (target.mode or "thin").lower()
    dsn = (target.dsn or "").strip()
    user = (target.user or "").strip() or None
    pwd = _decrypt_password(target.password_enc) if target.password_enc else None
    if mode == "thick":
        init_oracle_client_if_needed(cfg)
        tns = normalize_tns(dsn)
        if user and pwd:
            return oracledb.connect(user=user, password=pwd, dsn=tns)
        return oracledb.connect(dsn=tns)
    else:
        host, port, service, sid = parse_ezconnect(dsn)
        if not host:
            raise ValueError("Invalid JDBC thin DSN. Please set Host, Port and Service/SID.")
        kwargs = {"host": host, "port": int(port or 1521)}
        if service:
            kwargs["service_name"] = service
        elif sid:
            kwargs["sid"] = sid
        if user:
            kwargs["user"] = user
        if pwd:
            kwargs["password"] = pwd
        return oracledb.connect(**kwargs)

# -------- SQL --------
SQLS = {
    "db": "SELECT name, open_mode, database_role, log_mode FROM v$database",
    "inst": "SELECT instance_name, status, host_name, version, startup_time FROM v$instance",
    "sess_curr": "SELECT COUNT(*) FROM v$session",
    "sess_limit": "SELECT value FROM v$parameter WHERE name='sessions'",
    "tspace": (
        "SELECT ts.tablespace_name, ROUND((1 - NVL(fs.free_mb,0)/ts.size_mb)*100,2) pct_used "
        "FROM (SELECT tablespace_name, SUM(bytes)/1024/1024 size_mb FROM dba_data_files GROUP BY tablespace_name) ts "
        "LEFT JOIN (SELECT tablespace_name, SUM(bytes)/1024/1024 free_mb FROM dba_free_space GROUP BY tablespace_name) fs "
        "ON ts.tablespace_name=fs.tablespace_name"
    ),
    "ts_online": "SELECT COUNT(*) total, SUM(CASE WHEN UPPER(status)='ONLINE' THEN 1 ELSE 0 END) tonline FROM dba_tablespaces",
    "db_size": "SELECT ROUND(SUM(bytes)/1024/1024/1024,1) FROM dba_data_files",
    "bk_data": (
        "SELECT MAX(bp.completion_time) "
        "FROM v$backup_set bs JOIN v$backup_piece bp "
        "ON bs.set_stamp=bp.set_stamp AND bs.set_count=bp.set_count "
        "WHERE bs.backup_type='D'"
    ),
    "bk_arch": (
        "SELECT MAX(bp.completion_time) "
        "FROM v$backup_set bs JOIN v$backup_piece bp "
        "ON bs.set_stamp=bp.set_stamp AND bs.set_count=bp.set_count "
        "WHERE bs.backup_type='L'"
    ),
}

def _dt_str(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "-"

# -------- Health check --------
@dataclass
class _RowResult:
    status_cell: str = "-"
    inst_cell: str = "-"
    sessions_cell: str = "-"
    worst_cell: str = "-"
    last_full_cell: str = "-"
    last_arch_cell: str = "-"
    version: str = "-"
    startup_str: str = "-"
    ts_cell: str = "-"
    db_size_cell: str = "-"
    ms: int = 0
    last_checked: str = "-"
    check_status: str = "-"
    error: str = ""

def check_one(target: DbTarget, cfg: Dict[str, Any], timeout_sec: int = 25) -> "DbHealth":
    t0 = time.time()
    try:
        with connect_target(target, cfg) as conn:
            conn.call_timeout = timeout_sec * 1000
            cur = conn.cursor()
            details = ""
            try:
                cur.execute(SQLS["db"])
                _name, _open_mode, _role, log_mode = cur.fetchone()
                details = f"Log:{log_mode}"
            except Exception:
                pass
            cur.execute(SQLS["inst"])
            _inst_name, inst_status, host_name, inst_version, startup_time = cur.fetchone()
            sessions_curr = 0
            sessions_limit = 0
            try:
                cur.execute(SQLS["sess_curr"])
                sessions_curr = int(cur.fetchone()[0])
            except Exception:
                pass
            try:
                cur.execute(SQLS["sess_limit"])
                sessions_limit = int(cur.fetchone()[0])
            except Exception:
                pass
            worst_pct = None
            try:
                cur.execute(SQLS["tspace"])
                worst_pct = 0.0
                for _ts_name, pct_used in cur.fetchall():
                    if pct_used is not None and pct_used > (worst_pct or 0):
                        worst_pct = float(pct_used)
            except Exception:
                worst_pct = None
            ts_total = None
            ts_online = None
            try:
                cur.execute(SQLS["ts_online"])
                total, tonline = cur.fetchone()
                ts_total = int(total or 0)
                ts_online = int(tonline or 0)
            except Exception:
                pass
            db_size_gb = None
            try:
                cur.execute(SQLS["db_size"])
                row = cur.fetchone()
                db_size_gb = float(row[0]) if row and row[0] is not None else None
            except Exception:
                pass
            last_df = None
            last_arch = None
            try:
                cur.execute(SQLS["bk_data"])
                r = cur.fetchone()
                last_df = r[0] if r else None
            except Exception:
                pass
            try:
                cur.execute(SQLS["bk_arch"])
                r = cur.fetchone()
                last_arch = r[0] if r else None
            except Exception:
                pass
            elapsed_ms = int((time.time() - t0) * 1000)
            return DbHealth(
                status="UP",
                details=details,
                version=inst_version,
                inst_status=inst_status,
                sessions_curr=sessions_curr,
                sessions_limit=sessions_limit,
                worst_ts_pct_used=worst_pct,
                host=host_name,
                elapsed_ms=elapsed_ms,
                last_full_inc_backup=last_df,
                last_arch_backup=last_arch,
                startup_time=startup_time,
                ts_online=ts_online,
                ts_total=ts_total,
                db_size_gb=db_size_gb,
                error="",
            )
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        return DbHealth(status="DOWN", details=str(e), elapsed_ms=elapsed_ms, error=str(e))

# -------- Oracle Monitor UI --------
class MonitorApp(ttk.Frame):
    LOGICAL_COLUMNS = tuple(logical_columns())
    STATUS_COLUMNS = {
        "Host",
        "DB Version",
        "Startup Time",
        "Status",
        "Inst_status",
        "Sessions",
        "WorstTS%",
        "TS Online",
        "DB Size",
        "LastFull/Inc",
        "LastArch",
        "Ms",
        "LastChecked",
        "Check status",
        "Error",
    }

    def __init__(self, master, cfg: Dict[str, Any]):
        super().__init__(master)
        self.cfg = cfg
        self.interval_sec = int(cfg.get("interval_sec", 300))
        self.targets: List[DbTarget] = [_hydrate_target(t) if isinstance(t, dict) else t for t in cfg.get("targets", [])]
        self.last_health: Dict[str, Dict[str, Any]] = cfg.get("last_health", {})
        self._auto_flag = False
        self._build_ui()
        self._refresh_table_from_targets()
        self._load_last_health_into_rows()
        if cfg.get("auto_run"):
            self.auto_var.set(True)
            self._start_auto()

    def _build_ui(self):
        # Allow MonitorApp to be gridded by its parent container (router)
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
        style.configure(
            "App.Treeview.Heading",
            background="#cfe8ff",
            foreground="#000000",
            font=(self._font.actual("family"), self._font.actual("size"), "bold"),
        )
        style.map("App.Treeview.Heading", background=[("active", "#b7dbff")])
        style.configure("App.Treeview", rowheight=22)

        # Toolbar
        t1 = ttk.Frame(self)
        t1.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 3))
        self.interval_var = tk.IntVar(value=self.interval_sec)
        ttk.Label(t1, text="Interval (sec):").pack(side=tk.LEFT)
        ttk.Spinbox(t1, from_=30, to=3600, textvariable=self.interval_var, width=8).pack(side=tk.LEFT, padx=(4, 10))
        self.auto_var = tk.BooleanVar(value=self.cfg.get("auto_run", False))
        ttk.Checkbutton(t1, text="Auto-run", variable=self.auto_var, command=self._toggle_auto).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(t1, text="Run All", command=self.run_all_once).pack(side=tk.LEFT)
        ttk.Button(t1, text="Run Selected", command=self.run_selected_once).pack(side=tk.LEFT, padx=(6, 10))
        ttk.Button(t1, text="Clear All", command=self._clear_all_rows).pack(side=tk.LEFT, padx=(6, 4))
        ttk.Button(t1, text="Clear Selected", command=self._clear_selected_row).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Button(t1, text="Add DB", command=self._add_dialog).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(t1, text="Edit DB", command=self._edit_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Remove DB", command=self._remove_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Import Config", command=self._import_json).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(t1, text="Export Config", command=self._export_json).pack(side=tk.LEFT)

        # Email bar
        t2 = ttk.Frame(self)
        t2.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        email_cfg = self.cfg.get("email", {})
        ttk.Label(t2, text="SMTP/Exchange:").pack(side=tk.LEFT)
        self.smtp_server_var = tk.StringVar(value=email_cfg.get("server", ""))
        self.smtp_port_var = tk.IntVar(value=int(email_cfg.get("port", 25)))
        ttk.Entry(t2, textvariable=self.smtp_server_var, width=22).pack(side=tk.LEFT, padx=(4, 2))
        ttk.Entry(t2, textvariable=self.smtp_port_var, width=6).pack(side=tk.LEFT, padx=(2, 6))
        ttk.Label(t2, text="From:").pack(side=tk.LEFT)
        self.from_var = tk.StringVar(value=email_cfg.get("from_addr", ""))
        ttk.Entry(t2, textvariable=self.from_var, width=24).pack(side=tk.LEFT, padx=(4, 6))
        ttk.Label(t2, text="To (comma):").pack(side=tk.LEFT)
        self.to_var = tk.StringVar(value=email_cfg.get("to_addrs", ""))
        ttk.Entry(t2, textvariable=self.to_var, width=32).pack(side=tk.LEFT, padx=(4, 6))
        ttk.Button(t2, text="Save Mail", command=self._save_mail_settings).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(t2, text="Email Columns", command=self._select_email_columns_dialog).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(t2, text="Email Report", command=self._email_report).pack(side=tk.LEFT, padx=(6, 0))

        # Table + scrollbars
        tree_frame = ttk.Frame(self)
        tree_frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=8)
        self.tree = ttk.Treeview(tree_frame, columns=self.LOGICAL_COLUMNS, show="headings", height=20, style="App.Treeview")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=xsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)

        for col in self.LOGICAL_COLUMNS:
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c, False))
            self.tree.column(col, width=120, stretch=True, anchor="w")

        # Visible columns and order
        order = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
        order = [c for c in order if c in self.LOGICAL_COLUMNS]
        for c in self.LOGICAL_COLUMNS:
            if c not in order:
                order.append(c)
        vis = self.cfg.get("visible_columns", order[:])
        vis = [c for c in vis if c in self.LOGICAL_COLUMNS]
        if "S.No" not in vis:
            vis = ["S.No"] + [c for c in vis if c != "S.No"]
        if order and order[0] != "S.No":
            order = ["S.No"] + [c for c in order if c != "S.No"]
        display = [c for c in order if c in vis]
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
        self.menu.add_command(label="Copy DB Name", command=lambda: self._copy_by_col("DB Name"))
        self.menu.add_command(label="Copy Host", command=lambda: self._copy_by_col("Host"))
        self.menu.add_command(label="Copy Error", command=lambda: self._copy_by_col("Error"))
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<ButtonRelease-1>", lambda e: self._persist_column_layout())

        # Bottom status
        bottombar = ttk.Frame(self)
        bottombar.grid(row=3, column=0, sticky="ew", padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottombar, textvariable=self.status_var).pack(side=tk.LEFT)

        # Column customization
        t3 = ttk.Frame(self)
        t3.grid(row=4, column=0, sticky="ew", padx=8, pady=(0, 6))
        ttk.Button(t3, text="Customize Columns", command=self._customize_columns).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(t3, text="Select Columns", command=self._select_columns_dialog).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Label(t3, text="Client lib dir (for TNS):").pack(side=tk.LEFT, padx=(16, 4))
        self.client_dir_var = tk.StringVar(value=self.cfg.get("client_lib_dir", ""))
        ttk.Entry(t3, textvariable=self.client_dir_var, width=28).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(t3, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT)

    # ---- UI Handlers ----
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

    def _persist_column_layout(self):
        widths = {col: self.tree.column(col, option="width") for col in self.LOGICAL_COLUMNS}
        self.cfg["column_widths"] = widths
        visible = list(self.tree["displaycolumns"])
        full = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
        new_full, seen = [], set()
        for c in visible:
            if c not in seen:
                new_full.append(c)
                seen.add(c)
        for c in full:
            if c not in seen and c in self.LOGICAL_COLUMNS:
                new_full.append(c)
                seen.add(c)
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

    def _customize_columns(self):
        dlg = tk.Toplevel(self)
        dlg.title("Customize Columns (Order)")
        dlg.geometry("380x420")
        ttk.Label(dlg, text="Reorder columns (S.No is fixed at first):").pack(pady=6)
        current = [c for c in self.tree["displaycolumns"] if c != "S.No"]
        var_list = tk.Variable(value=current)
        lb = tk.Listbox(dlg, listvariable=var_list, selectmode=tk.SINGLE, height=16)
        lb.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        btns = ttk.Frame(dlg)
        btns.pack(fill=tk.X, padx=10, pady=6)

        def move(offset: int):
            sel = lb.curselection()
            if not sel:
                return
            i = sel[0]
            j = i + offset
            if j < 0 or j >= lb.size():
                return
            items = list(lb.get(0, tk.END))
            items[i], items[j] = items[j], items[i]
            var_list.set(items)
            lb.selection_clear(0, tk.END)
            lb.selection_set(j)
            lb.activate(j)

        ttk.Button(btns, text="Up", command=lambda: move(-1)).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Down", command=lambda: move(1)).pack(side=tk.LEFT, padx=4)

        def apply_and_close():
            items = list(lb.get(0, tk.END))
            order = ["S.No"] + items
            self.tree["displaycolumns"] = order
            self.cfg["visible_columns"] = order
            self._persist_column_layout()
            dlg.destroy()

        ttk.Button(btns, text="Apply", command=apply_and_close).pack(side=tk.RIGHT, padx=4)
        ttk.Button(btns, text="Cancel", command=dlg.destroy).pack(side=tk.RIGHT, padx=4)

    def _select_columns_dialog(self):
        dlg = tk.Toplevel(self)
        dlg.title("Select Visible Columns")
        dlg.geometry("360x500")
        ttk.Label(dlg, text="Choose which columns to display (S.No always visible):").pack(pady=6)
        current_vis = set(self.cfg.get("visible_columns", list(self.LOGICAL_COLUMNS)))
        vars_by_col: Dict[str, tk.BooleanVar] = {}
        box = ttk.Frame(dlg)
        box.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        for col in self.LOGICAL_COLUMNS:
            if col == "S.No":
                ttk.Label(box, text="S.No (always visible)").pack(anchor="w")
                continue
            v = tk.BooleanVar(value=(col in current_vis))
            vars_by_col[col] = v
            ttk.Checkbutton(box, text=col, variable=v).pack(anchor="w")
    def _select_email_columns_dialog(self):
        """Dialog to choose which columns appear in the emailed HTML report."""
        dlg = tk.Toplevel(self)
        dlg.title("Select Email Columns")
        dlg.geometry("360x520")
        ttk.Label(dlg, text="Choose which columns to include in the email report:").pack(pady=6)

        current_email_cols = list(self.cfg.get("email_columns", list(self.LOGICAL_COLUMNS)))
        # Ensure all known columns appear in the selection list
        all_cols = [c for c in self.LOGICAL_COLUMNS]
        # S.No is optional in emails; keep it selectable
        vars_by_col = {}
        box = ttk.Frame(dlg)
        box.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)

        # Add "Select All" / "Clear All" helpers
        helper = ttk.Frame(dlg)
        helper.pack(fill=tk.X, padx=10, pady=(0,6))
        def set_all(state: bool):
            for v in vars_by_col.values():
                v.set(state)

        ttk.Button(helper, text="Select All", command=lambda: set_all(True)).pack(side=tk.LEFT, padx=(0,6))
        ttk.Button(helper, text="Clear All", command=lambda: set_all(False)).pack(side=tk.LEFT)

        for col in all_cols:
            v = tk.BooleanVar(value=(col in current_email_cols))
            vars_by_col[col] = v
            ttk.Checkbutton(box, text=col, variable=v).pack(anchor="w")

        def apply_and_close():
            # Keep the order consistent with current display order if possible, otherwise LOGICAL_COLUMNS
            current_display = list(self.tree["displaycolumns"])
            order_ref = current_display if current_display else list(self.LOGICAL_COLUMNS)
            selected = [c for c in order_ref if vars_by_col.get(c, tk.BooleanVar(value=True)).get() and c in all_cols]
            if not selected:
                # Fail-safe: if user clears everything, keep at least S.No and DB Name
                selected = [c for c in ["S.No","DB Name"] if c in all_cols]
            self.cfg["email_columns"] = selected
            from tkinter import messagebox
            messagebox.showinfo(APP_NAME, f"Email columns updated ({len(selected)} selected).")
            # Persist to config
            save_config(self.cfg)
            dlg.destroy()

        ttk.Button(dlg, text="Apply", command=apply_and_close).pack(pady=8)


        def apply_and_close():
            order = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
            order = [c for c in order if c in self.LOGICAL_COLUMNS]
            vis = ["S.No"] + [c for c in order if c != "S.No" and vars_by_col.get(c, tk.BooleanVar(value=True)).get()]
            self.tree["displaycolumns"] = vis
            self.cfg["visible_columns"] = vis
            save_config(self.cfg)
            dlg.destroy()

        ttk.Button(dlg, text="Apply", command=apply_and_close).pack(pady=8)

    # ---- Context menu ----
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
        col_index = int(colid.replace("#", "")) - 1
        vals = self.tree.item(row)["values"]
        text = str(vals[col_index]) if col_index < len(vals) else ""
        self.clipboard_clear()
        self.clipboard_append(text)

    def _copy_by_col(self, colname: str):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        vals = self.tree.item(iid)["values"]
        idx = self.LOGICAL_COLUMNS.index(colname)
        text = str(vals[idx]) if idx < len(vals) else ""
        self.clipboard_clear()
        self.clipboard_append(text)

    # ---- Sorting helpers ----
    def _parse_sessions(self, s: str) -> Tuple[int, int]:
        t = str(s).strip()
        if " " in t:
            t = t.split()[-1]
        try:
            a, b = t.split("/")
            return (int(a), int(b))
        except Exception:
            return (0, 0)

    def _parse_pct(self, s: str) -> float:
        try:
            return float(str(s).replace("%", "").split()[-1])
        except Exception:
            return -1.0

    def _parse_datecell(self, s: str) -> float:
        parts = str(s).strip().split()
        if not parts or parts[-1] == "-":
            return float("-inf")
        try:
            if len(parts) >= 2 and ":" in parts[-1]:
                dt = datetime.strptime(f"{parts[-2]} {parts[-1]}", "%Y-%m-%d %H:%M:%S")
                return dt.timestamp()
            dt = datetime.strptime(parts[-1], "%Y-%m-%d")
            return dt.timestamp()
        except Exception:
            return float("-inf")

    def _status_rank(self, s: str) -> int:
        return 1 if str(s).strip().startswith(GOOD) else 0

    def _inst_rank(self, s: str) -> int:
        return 1 if ("OPEN" in str(s).upper() and str(s).strip().startswith(GOOD)) else 0

    def _ts_online_rank(self, s: str) -> Tuple[int, int]:
        t = str(s).strip()
        if " " in t:
            t = t.split()[-1]
        try:
            a, b = t.split("/")
            return (int(a), int(b))
        except Exception:
            return (0, 0)

    def _generic_key(self, col: str, s: str):
        if col == "S.No":
            try:
                return (int(s),)
            except:
                return (0,)
        if col == "Status":
            return (self._status_rank(s), s)
        if col == "Inst_status":
            return (self._inst_rank(s), s)
        if col == "WorstTS%":
            return (self._parse_pct(s),)
        if col in ("LastFull/Inc", "LastArch", "LastChecked", "Startup Time"):
            return (self._parse_datecell(s),)
        if col == "Sessions":
            curr, limit = self._parse_sessions(s)
            return (curr / limit if limit else -1.0, curr, limit)
        if col == "TS Online":
            on, tot = self._ts_online_rank(s)
            return (on, tot)
        if col == "DB Size":
            try:
                return (float(str(s).split()[0]),)
            except:
                return (-1.0,)
        if col == "Ms":
            try:
                return (int(s),)
            except:
                return (-1,)
        if col == "Check status":
            order = {"In Progress": 0, "Complete": 1}
            return (order.get(s, 2), s)
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

    def _pick_client_dir(self):
        d = filedialog.askdirectory(title="Select Oracle Client lib directory")
        if d:
            self.client_dir_var.set(d)
            self.cfg["client_lib_dir"] = d
            save_config(self.cfg)

    def _refresh_table_from_targets(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for idx, t in enumerate(self.targets, start=1):
            values = ["-"] * len(self.LOGICAL_COLUMNS)
            values[0] = idx
            values[1] = t.name
            values[2] = t.environment
            self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber()
        self._autosize_columns()

    def _load_last_health_into_rows(self):
        for t in self.targets:
            hdict = self.last_health.get(t.name)
            if not hdict:
                continue
            self._apply_persisted_row(t.name, hdict)
        self._autosize_columns()

    def _apply_persisted_row(self, name: str, hdict: Dict[str, Any]):
        vals = list(self.tree.item(name)["values"])
        def mark(ok: bool) -> str:
            return GOOD if ok else BAD

        status_cell = f"{mark(hdict.get('status', '').upper() == 'UP')} {hdict.get('status', '-')}"
        inst_cell = f"{mark((hdict.get('inst_status', '') or '').upper() == 'OPEN')} {hdict.get('inst_status', '-')}"
        sc = int(hdict.get("sessions_curr", 0))
        sl = int(hdict.get("sessions_limit", 0) or 0)
        sess_ok = (sl == 0) or (sc < 0.95 * sl)
        sessions_cell = f"{mark(sess_ok)} {sc}/{sl}" if sl else f"{BAD} 0/0"
        worst = hdict.get("worst_ts_pct_used")
        worst_ok = not (worst is not None and float(worst) >= 90.0)
        worst_val = "-" if worst is None else f"{float(worst):.1f}%"
        worst_cell = f"{mark(worst_ok)} {worst_val}"
        startup_str = hdict.get("startup_time_str", "-")
        on = int(hdict.get("ts_online", 0) or 0)
        tot = int(hdict.get("ts_total", 0) or 0)
        ts_ok = (tot == on and tot > 0)
        ts_cell = f"{mark(ts_ok)} {on}/{tot}" if tot else f"{BAD} 0/0"
        db_size_cell = f"{hdict.get('db_size_gb', '-')} GB" if hdict.get("db_size_gb") is not None else "-"
        colidx = {c: i for i, c in enumerate(self.LOGICAL_COLUMNS)}
        vals[colidx["Host"]] = hdict.get("host", "-")
        vals[colidx["Status"]] = status_cell
        vals[colidx["Inst_status"]] = inst_cell
        vals[colidx["Sessions"]] = sessions_cell
        vals[colidx["WorstTS%"]] = worst_cell
        vals[colidx["LastFull/Inc"]] = hdict.get("last_full_inc_backup_str", f"{BAD} -")
        vals[colidx["LastArch"]] = hdict.get("last_arch_backup_str", f"{BAD} -")
        vals[colidx["DB Version"]] = hdict.get("version", "-")
        vals[colidx["Startup Time"]] = startup_str
        vals[colidx["TS Online"]] = ts_cell
        vals[colidx["DB Size"]] = db_size_cell
        vals[colidx["Ms"]] = hdict.get("elapsed_ms", 0)
        vals[colidx["LastChecked"]] = hdict.get("ts", "-")
        vals[colidx["Check status"]] = "Complete"
        vals[colidx["Error"]] = hdict.get("error", "")
        self.tree.item(name, values=vals)

    def run_all_once(self):
        self._checks_async(targets=self.targets)

    def run_selected_once(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row (DB) to run.")
            return
        name = sel[0]
        target = next((x for x in self.targets if x.name == name), None)
        if not target:
            messagebox.showerror(APP_NAME, "Selected DB not found.")
            return
        self._checks_async(targets=[target])

    def _loop(self):
        if not self.auto_var.get():
            return
        self._checks_async(targets=self.targets)
        self.after(self.interval_var.get() * 1000, self._loop)

    def _checks_async(self, targets: List[DbTarget]):
        for t in targets:
            self._set_check_status(t.name, "In Progress")

        def job(t: DbTarget):
            try:
                res = check_one(t, self.cfg)
            except Exception as e:
                res = DbHealth(status="DOWN", details=str(e), error=str(e))  # type: ignore
            self.after(0, lambda tn=t.name, tr=t, rh=res: self._apply_result(tn, tr, rh))

        for t in targets:
            threading.Thread(target=job, args=(t,), daemon=True).start()

    def _set_check_status(self, name: str, status: str):
        if name in self.tree.get_children():
            vals = list(self.tree.item(name)["values"])
            idx = self.LOGICAL_COLUMNS.index("Check status")
            if len(vals) <= idx:
                vals += [""] * (idx + 1 - len(vals))
            vals[idx] = status
            self.tree.item(name, values=vals)

    def _apply_result(self, name: str, target: DbTarget, h: DbHealth):
        status_cell = f"{GOOD if h.status.upper() == 'UP' else BAD} {h.status}"
        inst_cell = f"{GOOD if (h.inst_status or '').upper() == 'OPEN' else BAD} {h.inst_status or '-'}"
        if h.sessions_limit and h.sessions_limit > 0:
            sess_ok = h.sessions_curr < 0.95 * h.sessions_limit
            sessions_cell = f"{GOOD if sess_ok else BAD} {h.sessions_curr}/{h.sessions_limit}"
        else:
            sessions_cell = f"{BAD} 0/0"
        worst_ok = not (h.worst_ts_pct_used is not None and h.worst_ts_pct_used >= 90.0)
        worst_val = "-" if h.worst_ts_pct_used is None else f"{h.worst_ts_pct_used:.1f}%"
        worst_cell = f"{GOOD if worst_ok else BAD} {worst_val}"

        def fmt_backup(dt: Optional[datetime], arch=False):
            if not dt:
                return f"{BAD} -"
            age_hours = (datetime.now(dt.tzinfo) - dt).total_seconds() / 3600.0
            ok = (age_hours <= 12) if arch else ((age_hours / 24.0) <= 3)
            return f"{GOOD if ok else BAD} {_dt_str(dt)}"

        last_full_cell = fmt_backup(h.last_full_inc_backup, arch=False)
        last_arch_cell = fmt_backup(h.last_arch_backup, arch=True)
        startup_str = _dt_str(h.startup_time)

        if (h.ts_total or 0) > 0:
            ts_ok = (h.ts_online == h.ts_total)
            ts_cell = f"{GOOD if ts_ok else BAD} {h.ts_online}/{h.ts_total}"
        else:
            ts_cell = f"{BAD} 0/0"

        db_size_cell = f"{h.db_size_gb:.1f} GB" if h.db_size_gb is not None else "-"

        vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
        colidx = {c: i for i, c in enumerate(self.LOGICAL_COLUMNS)}
        vals[colidx["Host"]] = h.host or "-"
        vals[colidx["Status"]] = status_cell
        vals[colidx["Inst_status"]] = inst_cell
        vals[colidx["Sessions"]] = sessions_cell
        vals[colidx["WorstTS%"]] = worst_cell
        vals[colidx["LastFull/Inc"]] = last_full_cell
        vals[colidx["LastArch"]] = last_arch_cell
        vals[colidx["DB Version"]] = h.version or "-"
        vals[colidx["Startup Time"]] = startup_str
        vals[colidx["TS Online"]] = ts_cell
        vals[colidx["DB Size"]] = db_size_cell
        vals[colidx["Ms"]] = h.elapsed_ms
        vals[colidx["LastChecked"]] = h.ts
        vals[colidx["Check status"]] = "Complete"
        vals[colidx["Error"]] = h.error or ("" if h.status == "UP" else h.details)
        self.tree.item(name, values=vals)

        # persist
        self.last_health[name] = {
            "status": h.status,
            "inst_status": h.inst_status,
            "sessions_curr": h.sessions_curr,
            "sessions_limit": h.sessions_limit,
            "worst_ts_pct_used": h.worst_ts_pct_used,
            "host": h.host,
            "elapsed_ms": h.elapsed_ms,
            "version": h.version,
            "startup_time_str": startup_str,
            "ts_online": h.ts_online,
            "ts_total": h.ts_total,
            "db_size_gb": h.db_size_gb,
            "ts": h.ts,
            "error": vals[colidx["Error"]],
            "last_full_inc_backup_str": last_full_cell,
            "last_arch_backup_str": last_arch_cell,
        }
        self.cfg["last_health"] = self.last_health
        save_config(self.cfg)

        self._renumber()
        self._autosize_columns()

    # ---- Clear / CRUD / Import-Export / Email ----
    def _clear_row_values(self, vals: List[Any]) -> List[Any]:
        res = list(vals)
        colidx = {c: i for i, c in enumerate(self.LOGICAL_COLUMNS)}
        for col in self.STATUS_COLUMNS:
            i = colidx[col]
            res[i] = 0 if col == "Ms" else "-"
        return res

    def _clear_all_rows(self):
        for iid in self.tree.get_children(""):
            vals = list(self.tree.item(iid)["values"])
            cleared = self._clear_row_values(vals)
            self.tree.item(iid, values=cleared)
        self.status_var.set("Cleared all rows (except S.No, DB Name, Environment).")

    def _clear_selected_row(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row to clear.")
            return
        iid = sel[0]
        vals = list(self.tree.item(iid)["values"])
        cleared = self._clear_row_values(vals)
        self.tree.item(iid, values=cleared)
        self.status_var.set(f"Cleared row: {iid}")

    def _add_dialog(self):
        DbEditor(self, on_save=self._add_target)

    def _edit_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row to edit.")
            return
        name = sel[0]
        t = next((x for x in self.targets if x.name == name), None)
        if not t:
            messagebox.showerror(APP_NAME, "Target not found.")
            return
        DbEditor(self, target=t, on_save=self._update_target)

    def _remove_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        name = sel[0]
        self.targets = [t for t in self.targets if t.name != name]
        self.tree.delete(name)
        self._persist_targets()
        self._renumber()

    def _add_target(self, t: DbTarget):
        if any(x.name == t.name for x in self.targets):
            messagebox.showerror(APP_NAME, "A target with this name already exists.")
            return
        self.targets.append(t)
        self._persist_targets()
        values = ["-"] * len(self.LOGICAL_COLUMNS)
        values[0] = len(self.targets)
        values[1] = t.name
        values[2] = t.environment
        self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber()
        self._autosize_columns()

    def _update_target(self, t: DbTarget):
        found = False
        for i, x in enumerate(self.targets):
            if x.name == t.name:
                self.targets[i] = t
                found = True
                break
        if not found:
            self.targets.append(t)
        self._persist_targets()
        if t.name in self.tree.get_children(""):
            vals = list(self.tree.item(t.name)["values"])
            vals[1] = t.name
            vals[2] = t.environment
            self.tree.item(t.name, values=vals)
        self._autosize_columns()

    def _persist_targets(self):
        self.cfg["interval_sec"] = self.interval_var.get()
        self.cfg["targets"] = [_serialize_target(t) for t in self.targets]
        self.cfg["client_lib_dir"] = self.client_dir_var.get()
        self.cfg["auto_run"] = self.auto_var.get()
        save_config(self.cfg)

    def _import_json(self):
        p = filedialog.askopenfilename(title="Import config (.json)", filetypes=[["JSON", "*.json"]])
        if not p:
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            self.cfg.update(cfg)
            if "email" in cfg:
                self.cfg["email"].update(cfg["email"] or {})
            self.interval_var.set(int(self.cfg.get("interval_sec", 300)))
            self.client_dir_var.set(self.cfg.get("client_lib_dir", ""))
            self.auto_var.set(bool(self.cfg.get("auto_run", False)))
            self.targets = [_hydrate_target(t) for t in self.cfg.get("targets", [])]
            self.last_health = self.cfg.get("last_health", {})
            order = [c for c in self.cfg.get("column_order", list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
            if order and order[0] != "S.No":
                order = ["S.No"] + [c for c in order if c != "S.No"]
            visible = [c for c in self.cfg.get("visible_columns", order) if c in self.LOGICAL_COLUMNS]
            if visible and visible[0] != "S.No":
                visible = ["S.No"] + [c for c in visible if c != "S.No"]
            self.tree["displaycolumns"] = visible
            if "column_widths" in self.cfg:
                for col, w in self.cfg["column_widths"].items():
                    try:
                        self.tree.column(col, width=int(w))
                    except:
                        pass
            save_config(self.cfg)
            self._refresh_table_from_targets()
            self._load_last_health_into_rows()
            messagebox.showinfo(APP_NAME, "Imported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to import: {e}")

    def _export_json(self):
        p = filedialog.asksaveasfilename(
            title="Export config", defaultextension=".json", initialfile="oracle_config.json"
        )
        if not p:
            return
        try:
            export = {
                "interval_sec": self.interval_var.get(),
                "targets": [_serialize_target(t) for t in self.targets],
                "client_lib_dir": self.client_dir_var.get(),
                "email": {
                    "server": self.smtp_server_var.get().strip(),
                    "port": int(self.smtp_port_var.get() or 25),
                    "from_addr": self.from_var.get().strip(),
                    "to_addrs": self.to_var.get().strip(),
                    "subject": self.cfg.get("email", {}).get("subject", "Oracle DB Health Report"),
                },
                "last_health": self.last_health,
                "auto_run": self.auto_var.get(),
                "column_order": list(self.cfg.get("column_order", self.LOGICAL_COLUMNS)),
                "visible_columns": list(self.tree["displaycolumns"]),
                "email_columns": list(self.cfg.get("email_columns", self.LOGICAL_COLUMNS)),
                "column_widths": {c: self.tree.column(c, "width") for c in self.LOGICAL_COLUMNS},
            }
            with open(p, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo(APP_NAME, "Exported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to export: {e}")

    def _save_mail_settings(self):
        self.cfg.setdefault("email", {})
        self.cfg["email"]["server"] = self.smtp_server_var.get().strip()
        try:
            self.cfg["email"]["port"] = int(self.smtp_port_var.get() or 25)
        except Exception:
            self.cfg["email"]["port"] = 25
        self.cfg["email"]["from_addr"] = self.from_var.get().strip()
        self.cfg["email"]["to_addrs"] = self.to_var.get().strip()
        save_config(self.cfg)
        messagebox.showinfo(APP_NAME, "Mail settings saved.")

    def _email_report(self):
        email_cfg = self.cfg.get("email", {})
        server = self.smtp_server_var.get().strip() or email_cfg.get("server", "")
        port = int(self.smtp_port_var.get() or email_cfg.get("port", 25))
        from_addr = self.from_var.get().strip() or email_cfg.get("from_addr", "")
        to_addrs = self.to_var.get().strip() or email_cfg.get("to_addrs", "")
        subject = email_cfg.get("subject", "Oracle DB Health Report")
        if not (server and from_addr and to_addrs):
            messagebox.showerror(APP_NAME, "Set SMTP server, From, and To addresses first.")
            return
        rows = [self.tree.item(i)["values"] for i in self.tree.get_children("")]
        html = self._build_html(rows)
        try:
            self._send_html_email(
                server, port, from_addr, [x.strip() for x in to_addrs.split(",") if x.strip()], subject, html
            )
            messagebox.showinfo(APP_NAME, "Email report sent.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to send email: {e}")

    def _build_html(self, rows: List[List]) -> str:
        headers = [c for c in self.cfg.get("email_columns", list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
        if not headers:
            headers = list(self.LOGICAL_COLUMNS)

        def cell_style(text: str, col: str) -> str:
            ok = None
            if col in ("Status", "Inst_status", "WorstTS%", "LastFull/Inc", "LastArch", "Sessions", "TS Online"):
                t = str(text).strip()
                if t.startswith(GOOD):
                    ok = True
                elif t.startswith(BAD):
                    ok = False
            if col == "WorstTS%":
                try:
                    pct = float(str(text).split()[-1].replace("%", ""))
                    ok = pct < 90.0
                except Exception:
                    pass
            if ok is True:
                return "background-color:#e6ffe6;color:#064b00;font-weight:bold;"
            if ok is False:
                return "background-color:#ffe6e6;color:#7a0000;font-weight:bold;"
            return ""

        thead = (
            "<tr>"
            + "".join(
                f"<th style='padding:6px 10px;border-bottom:1px solid #ccc;text-align:left'>{h}</th>" for h in headers
            )
            + "</tr>"
        )
        body_rows = []
        for r in rows:
            tds = []
            for col in headers:
                try:
                    idx = self.LOGICAL_COLUMNS.index(col)
                    val = r[idx]
                except Exception:
                    val = ""
                style = cell_style(val, col)
                tds.append(f"<td style='padding:4px 8px;border-bottom:1px solid #eee;{style}'>{val}</td>")
            body_rows.append("<tr>" + "".join(tds) + "</tr>")
        table = (
            "<table style='border-collapse:collapse;font-family:Segoe UI,Arial,sans-serif;font-size:12px'>"
            + thead
            + "".join(body_rows)
            + "</table>"
        )
        title = f"<h3>Oracle DB Health Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h3>"
        return "<html><body>" + title + table + "</body></html>"

    def _send_html_email(self, server: str, port: int, from_addr: str, to_addrs: List[str], subject: str, html: str):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        part = MIMEText(html, "html", "utf-8")
        msg.attach(part)
        with smtplib.SMTP(server, port, timeout=20) as s:
            s.sendmail(from_addr, to_addrs, msg.as_string())

# -------- Add/Edit DB Dialog --------
class DbEditor(tk.Toplevel):
    def __init__(self, app: MonitorApp, target: Optional[DbTarget] = None, on_save=None):
        super().__init__(app)
        self.app = app
        self.cfg = app.cfg
        self.on_save = on_save
        self.title("Add / Edit Database")
        self.resizable(False, False)

        # vars
        self.name_var = tk.StringVar(value=target.name if target else "")
        self.env_var = tk.StringVar(value=target.environment if target else "NON-PROD")
        self.mode_var = tk.StringVar(value=("thick" if (target and target.mode.lower() == "thick") else "thin"))

        # common creds (thin also needs user/pass)
        self.user_var = tk.StringVar(value=target.user if target else "")
        self.pass_var = tk.StringVar(value=_decrypt_password(target.password_enc) if target and target.password_enc else "")

        # TNS
        self.tns_var = tk.StringVar(value=(target.dsn if target and target.mode == "thick" else ""))

        # Thin
        host = port = svc = sid = ""
        if target and target.mode == "thin":
            h, p, svc, sid = parse_ezconnect(target.dsn)
            host, port = h or "", p or "1521"
        self.thin_host = tk.StringVar(value=host)
        self.thin_port = tk.StringVar(value=port or "1521")
        self.thin_conn_using = tk.StringVar(value=("Service Name" if svc else "SID" if sid else "Service Name"))
        self.thin_service = tk.StringVar(value=svc or "")
        self.thin_sid = tk.StringVar(value=sid or "")

        # layout
        body = ttk.Frame(self, padding=10)
        body.pack(fill=tk.BOTH, expand=True)

        row = 0
        ttk.Label(body, text="DB Name:").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(body, textvariable=self.name_var, width=32).grid(row=row, column=1, columnspan=3, sticky="w", padx=4, pady=4)

        row += 1
        ttk.Label(body, text="Environment:").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Combobox(body, textvariable=self.env_var, values=["NON-PROD", "PROD"], width=12, state="readonly").grid(
            row=row, column=1, sticky="w", padx=4, pady=4
        )

        row += 1
        ttk.Label(body, text="Connection Type:").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Radiobutton(body, text="TNS (Oracle Client)", variable=self.mode_var, value="thick", command=self._refresh_mode).grid(
            row=row, column=1, sticky="w"
        )
        ttk.Radiobutton(body, text="JDBC Thin (no client)", variable=self.mode_var, value="thin", command=self._refresh_mode).grid(
            row=row, column=2, sticky="w"
        )

        # TNS block
        row += 1
        self.tns_block = ttk.LabelFrame(body, text="TNS (uses Oracle Client)")
        self.tns_block.grid(row=row, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(self.tns_block, text="TNS Alias/Descriptor:").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.tns_block, textvariable=self.tns_var, width=36).grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.tns_block, text="User:").grid(row=1, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.tns_block, textvariable=self.user_var, width=24).grid(row=1, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.tns_block, text="Password:").grid(row=2, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.tns_block, textvariable=self.pass_var, width=24, show="*").grid(row=2, column=1, sticky="w", padx=4, pady=4)

        # Thin block
        row += 1
        self.thin_block = ttk.LabelFrame(body, text="JDBC Thin (no client)")
        self.thin_block.grid(row=row, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(self.thin_block, text="Host:").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_block, textvariable=self.thin_host, width=24).grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.thin_block, text="Port:").grid(row=0, column=2, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_block, textvariable=self.thin_port, width=8).grid(row=0, column=3, sticky="w", padx=4, pady=4)

        ttk.Label(self.thin_block, text="Connection Using:").grid(row=1, column=0, sticky="e", padx=4, pady=4)
        ttk.Radiobutton(self.thin_block, text="Service Name", variable=self.thin_conn_using, value="Service Name", command=self._refresh_mode).grid(
            row=1, column=1, sticky="w"
        )
        ttk.Radiobutton(self.thin_block, text="SID", variable=self.thin_conn_using, value="SID", command=self._refresh_mode).grid(
            row=1, column=2, sticky="w"
        )

        ttk.Label(self.thin_block, text="Service Name:").grid(row=2, column=0, sticky="e", padx=4, pady=4)
        self.entry_service = ttk.Entry(self.thin_block, textvariable=self.thin_service, width=24)
        self.entry_service.grid(row=2, column=1, sticky="w", padx=4, pady=4)

        ttk.Label(self.thin_block, text="SID:").grid(row=2, column=2, sticky="e", padx=4, pady=4)
        self.entry_sid = ttk.Entry(self.thin_block, textvariable=self.thin_sid, width=18)
        self.entry_sid.grid(row=2, column=3, sticky="w", padx=4, pady=4)

        ttk.Label(self.thin_block, text="User:").grid(row=3, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_block, textvariable=self.user_var, width=24).grid(row=3, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.thin_block, text="Password:").grid(row=3, column=2, sticky="e", padx=4, pady=4)
        ttk.Entry(self.thin_block, textvariable=self.pass_var, width=18, show="*").grid(row=3, column=3, sticky="w", padx=4, pady=4)

        # Buttons
        btns = ttk.Frame(self, padding=(10, 6))
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="Test Connection", command=self._test_connection).pack(side=tk.LEFT)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(btns, text="Save", command=self._save).pack(side=tk.RIGHT)

        self._refresh_mode()
        self.grab_set()
        self.transient(app)

    def _refresh_mode(self):
        thick = self.mode_var.get() == "thick"
        # Show/hide blocks
        for child in self.tns_block.winfo_children():
            child.configure(state=("normal" if thick else "disabled"))
        for child in self.thin_block.winfo_children():
            child.configure(state=("normal" if not thick else "disabled"))
        # Toggle service vs sid
        use_service = self.thin_conn_using.get() == "Service Name"
        self.entry_service.configure(state=("normal" if (not thick and use_service) else "disabled"))
        self.entry_sid.configure(state=("normal" if (not thick and not use_service) else "disabled"))

    def _test_connection(self):
        try:
            t = self._target_from_fields()
            with connect_target(t, self.cfg) as conn:
                cur = conn.cursor()
                cur.execute("select 1 from dual")
                _ = cur.fetchone()
            messagebox.showinfo(APP_NAME, f"Connection OK: {t.name}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Connection failed:\n{e}")

    def _target_from_fields(self) -> DbTarget:
        name = self.name_var.get().strip()
        env = self.env_var.get().strip() or "NON-PROD"
        mode = self.mode_var.get()
        user = self.user_var.get().strip() or None
        pwd = self.pass_var.get()
        if mode == "thick":
            dsn = self.tns_var.get().strip()
            return DbTarget(name=name, dsn=dsn, user=user, password_enc=_encrypt_password(pwd) if pwd else None, mode="thick", environment=env)
        else:
            host = self.thin_host.get().strip()
            port = self.thin_port.get().strip() or "1521"
            if self.thin_conn_using.get() == "Service Name":
                service = self.thin_service.get().strip()
                dsn = ezconnect_service(host, port, service)
            else:
                sid = self.thin_sid.get().strip()
                dsn = ezconnect_sid(host, port, sid)
            return DbTarget(name=name, dsn=dsn, user=user, password_enc=_encrypt_password(pwd) if pwd else None, mode="thin", environment=env)

    def _save(self):
        try:
            t = self._target_from_fields()
            if not t.name:
                messagebox.showerror(APP_NAME, "DB Name is required.")
                return
            if self.on_save:
                self.on_save(t)
            self.destroy()
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to save: {e}")
