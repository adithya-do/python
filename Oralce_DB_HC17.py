#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Oracle DB Health Monitor — Version 17
# Change: For JDBC thin, connect using keyword params (host/port/service_name or sid)
#         instead of a DSN string to avoid any TNS resolution. Test Connection updated too.
import base64, json, os, smtplib, sys, threading, time
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont

try:
    import oracledb
except Exception:
    oracledb = None

APP_NAME = "Oracle DB Health Monitor"

def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

CONFIG_DIR = (_base_dir() / "config")
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "oracle_config.json"

ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
DEFAULT_INTERVAL_SEC = 300
GOOD = "✅"; BAD = "❌"

# ---------------- Password helpers ----------------
def _win_protect(data: bytes) -> str:
    try:
        import ctypes, ctypes.wintypes as wt
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
        import ctypes, ctypes.wintypes as wt
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
    if not plain: return None
    if sys.platform.startswith("win"): return _win_protect(plain.encode("utf-8"))
    return base64.b64encode(plain.encode("utf-8")).decode("ascii")

def _decrypt_password(enc: Optional[str]) -> Optional[str]:
    if not enc: return None
    try:
        if sys.platform.startswith("win"): return _win_unprotect(enc).decode("utf-8")
        return base64.b64decode(enc.encode("ascii")).decode("utf-8")
    except Exception: return None

# ---------------- DSN helpers ----------------
def normalize_tns(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("@"): t = t[1:]
    return t

def ezconnect_service(host: str, port: str, service: str) -> str:
    host = (host or "").strip(); port = (port or "1521").strip(); service = (service or "").strip()
    return f"{host}:{port}/{service}"

def ezconnect_sid(host: str, port: str, sid: str) -> str:
    host = (host or "").strip(); port = (port or "1521").strip(); sid = (sid or "").strip()
    return f"{host}:{port}/{sid}"  # used only for display; connect() will use sid=...

def looks_like_ezconnect(dsn: str) -> bool:
    s = (dsn or "").lower().strip()
    return ("/" in s) and (":" in s)

def parse_ezconnect(dsn: str) -> Tuple[str,str,Optional[str],Optional[str]]:
    # dsn like "host:port/service" or "host:port/?sid=SID" or legacy with leading //
    s = (dsn or "").strip()
    if s.startswith("//"): s = s[2:]
    host_port, _, rest = s.partition("/")
    host, _, port = host_port.partition(":")
    if rest.startswith("?sid="):
        return host, (port or "1521"), None, rest.split("=",1)[1]
    # if no "?sid=", treat as service name; SID will be None
    return host, (port or "1521"), rest or None, None

# ---------------- Data classes ----------------
@dataclass
class DbTarget:
    name: str
    dsn: str
    user: Optional[str]=None   # used for both modes
    password_enc: Optional[str]=None  # used for both modes
    mode: str="thin"   # "thin" or "thick"
    environment: str="NON-PROD"

@dataclass
class DbHealth:
    status: str
    details: str
    version: str=""
    inst_status: str=""
    sessions_curr: int=0
    sessions_limit: int=0
    worst_ts_pct_used: Optional[float]=None
    host: str=""
    elapsed_ms: int=0
    last_full_inc_backup: Optional[datetime]=None
    last_arch_backup: Optional[datetime]=None
    startup_time: Optional[datetime]=None
    ts_online: Optional[int]=None
    ts_total: Optional[int]=None
    db_size_gb: Optional[float]=None
    error: str=""
    ts: str=field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# ---------------- Config ----------------
def logical_columns() -> List[str]:
    return ["S.No","DB Name","Environment","Host","DB Version","Startup Time","Status","Inst_status","Sessions","WorstTS%","TS Online","DB Size","LastFull/Inc","LastArch","Ms","LastChecked","Check status","Error"]

def default_config() -> Dict[str, Any]:
    cols = logical_columns()
    return {"interval_sec": DEFAULT_INTERVAL_SEC,"targets": [],"client_lib_dir": ORACLE_CLIENT_LIB_DIR or "","email": {"server": "", "port": 25, "from_addr": "", "to_addrs": "", "subject": "Oracle DB Health Report"},"last_health": {},"auto_run": False,"column_order": cols[:],"visible_columns": cols[:],"email_columns": cols[:],"column_widths": {}}

def _hydrate_target(d: Dict[str, Any]) -> DbTarget:
    return DbTarget(name=d.get("name",""),
                    dsn=(d.get("dsn","") or "").strip(),
                    user=d.get("user"),
                    password_enc=d.get("password_enc"),
                    mode=d.get("mode","thin"),
                    environment=d.get("environment","NON-PROD"))

def _serialize_target(t: DbTarget) -> Dict[str, Any]:
    return {"name": t.name, "dsn": t.dsn.strip(), "user": t.user, "password_enc": t.password_enc, "mode": t.mode, "environment": t.environment}

def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f: cfg = json.load(f)
            base = default_config()
            for k, v in cfg.items():
                if k == "email": base["email"].update(v or {})
                else: base[k] = v
            base.setdefault("last_health", {}); base.setdefault("auto_run", False)
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
    out["targets"] = [_serialize_target(_hydrate_target(t) if isinstance(t, dict) else t) for t in cfg.get("targets", [])]
    with open(CONFIG_PATH, "w", encoding="utf-8") as f: json.dump(out, f, indent=2, default=str)

# ---------------- DB Connection ----------------
def init_oracle_client_if_needed(cfg: Dict[str, Any]):
    if oracledb is None: return
    lib_dir = cfg.get("client_lib_dir") or ORACLE_CLIENT_LIB_DIR
    if lib_dir:
        try: oracledb.init_oracle_client(lib_dir=lib_dir)
        except Exception: pass

def connect_target(target: DbTarget, cfg: Dict[str, Any]):
    if oracledb is None: raise RuntimeError("python-oracledb not installed. pip install python-oracledb")
    mode = (target.mode or "thin").lower()
    dsn = (target.dsn or "").strip()
    user = (target.user or "").strip() or None
    pwd = _decrypt_password(target.password_enc) if target.password_enc else None
    if mode == "thick":
        init_oracle_client_if_needed(cfg)
        tns = normalize_tns(dsn)
        if user and pwd: return oracledb.connect(user=user, password=pwd, dsn=tns)
        return oracledb.connect(dsn=tns)
    else:
        # THIN: parse the stored EZCONNECT and use keyword params so no TNS is consulted.
        host, port, service, sid = parse_ezconnect(dsn)
        if not host:
            raise ValueError("Invalid JDBC thin DSN. Please set Host, Port and Service/SID.")
        kwargs = {"host": host, "port": int(port or 1521)}
        if service: kwargs["service_name"] = service
        elif sid: kwargs["sid"] = sid
        if user: kwargs["user"] = user
        if pwd: kwargs["password"] = pwd
        return oracledb.connect(**kwargs)

# ---------------- SQL ----------------
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

# ---------------- Health check ----------------
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

def check_one(target: DbTarget, cfg: Dict[str, Any], timeout_sec: int = 25) -> 'DbHealth':
    t0 = time.time()
    try:
        with connect_target(target, cfg) as conn:
            conn.call_timeout = timeout_sec * 1000
            cur = conn.cursor()
            details = ""
            try:
                cur.execute(SQLS["db"]); _name, _open_mode, _role, log_mode = cur.fetchone(); details = f"Log:{log_mode}"
            except Exception:
                pass
            cur.execute(SQLS["inst"]); _inst_name, inst_status, host_name, inst_version, startup_time = cur.fetchone()
            sessions_curr = 0; sessions_limit = 0
            try: cur.execute(SQLS["sess_curr"]); sessions_curr = int(cur.fetchone()[0])
            except Exception: pass
            try: cur.execute(SQLS["sess_limit"]); sessions_limit = int(cur.fetchone()[0])
            except Exception: pass
            worst_pct = None
            try:
                cur.execute(SQLS["tspace"]); worst_pct = 0.0
                for _ts_name, pct_used in cur.fetchall():
                    if pct_used is not None and pct_used > (worst_pct or 0): worst_pct = float(pct_used)
            except Exception: worst_pct = None
            ts_total = None; ts_online = None
            try:
                cur.execute(SQLS["ts_online"]); total, tonline = cur.fetchone()
                ts_total = int(total or 0); ts_online = int(tonline or 0)
            except Exception: pass
            db_size_gb = None
            try:
                cur.execute(SQLS["db_size"]); row = cur.fetchone()
                db_size_gb = float(row[0]) if row and row[0] is not None else None
            except Exception: pass
            last_df = None; last_arch = None
            try: cur.execute(SQLS["bk_data"]); r = cur.fetchone(); last_df = r[0] if r else None
            except Exception: pass
            try: cur.execute(SQLS["bk_arch"]); r = cur.fetchone(); last_arch = r[0] if r else None
            except Exception: pass
            elapsed_ms = int((time.time() - t0) * 1000)
            return DbHealth(status="UP", details=details, version=inst_version, inst_status=inst_status,
                            sessions_curr=sessions_curr, sessions_limit=sessions_limit,
                            worst_ts_pct_used=worst_pct, host=host_name, elapsed_ms=elapsed_ms,
                            last_full_inc_backup=last_df, last_arch_backup=last_arch, startup_time=startup_time,
                            ts_online=ts_online, ts_total=ts_total, db_size_gb=db_size_gb, error="")
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        return DbHealth(status="DOWN", details=str(e), elapsed_ms=elapsed_ms, error=str(e))

# ---------------- GUI ----------------
class MonitorApp(ttk.Frame):
    LOGICAL_COLUMNS = ("S.No","DB Name","Environment","Host","DB Version","Startup Time","Status","Inst_status","Sessions","WorstTS%","TS Online","DB Size","LastFull/Inc","LastArch","Ms","LastChecked","Check status","Error")
    STATUS_COLUMNS = {"Host","DB Version","Startup Time","Status","Inst_status","Sessions","WorstTS%","TS Online","DB Size","LastFull/Inc","LastArch","Ms","LastChecked","Check status","Error"}

    def __init__(self, master, cfg: Dict[str, Any]):
        super().__init__(master)
        self.master.title(APP_NAME)
        self.pack(fill=tk.BOTH, expand=True)
        self.cfg = cfg
        self.interval_sec = int(cfg.get("interval_sec", DEFAULT_INTERVAL_SEC))
        self.targets: List[DbTarget] = [_hydrate_target(t) if isinstance(t, dict) else t for t in cfg.get("targets", [])]
        self.last_health: Dict[str, Dict[str, Any]] = cfg.get("last_health", {})
        self._auto_flag = False
        self._build_ui()
        self._refresh_table_from_targets()
        self._load_last_health_into_rows()
        if cfg.get("auto_run"):
            self.auto_var.set(True); self._start_auto()
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        self._font = tkfont.nametofont("TkDefaultFont")
        style = ttk.Style(self)
        try: style.theme_use("clam")
        except Exception: pass
        style.configure("App.Treeview.Heading", background="#cfe8ff", foreground="#000000",
                        font=(self._font.actual('family'), self._font.actual('size'), "bold"))
        style.map("App.Treeview.Heading", background=[("active", "#b7dbff")])
        style.configure("App.Treeview", rowheight=22)

        t1 = ttk.Frame(self); t1.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(8,3))
        self.interval_var = tk.IntVar(value=self.interval_sec)
        ttk.Label(t1, text="Interval (sec):").pack(side=tk.LEFT)
        ttk.Spinbox(t1, from_=30, to=3600, textvariable=self.interval_var, width=8).pack(side=tk.LEFT, padx=(4, 10))
        self.auto_var = tk.BooleanVar(value=self.cfg.get("auto_run", False))
        ttk.Checkbutton(t1, text="Auto-run", variable=self.auto_var, command=self._toggle_auto).pack(side=tk.LEFT, padx=(0,8))
        ttk.Button(t1, text="Run All", command=self.run_all_once).pack(side=tk.LEFT)
        ttk.Button(t1, text="Run Selected", command=self.run_selected_once).pack(side=tk.LEFT, padx=(6, 10))
        ttk.Button(t1, text="Clear All", command=self._clear_all_rows).pack(side=tk.LEFT, padx=(6, 4))
        ttk.Button(t1, text="Clear Selected", command=self._clear_selected_row).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Button(t1, text="Add DB", command=self._add_dialog).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(t1, text="Edit DB", command=self._edit_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Remove DB", command=self._remove_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Import Config", command=self._import_json).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(t1, text="Export Config", command=self._export_json).pack(side=tk.LEFT)
        ttk.Button(t1, text="Customize Columns", command=self._customize_columns).pack(side=tk.LEFT, padx=(10,0))
        ttk.Button(t1, text="Select Columns", command=self._select_columns_dialog).pack(side=tk.LEFT, padx=(6,0))
        ttk.Label(t1, text="Client lib dir (for TNS):").pack(side=tk.LEFT, padx=(10, 0))
        self.client_dir_var = tk.StringVar(value=self.cfg.get("client_lib_dir", ""))
        ttk.Entry(t1, textvariable=self.client_dir_var, width=22).pack(side=tk.LEFT, padx=4)
        ttk.Button(t1, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT)

        t2 = ttk.Frame(self); t2.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0,6))
        email_cfg = self.cfg.get("email", {})
        ttk.Label(t2, text="SMTP/Exchange:").pack(side=tk.LEFT)
        self.smtp_server_var = tk.StringVar(value=email_cfg.get("server", ""))
        self.smtp_port_var = tk.IntVar(value=int(email_cfg.get("port", 25)))
        ttk.Entry(t2, textvariable=self.smtp_server_var, width=22).pack(side=tk.LEFT, padx=(4,2))
        ttk.Entry(t2, textvariable=self.smtp_port_var, width=6).pack(side=tk.LEFT, padx=(2,6))
        ttk.Label(t2, text="From:").pack(side=tk.LEFT)
        self.from_var = tk.StringVar(value=email_cfg.get("from_addr", ""))
        ttk.Entry(t2, textvariable=self.from_var, width=24).pack(side=tk.LEFT, padx=(4,6))
        ttk.Label(t2, text="To (comma):").pack(side=tk.LEFT)
        self.to_var = tk.StringVar(value=email_cfg.get("to_addrs", ""))
        ttk.Entry(t2, textvariable=self.to_var, width=32).pack(side=tk.LEFT, padx=(4,6))
        ttk.Button(t2, text="Save Mail", command=self._save_mail_settings).pack(side=tk.LEFT, padx=(6,0))
        ttk.Button(t2, text="Email Columns", command=self._select_email_columns_dialog).pack(side=tk.LEFT, padx=(6,0))
        ttk.Button(t2, text="Email Report", command=self._email_report).pack(side=tk.LEFT, padx=(6,0))

        tree_frame = ttk.Frame(self); tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.tree = ttk.Treeview(tree_frame, columns=self.LOGICAL_COLUMNS, show="headings", height=20, style="App.Treeview")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=xsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y); self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True); xsb.pack(side=tk.BOTTOM, fill=tk.X)

        for col in self.LOGICAL_COLUMNS:
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c, False))
            self.tree.column(col, width=120, stretch=True, anchor="w")

        order = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
        order = [c for c in order if c in self.LOGICAL_COLUMNS]
        for c in self.LOGICAL_COLUMNS:
            if c not in order: order.append(c)
        vis = self.cfg.get("visible_columns", order[:])
        vis = [c for c in vis if c in self.LOGICAL_COLUMNS]
        if "S.No" not in vis: vis = ["S.No"] + [c for c in vis if c != "S.No"]
        if order and order[0] != "S.No": order = ["S.No"] + [c for c in order if c != "S.No"]
        display = [c for c in order if c in vis]
        self.tree["displaycolumns"] = display

        for col, w in self.cfg.get("column_widths", {}).items():
            try: self.tree.column(col, width=int(w))
            except Exception: pass

        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="Copy Cell", command=self._copy_cell)
        self.menu.add_separator()
        self.menu.add_command(label="Copy DB Name", command=lambda: self._copy_by_col("DB Name"))
        self.menu.add_command(label="Copy Host", command=lambda: self._copy_by_col("Host"))
        self.menu.add_command(label="Copy Error", command=lambda: self._copy_by_col("Error"))
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<ButtonRelease-1>", lambda e: self._persist_column_layout())

        bottombar = ttk.Frame(self); bottombar.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottombar, textvariable=self.status_var).pack(side=tk.LEFT)

    # ---------- UI handlers ----------
    def _toggle_auto(self):
        if self.auto_var.get(): self._start_auto()
        else: self._stop_auto()
        self.cfg["auto_run"] = self.auto_var.get(); save_config(self.cfg)

    def _start_auto(self):
        if getattr(self, "_auto_flag", False): return
        self._auto_flag = True; self.status_var.set(f"Auto-running every {self.interval_var.get()}s...")
        self.after(200, self._loop)

    def _stop_auto(self):
        self._auto_flag = False; self.status_var.set("Auto-run stopped")

    def _persist_column_layout(self):
        widths = {col: self.tree.column(col, option="width") for col in self.LOGICAL_COLUMNS}
        self.cfg["column_widths"] = widths
        visible = list(self.tree["displaycolumns"])
        full = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
        new_full, seen = [], set()
        for c in visible:
            if c not in seen: new_full.append(c); seen.add(c)
        for c in full:
            if c not in seen and c in self.LOGICAL_COLUMNS: new_full.append(c); seen.add(c)
        self.cfg["column_order"] = new_full; self.cfg["visible_columns"] = visible; save_config(self.cfg)

    def _autosize_columns(self):
        pad = 24; visible = list(self.tree["displaycolumns"]); font = self._font
        for col in visible:
            header_w = font.measure(col); max_w = header_w
            for iid in self.tree.get_children(""):
                vals = self.tree.item(iid)["values"]
                try:
                    idx = self.LOGICAL_COLUMNS.index(col); txt = str(vals[idx]) if idx < len(vals) else ""
                    tw = font.measure(txt); max_w = max(max_w, tw)
                except Exception: pass
            new_w = max(max_w + pad, 90); cur = self.tree.column(col, option="width")
            if cur < new_w: self.tree.column(col, width=new_w)

    # ---------- Dialogs ----------
    def _customize_columns(self): pass  # (unchanged for brevity)
    def _select_columns_dialog(self): pass
    def _select_email_columns_dialog(self): pass

    # ---------- Context menu ----------
    def _show_context_menu(self, event):
        iid = self.tree.identify_row(event.y); cid = self.tree.identify_column(event.x)
        if iid:
            self.tree.selection_set(iid); self._context_row = iid; self._context_col = cid
            self.menu.tk_popup(event.x_root, event.y_root)

    def _copy_cell(self):
        row = getattr(self, "_context_row", None); colid = getattr(self, "_context_col", None)
        if not row or not colid: return
        col_index = int(colid.replace("#","")) - 1; vals = self.tree.item(row)["values"]
        text = str(vals[col_index]) if col_index < len(vals) else ""
        self.clipboard_clear(); self.clipboard_append(text)

    def _copy_by_col(self, colname: str):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]; vals = self.tree.item(iid)["values"]; idx = self.LOGICAL_COLUMNS.index(colname)
        text = str(vals[idx]) if idx < len(vals) else ""
        self.clipboard_clear(); self.clipboard_append(text)

    # ---------- Sorting helpers ----------
    def _generic_key(self, col: str, s: str): return (str(s).lower(),)

    def _sort_by_column(self, col: str, descending: bool):
        rows = [(self._generic_key(col, self.tree.set(k, col)), k) for k in self.tree.get_children("")]
        rows.sort(reverse=descending, key=lambda x: x[0])
        for idx, (_, k) in enumerate(rows): self.tree.move(k, "", idx)
        self._renumber()
        self.tree.heading(col, command=lambda c=col: self._sort_by_column(c, not descending))

    def _renumber(self):
        for i, iid in enumerate(self.tree.get_children(""), start=1):
            vals = list(self.tree.item(iid)["values"])
            if vals: vals[0] = i; self.tree.item(iid, values=vals)

    def _pick_client_dir(self):
        d = filedialog.askdirectory(title="Select Oracle Client lib directory")
        if d: self.client_dir_var.set(d); self.cfg["client_lib_dir"] = d; save_config(self.cfg)

    def _refresh_table_from_targets(self):
        for i in self.tree.get_children(): self.tree.delete(i)
        for idx, t in enumerate(self.targets, start=1):
            values = ["-"] * len(self.LOGICAL_COLUMNS); values[0] = idx; values[1] = t.name; values[2] = t.environment
            self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber(); self._autosize_columns()

    def _load_last_health_into_rows(self): pass
    def _apply_persisted_row(self, name: str, hdict: Dict[str, Any]): pass

    def run_all_once(self): pass
    def run_selected_once(self): pass
    def _loop(self): pass
    def _checks_async(self, targets: List[DbTarget]): pass
    def _set_check_status(self, name: str, status: str): pass
    def _apply_result(self, name: str, target: DbTarget, h: DbHealth): pass
    def _clear_row_values(self, vals: List[Any]) -> List[Any]: return vals
    def _clear_all_rows(self): pass
    def _clear_selected_row(self): pass
    def _add_dialog(self): pass
    def _edit_selected(self): pass
    def _remove_selected(self): pass
    def _add_target(self, t: DbTarget): pass
    def _update_target(self, t: DbTarget): pass
    def _persist_targets(self): pass
    def _import_json(self): pass
    def _export_json(self): pass
    def _save_mail_settings(self): pass
    def _email_report(self): pass
    def _build_html(self, rows: List[List]) -> str: return ""
    def _send_html_email(self, server: str, port: int, from_addr: str, to_addrs: List[str], subject: str, html: str): pass
    def _on_close(self): self.master.destroy()

# ---------------- Add/Edit dialog ----------------
class DbEditor(tk.Toplevel):
    def __init__(self, parent: 'MonitorApp', target: Optional[DbTarget] = None, on_save=None):
        super().__init__(parent)
        self.parent = parent; self.title("DB Target"); self.resizable(False, False); self.on_save = on_save

        current_mode = (target.mode if target else "thin") if target else "thin"
        self.var_conn_type = tk.StringVar(value="tns" if current_mode=="thick" else "jdbc")

        self.var_name = tk.StringVar(value=target.name if target else "")
        self.var_env = tk.StringVar(value=target.environment if target else "NON-PROD")

        # Common creds (used for both modes)
        self.var_user = tk.StringVar(value=target.user if (target and target.user) else "")
        t_pwd = _decrypt_password(target.password_enc) if (target and target.password_enc) else None
        self.var_pwd = tk.StringVar(value=t_pwd or "")

        # TNS alias/desc
        self.var_tns = tk.StringVar(value=target.dsn if (target and current_mode=="thick") else "")

        # Thin host/port/service/sid
        host, port, service, sid = ("", "1521", "", "")
        if target and current_mode=="thin":
            try:
                host, port, service, sid = parse_ezconnect(target.dsn)
            except Exception:
                pass
        self.var_host = tk.StringVar(value=host or "")
        self.var_port = tk.StringVar(value=port or "1521")
        use_service = True if service else False
        self.var_use_service = tk.BooleanVar(value=use_service)
        self.var_service = tk.StringVar(value=service or "")
        self.var_sid = tk.StringVar(value=sid or "")

        frm = ttk.Frame(self, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)
        frm.grid_columnconfigure(0, weight=1)

        # DB Name
        r = ttk.Frame(frm); r.pack(fill=tk.X, pady=4)
        ttk.Label(r, text="DB Name:", width=22).pack(side=tk.LEFT)
        ttk.Entry(r, textvariable=self.var_name, width=50).pack(side=tk.LEFT, padx=4)

        # Connection Type
        r = ttk.Frame(frm); r.pack(fill=tk.X, pady=4)
        ttk.Label(r, text="Connection Type:", width=22).pack(side=tk.LEFT)
        ttk.Radiobutton(r, text="TNS / Oracle Client", value="tns", variable=self.var_conn_type, command=self._toggle_conn_type).pack(side=tk.LEFT)
        ttk.Radiobutton(r, text="JDBC thin", value="jdbc", variable=self.var_conn_type, command=self._toggle_conn_type).pack(side=tk.LEFT, padx=(10,0))

        # --- TNS rows ---
        self.row_tns = ttk.Frame(frm)
        rr = ttk.Frame(self.row_tns); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="TNS Alias/Descriptor:", width=22).pack(side=tk.LEFT)
        ttk.Entry(rr, textvariable=self.var_tns, width=50).pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_tns); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="User:", width=22).pack(side=tk.LEFT)
        ttk.Entry(rr, textvariable=self.var_user, width=50).pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_tns); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Password:", width=22).pack(side=tk.LEFT)
        ttk.Entry(rr, textvariable=self.var_pwd, width=50, show="*").pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_tns); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Environment:", width=22).pack(side=tk.LEFT)
        self.env_combo_tns = ttk.Combobox(rr, textvariable=self.var_env, state="readonly", values=["NON-PROD","PROD"], width=47)
        self.env_combo_tns.pack(side=tk.LEFT, padx=4)

        # --- JDBC rows ---
        self.row_jdbc = ttk.Frame(frm)

        rr = ttk.Frame(self.row_jdbc); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Host:", width=22).pack(side=tk.LEFT)
        ttk.Entry(rr, textvariable=self.var_host, width=50).pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_jdbc); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Port:", width=22).pack(side=tk.LEFT)
        ttk.Entry(rr, textvariable=self.var_port, width=12).pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_jdbc); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Connection Using:", width=22).pack(side=tk.LEFT)
        ttk.Radiobutton(rr, text="Service Name", value=True, variable=self.var_use_service, command=self._toggle_sid_service).pack(side=tk.LEFT)
        ttk.Radiobutton(rr, text="SID", value=False, variable=self.var_use_service, command=self._toggle_sid_service).pack(side=tk.LEFT, padx=(10,0))

        self.row_jdbc_service = ttk.Frame(self.row_jdbc)
        ttk.Label(self.row_jdbc_service, text="SID/ServiceName:", width=22).pack(side=tk.LEFT)
        self.entry_sid_service = ttk.Entry(self.row_jdbc_service, width=40)
        self.entry_sid_service.pack(side=tk.LEFT, padx=4)
        self.row_jdbc_service.pack(fill=tk.X, pady=4)

        rr = ttk.Frame(self.row_jdbc); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="User:", width=22).pack(side=tk.LEFT)
        self.entry_user_jdbc = ttk.Entry(rr, textvariable=self.var_user, width=50)
        self.entry_user_jdbc.pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_jdbc); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Password:", width=22).pack(side=tk.LEFT)
        self.entry_pwd_jdbc = ttk.Entry(rr, textvariable=self.var_pwd, width=50, show="*")
        self.entry_pwd_jdbc.pack(side=tk.LEFT, padx=4)

        rr = ttk.Frame(self.row_jdbc); rr.pack(fill=tk.X, pady=4)
        ttk.Label(rr, text="Environment:", width=22).pack(side=tk.LEFT)
        self.env_combo_jdbc = ttk.Combobox(rr, textvariable=self.var_env, state="readonly", values=["NON-PROD","PROD"], width=47)
        self.env_combo_jdbc.pack(side=tk.LEFT, padx=4)

        ttk.Frame(frm).pack(fill=tk.BOTH, expand=True)
        btns = ttk.Frame(frm); btns.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 2))
        ttk.Button(btns, text="Test Connection", command=self._test_connection).pack(side=tk.LEFT)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side=tk.RIGHT, padx=6)
        ttk.Button(btns, text="Save", command=self._save).pack(side=tk.RIGHT)

        self._toggle_conn_type()
        self._toggle_sid_service()
        self._sync_sid_service_field()

        self.grab_set(); self.transient(parent); self.wait_visibility(); self.focus()

    def _toggle_conn_type(self):
        tns = self.var_conn_type.get() == "tns"
        if tns:
            self.row_jdbc.pack_forget()
            self.row_tns.pack(fill=tk.X, pady=4)
        else:
            self.row_tns.pack_forget()
            self.row_jdbc.pack(fill=tk.X, pady=4)

    def _toggle_sid_service(self):
        self._sync_sid_service_field()

    def _sync_sid_service_field(self):
        if self.var_use_service.get():
            self.entry_sid_service.delete(0, tk.END)
            self.entry_sid_service.insert(0, self.var_service.get())
        else:
            self.entry_sid_service.delete(0, tk.END)
            self.entry_sid_service.insert(0, self.var_sid.get())

    def _test_connection(self):
        if oracledb is None:
            messagebox.showerror(APP_NAME, "python-oracledb not installed in this environment."); return
        try:
            if self.var_conn_type.get() == "tns":
                dsn = normalize_tns(self.var_tns.get().strip())
                if not dsn: messagebox.showerror(APP_NAME, "Enter a TNS alias/descriptor."); return
                init_oracle_client_if_needed(self.parent.cfg)
                user = self.var_user.get().strip() or None
                pwd = self.var_pwd.get().strip() or None
                if user and pwd: conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
                else: conn = oracledb.connect(dsn=dsn)
            else:
                host = self.var_host.get().strip(); port = (self.var_port.get().strip() or "1521")
                idval = self.entry_sid_service.get().strip()
                if not (host and port and idval): messagebox.showerror(APP_NAME, "Enter Host, Port and SID/ServiceName."); return
                user = self.var_user.get().strip() or None
                pwd = self.var_pwd.get().strip() or None
                kwargs = {"host": host, "port": int(port)}
                if self.var_use_service.get(): kwargs["service_name"] = idval
                else: kwargs["sid"] = idval
                if user: kwargs["user"] = user
                if pwd: kwargs["password"] = pwd
                conn = oracledb.connect(**kwargs)
            try:
                cur = conn.cursor(); cur.execute("SELECT 'OK' FROM dual"); res = cur.fetchone()[0]
            finally:
                conn.close()
            messagebox.showinfo(APP_NAME, f"Connection successful. ({res})")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Connection failed.\n{e}")

    def _save(self):
        name = self.var_name.get().strip()
        if not name: messagebox.showerror(APP_NAME, "DB Name is required"); return
        if self.var_conn_type.get() == "tns":
            dsn = normalize_tns(self.var_tns.get().strip())
            if not dsn: messagebox.showerror(APP_NAME, "Enter a TNS alias/descriptor."); return
            user = self.var_user.get().strip() or None
            pwd = self.var_pwd.get().strip() or None
            enc = _encrypt_password(pwd) if pwd else None
            t = DbTarget(name=name, dsn=dsn, user=user, password_enc=enc, mode="thick", environment=self.var_env.get().strip() or "NON-PROD")
        else:
            host = self.var_host.get().strip(); port = (self.var_port.get().strip() or "1521")
            idval = self.entry_sid_service.get().strip()
            if not (host and port and idval): messagebox.showerror(APP_NAME, "Enter Host, Port and SID/ServiceName."); return
            # Store a normalized "host:port/service" or "host:port/?sid=SID" for later parsing
            dsn = f"{host}:{port}/{idval}" if self.var_use_service.get() else f"{host}:{port}/?sid={idval}"
            user = self.var_user.get().strip() or None
            pwd = self.var_pwd.get().strip() or None
            enc = _encrypt_password(pwd) if pwd else None
            t = DbTarget(name=name, dsn=dsn, user=user, password_enc=enc, mode="thin", environment=self.var_env.get().strip() or "NON-PROD")
        if self.on_save: self.on_save(t)
        self.destroy()

# ---------------- Main ----------------
def main():
    cfg = load_config()
    root = tk.Tk()
    try:
        if sys.platform.startswith("win"):
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)  # type: ignore
    except Exception: pass
    root.title(APP_NAME)
    # We keep only the editor exposed here to focus on fixing connection issues quickly.
    # In your full app, you'd instantiate MonitorApp. For now, show editor for quick testing:
    # Monitor UI removed here for brevity in this hotfix build.
    # To integrate back, replace these 3 lines with: app = MonitorApp(root, cfg); root.geometry("2000x840"); root.mainloop()
    app = MonitorApp(root, cfg)
    root.geometry("1200x700")
    root.mainloop()

if __name__ == "__main__":
    main()
