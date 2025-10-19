#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Server module for Database Pulse.

- Similar layout and email report as Oracle module
- "Add Instance", "Edit Instance", "Remove Instance"
- Connectivity via sqlcmd (path selectable in toolbar; persisted to config/sqlserver_config.json)
- Columns: S.No, SQL Server Instance, Environment, Version, CU, Instance Status, Agent Status,
           DB Status (total/online), Last Full Backup (oldest of last full among DBs),
           Disk Size % (per drive; red if any >= 90%), Last Checked, Check Status, Error
- Auth: Windows (uses current Windows credentials: -E) or SQL Server auth (-U/-P)
"""

import base64
import json
import locale
import os
import subprocess
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

APP_NAME = "Database Pulse"
APP_VERSION = "Database Pulse v1.0"

GOOD = "✅"
BAD = "❌"
DEFAULT_INTERVAL_SEC = 300  # 5 min

# ---------------- Paths / config ----------------
def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

CONFIG_DIR = (_base_dir() / "config")
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "sqlserver_config.json"


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


# ---------------- Data model ----------------
@dataclass
class InstanceTarget:
    """Represents a SQL Server instance connection"""
    name: str                 # Display name for the instance row
    server: str               # e.g., "HOST\\INSTANCE" or "host,1433"
    environment: str = "NON-PROD"
    auth: str = "windows"     # "windows" or "sql"
    username: Optional[str] = None   # for SQL auth
    password_enc: Optional[str] = None  # for SQL auth
    win_user_display: Optional[str] = None  # optional display like DOMAIN\\User

@dataclass
class InstanceHealth:
    instance_status: str = "-"  # Running/Stopped/Unknown
    agent_status: str = "-"     # Running/Stopped/Unknown
    version_year: str = "-"     # 2019/2022 etc.
    cu: str = "-"               # CU label or "-"
    db_total: int = 0
    db_online: int = 0
    oldest_full_backup: Optional[str] = None  # "YYYY-MM-DD HH:MM:SS"
    disk_usages: List[Tuple[str, float]] = field(default_factory=list)  # [(mount, used_pct)]
    elapsed_ms: int = 0
    error: str = ""
    ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# ---------------- Config helpers ----------------
def logical_columns() -> List[str]:
    return [
        "S.No",
        "SQL Server Instance",
        "Environment",
        "Version",
        "CU",
        "Instance Status",
        "Agent Status",
        "DB Status",
        "Last Full Backup",
        "Disk Size %",
        "Last Checked",
        "Check Status",
        "Error",
    ]

def default_config() -> Dict[str, Any]:
    cols = logical_columns()
    return {
        "interval_sec": DEFAULT_INTERVAL_SEC,
        "instances": [],
        "sqlcmd_path": "",  # path to sqlcmd.exe
        "email": {
            "server": "",
            "port": 25,
            "from_addr": "",
            "to_addrs": "",
            "subject": "SQL Server Health Report",
        },
        "last_health": {},
        "auto_run": False,
        "column_order": cols[:],
        "visible_columns": cols[:],
        "email_columns": cols[:],
        "column_widths": {},
    }

def _serialize_inst(i: InstanceTarget) -> Dict[str, Any]:
    return {
        "name": i.name,
        "server": i.server,
        "environment": i.environment,
        "auth": i.auth,
        "username": i.username,
        "password_enc": i.password_enc,
        "win_user_display": i.win_user_display,
    }

def _hydrate_inst(d: Dict[str, Any]) -> InstanceTarget:
    return InstanceTarget(
        name=d.get("name", ""),
        server=d.get("server", ""),
        environment=d.get("environment", "NON-PROD"),
        auth=d.get("auth", "windows"),
        username=d.get("username"),
        password_enc=d.get("password_enc"),
        win_user_display=d.get("win_user_display"),
    )

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
            base.setdefault("column_order", default_config()["column_order"])
            base.setdefault("visible_columns", default_config()["visible_columns"])
            base.setdefault("email_columns", default_config()["email_columns"])
            base.setdefault("column_widths", {})
            if base.get("instances"):
                base["instances"] = [_serialize_inst(_hydrate_inst(i)) for i in base["instances"]]
            return base
        except Exception:
            pass
    return default_config()

def save_config(cfg: Dict[str, Any]):
    out = dict(cfg)
    out["instances"] = [
        _serialize_inst(_hydrate_inst(i) if isinstance(i, dict) else i) for i in cfg.get("instances", [])
    ]
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)


# ---------------- sqlcmd helpers ----------------
def build_sqlcmd_command(sqlcmd_path: str, server: str, auth: str, username: Optional[str], password: Optional[str],
                         query: str, login_timeout: int = 10, query_timeout: int = 30) -> List[str]:
    exe = sqlcmd_path.strip() or "sqlcmd"
    cmd = [exe, "-S", server, "-W", "-h", "-1", "-s", "|", "-b", "-l", str(login_timeout), "-Q", query]
    if auth == "windows":
        cmd.insert(2, "-E")
    else:
        if not username or password is None:
            raise ValueError("SQL authentication requires username and password.")
        cmd.insert(2, "-U")
        cmd.insert(3, username)
        cmd.insert(4, "-P")
        cmd.insert(5, password)
    cmd.extend(["-t", str(query_timeout)])
    return cmd

def run_sqlcmd(cmd: List[str]) -> Tuple[int, str, str]:
    enc = locale.getpreferredencoding(False) or "utf-8"
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        out_b, err_b = p.communicate(timeout=max(5, int(cmd[-1])) + 5 if cmd[-2] == "-t" else 40)
        out = out_b.decode(enc, errors="ignore").strip()
        err = err_b.decode(enc, errors="ignore").strip()
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        try:
            p.kill()
        except Exception:
            pass
        return 1, "", "sqlcmd timeout"
    except FileNotFoundError:
        return 2, "", "sqlcmd executable not found"
    except Exception as e:
        return 3, "", f"{e}"

def parse_scalar_list(out: str) -> List[List[str]]:
    rows = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        rows.append(parts)
    return rows


# ---------------- Queries ----------------
def q_version_and_cu() -> str:
    return (
        "SET NOCOUNT ON; "
        "SELECT "
        "CONVERT(varchar(10), SERVERPROPERTY('ProductMajorVersion')) AS major, "
        "ISNULL(CONVERT(varchar(20), SERVERPROPERTY('ProductUpdateLevel')), '-') AS cu;"
    )

def q_services() -> str:
    return (
        "SET NOCOUNT ON; "
        "SELECT servicename, status_desc "
        "FROM sys.dm_server_services;"
    )

def q_db_status() -> str:
    return (
        "SET NOCOUNT ON; "
        "SELECT COUNT(*) AS total, SUM(CASE WHEN state_desc='ONLINE' THEN 1 ELSE 0 END) AS online "
        "FROM sys.databases WHERE database_id > 4;"
    )

def q_oldest_full_backup() -> str:
    return (
        "SET NOCOUNT ON; "
        "WITH last_full AS ( "
        "  SELECT d.name, MAX(b.backup_finish_date) AS last_full "
        "  FROM sys.databases d "
        "  LEFT JOIN msdb.dbo.backupset b ON b.database_name = d.name AND b.type = 'D' "
        "  WHERE d.database_id > 4 "
        "  GROUP BY d.name "
        ") "
        "SELECT CONVERT(varchar(19), MIN(last_full), 120) AS oldest_full FROM last_full;"
    )

def q_disk_usage() -> str:
    return (
        "SET NOCOUNT ON; "
        "SELECT v.volume_mount_point, "
        "CAST(MAX((1.0 - v.available_bytes*1.0/v.total_bytes)*100.0) AS decimal(5,1)) AS used_pct "
        "FROM sys.master_files mf "
        "CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) v "
        "GROUP BY v.volume_mount_point;"
    )


def map_major_to_year(major: str) -> str:
    try:
        m = int(major)
    except Exception:
        return major
    return {
        16: "2022",
        15: "2019",
        14: "2017",
        13: "2016",
        12: "2014",
        11: "2012",
    }.get(m, str(m))


# ---------------- UI ----------------
class SqlServerMonitorApp(ttk.Frame):
    LOGICAL_COLUMNS = tuple(logical_columns())
    STATUS_COLUMNS = {
        "Version", "CU", "Instance Status", "Agent Status", "DB Status", "Last Full Backup", "Disk Size %",
        "Last Checked", "Check Status", "Error"
    }

    def __init__(self, master):
        super().__init__(master)
        self.cfg = load_config()
        self.interval_sec = int(self.cfg.get("interval_sec", DEFAULT_INTERVAL_SEC))
        self.instances: List[InstanceTarget] = [_hydrate_inst(i) if isinstance(i, dict) else i for i in self.cfg.get("instances", [])]
        self.last_health: Dict[str, Dict[str, Any]] = self.cfg.get("last_health", {})
        self._auto_flag = False
        self._build_ui()
        self._refresh_table_from_instances()
        self._load_last_health_into_rows()
        if self.cfg.get("auto_run"):
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
        style.configure("SS.Treeview.Heading", background="#cfe8ff", foreground="#000000",
                        font=(self._font.actual("family"), self._font.actual("size"), "bold"))
        style.map("SS.Treeview.Heading", background=[("active", "#b7dbff")])
        style.configure("SS.Treeview", rowheight=22)

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
        ttk.Button(t1, text="Add Instance", command=self._add_dialog).pack(side=tk.LEFT, padx=(10,0))
        ttk.Button(t1, text="Edit Instance", command=self._edit_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Remove Instance", command=self._remove_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Import Config", command=self._import_json).pack(side=tk.LEFT, padx=(10,0))
        ttk.Button(t1, text="Export Config", command=self._export_json).pack(side=tk.LEFT)

        # Add column customization + SQLCMD location to toolbar (beside Export)
        ttk.Separator(t1, orient="vertical").pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(t1, text="Customize Columns", command=self._customize_columns).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(t1, text="Select Columns", command=self._select_columns_dialog).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(t1, text="SQLCMD Location:").pack(side=tk.LEFT, padx=(0, 4))
        self.sqlcmd_path_var = tk.StringVar(value=self.cfg.get("sqlcmd_path", ""))
        ttk.Entry(t1, textvariable=self.sqlcmd_path_var, width=28).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(t1, text="Browse", command=self._pick_sqlcmd).pack(side=tk.LEFT)

        # Email bar (row 1)
        t2 = ttk.Frame(self)
        t2.grid(row=1, column=0, sticky="ew", padx=8, pady=(0,6))
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
        ttk.Button(t2, text="Save Mail", command=self._save_mail_settings).pack(side=tk.LEFT, padx=(6,0))
        ttk.Button(t2, text="Email Columns", command=self._select_email_columns_dialog).pack(side=tk.LEFT, padx=(6,0))
        ttk.Button(t2, text="Email Report", command=self._email_report).pack(side=tk.LEFT, padx=(6,0))

        # Table (row 2)
        tree_frame = ttk.Frame(self)
        tree_frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=8)
        self.tree = ttk.Treeview(tree_frame, columns=self.LOGICAL_COLUMNS, show="headings", height=20, style="SS.Treeview")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=xsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)

        for col in self.LOGICAL_COLUMNS:
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c, False))
            self.tree.column(col, width=120, stretch=True, anchor="w")

        # Use saved column order/visibility
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
        self.menu.add_command(label="Copy Instance", command=lambda: self._copy_by_col("SQL Server Instance"))
        self.menu.add_command(label="Copy Error", command=lambda: self._copy_by_col("Error"))
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<ButtonRelease-1>", lambda e: self._persist_column_layout())

        # Bottom status
        bottom = ttk.Frame(self)
        bottom.grid(row=3, column=0, sticky="ew", padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottom, textvariable=self.status_var).pack(side=tk.LEFT)

    # ---------- Helpers ----------
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
        self._checks_async(self.instances)
        self.after(self.interval_var.get() * 1000, self._loop)

    def _pick_sqlcmd(self):
        p = filedialog.askopenfilename(title="Locate sqlcmd.exe", filetypes=[("sqlcmd", "sqlcmd.exe"), ("All", "*.*")])
        if p:
            self.sqlcmd_path_var.set(p)
            self.cfg["sqlcmd_path"] = p
            save_config(self.cfg)

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

    # Sorting
    def _parse_db_status(self, s: str) -> Tuple[int, int]:
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
            numbers = []
            for tok in str(s).replace("%", "").replace(";", " ").split():
                try:
                    numbers.append(float(tok))
                except:
                    pass
            return max(numbers) if numbers else -1.0
        except Exception:
            return -1.0

    def _parse_datecell(self, s: str) -> float:
        t = str(s).strip()
        if not t or t == "-":
            return float("-inf")
        try:
            dt = datetime.strptime(t.split()[-2] + " " + t.split()[-1], "%Y-%m-%d %H:%M:%S")
            return dt.timestamp()
        except Exception:
            try:
                dt = datetime.strptime(t.split()[-1], "%Y-%m-%d")
                return dt.timestamp()
            except Exception:
                return float("-inf")

    def _status_rank(self, s: str) -> int:
        return 1 if str(s).strip().startswith(GOOD) else 0

    def _generic_key(self, col: str, s: str):
        if col == "S.No":
            try:
                return (int(s),)
            except:
                return (0,)
        if col in ("Instance Status", "Agent Status"):
            return (self._status_rank(s), s)
        if col == "DB Status":
            a, b = self._parse_db_status(s)
            return (a/b if b else -1.0, a, b)
        if col == "Disk Size %":
            return (self._parse_pct(s),)
        if col in ("Last Full Backup", "Last Checked"):
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

    # CRUD / Config
    def _refresh_table_from_instances(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for idx, t in enumerate(self.instances, start=1):
            values = ["-"] * len(self.LOGICAL_COLUMNS)
            values[0] = idx
            values[1] = t.name
            values[2] = t.environment
            self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber()
        self._autosize_columns()

    def _load_last_health_into_rows(self):
        for t in self.instances:
            hdict = self.last_health.get(t.name)
            if not hdict:
                continue
            self._apply_persisted_row(t.name, hdict)
        self._autosize_columns()

    def _apply_persisted_row(self, name: str, h: Dict[str, Any]):
        vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
        def mark(ok: bool) -> str:
            return GOOD if ok else BAD

        inst_ok = h.get("instance_status", "").upper().startswith("RUN")
        agent_ok = h.get("agent_status", "").upper().startswith("RUN")
        db_total = int(h.get("db_total", 0) or 0)
        db_online = int(h.get("db_online", 0) or 0)
        db_ok = (db_total == db_online) and db_total > 0
        disks = h.get("disk_usages", [])
        max_used = max([float(x[1]) for x in disks], default=-1.0)
        disks_str = "; ".join([f"{mnt} {pct:.1f}%" for mnt, pct in disks]) if disks else "-"
        disks_ok = (max_used < 90.0) if max_used >= 0 else False

        colidx = {c: i for i, c in enumerate(self.LOGICAL_COLUMNS)}
        vals[colidx["Version"]] = h.get("version_year", "-")
        vals[colidx["CU"]] = h.get("cu", "-")
        vals[colidx["Instance Status"]] = f"{mark(inst_ok)} {h.get('instance_status','-')}"
        vals[colidx["Agent Status"]] = f"{mark(agent_ok)} {h.get('agent_status','-')}"
        vals[colidx["DB Status"]] = f"{mark(db_ok)} {db_online}/{db_total}" if db_total else f"{BAD} 0/0"
        vals[colidx["Last Full Backup"]] = h.get("oldest_full_backup", "-")
        vals[colidx["Disk Size %"]] = f"{mark(disks_ok)} {disks_str}" if disks_str != "-" else f"{BAD} -"
        vals[colidx["Last Checked"]] = h.get("ts", "-")
        vals[colidx["Check Status"]] = "Complete"
        vals[colidx["Error"]] = h.get("error", "")
        self.tree.item(name, values=vals)

    def _persist_instances(self):
        self.cfg["interval_sec"] = self.interval_var.get()
        self.cfg["instances"] = [_serialize_inst(i) for i in self.instances]
        self.cfg["sqlcmd_path"] = self.sqlcmd_path_var.get().strip()
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
            self.interval_var.set(int(self.cfg.get("interval_sec", DEFAULT_INTERVAL_SEC)))
            self.sqlcmd_path_var.set(self.cfg.get("sqlcmd_path", ""))
            self.auto_var.set(bool(self.cfg.get("auto_run", False)))
            self.instances = [_hydrate_inst(i) for i in self.cfg.get("instances", [])]
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
            self._refresh_table_from_instances()
            self._load_last_health_into_rows()
            messagebox.showinfo(APP_NAME, "Imported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to import: {e}")

    def _export_json(self):
        p = filedialog.asksaveasfilename(title="Export config", defaultextension=".json", initialfile="sqlserver_config.json")
        if not p:
            return
        try:
            export = {
                "interval_sec": self.interval_var.get(),
                "instances": [_serialize_inst(i) for i in self.instances],
                "sqlcmd_path": self.sqlcmd_path_var.get().strip(),
                "email": {
                    "server": self.smtp_server_var.get().strip(),
                    "port": int(self.smtp_port_var.get() or 25),
                    "from_addr": self.from_var.get().strip(),
                    "to_addrs": self.to_var.get().strip(),
                    "subject": self.cfg.get("email", {}).get("subject", "SQL Server Health Report"),
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

    # Email settings
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

    def _build_html(self, rows: List[List]) -> str:
        headers = [c for c in self.cfg.get("email_columns", list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
        if not headers:
            headers = list(self.LOGICAL_COLUMNS)

        def cell_style(text: str, col: str) -> str:
            ok = None
            t = str(text).strip()
            if col in ("Instance Status", "Agent Status", "DB Status", "Disk Size %"):
                if t.startswith(GOOD):
                    ok = True
                elif t.startswith(BAD):
                    ok = False
            if col == "Disk Size %":
                try:
                    nums = [float(x.replace('%','')) for x in t.replace(GOOD,'').replace(BAD,'').split() if x.replace('.','',1).isdigit()]
                    ok = (max(nums) < 90.0) if nums else False
                except Exception:
                    pass
            if ok is True:
                return "background-color:#e6ffe6;color:#064b00;font-weight:bold;"
            if ok is False:
                return "background-color:#ffe6e6;color:#7a0000;font-weight:bold;"
            return ""

        thead = "<tr>" + "".join(f"<th style='padding:6px 10px;border-bottom:1px solid #ccc;text-align:left'>{h}</th>" for h in headers) + "</tr>"
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
        table = "<table style='border-collapse:collapse;font-family:Segoe UI,Arial,sans-serif;font-size:12px'>" + thead + "".join(body_rows) + "</table>"
        title = f"<h3>SQL Server Health Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h3>"
        return "<html><body>" + title + table + "</body></html>"

    def _send_html_email(self, server: str, port: int, from_addr: str, to_addrs: List[str], subject: str, html: str):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        part = MIMEText(html, "html", "utf-8")
        msg.attach(part)
        import smtplib
        with smtplib.SMTP(server, port, timeout=20) as s:
            s.sendmail(from_addr, to_addrs, msg.as_string())

    def _email_report(self):
        email_cfg = self.cfg.get("email", {})
        server = self.smtp_server_var.get().strip() or email_cfg.get("server", "")
        port = int(self.smtp_port_var.get() or email_cfg.get("port", 25))
        from_addr = self.from_var.get().strip() or email_cfg.get("from_addr", "")
        to_addrs = self.to_var.get().strip() or email_cfg.get("to_addrs", "")
        subject = email_cfg.get("subject", "SQL Server Health Report")
        if not (server and from_addr and to_addrs):
            messagebox.showerror(APP_NAME, "Set SMTP server, From, and To addresses first.")
            return
        rows = [self.tree.item(i)["values"] for i in self.tree.get_children("")]
        html = self._build_html(rows)
        try:
            self._send_html_email(server, port, from_addr, [x.strip() for x in to_addrs.split(",") if x.strip()], subject, html)
            messagebox.showinfo(APP_NAME, "Email report sent.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to send email: {e}")

    def _select_email_columns_dialog(self):
        dlg = tk.Toplevel(self)
        dlg.title("Select Email Columns")
        dlg.geometry("360x520")
        ttk.Label(dlg, text="Choose which columns to include in the email report:").pack(pady=6)

        current_email_cols = list(self.cfg.get("email_columns", list(self.LOGICAL_COLUMNS)))
        vars_by_col = {}
        box = ttk.Frame(dlg); box.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)

        helper = ttk.Frame(dlg); helper.pack(fill=tk.X, padx=10, pady=(0,6))
        def set_all(state: bool):
            for v in vars_by_col.values():
                v.set(state)
        ttk.Button(helper, text="Select All", command=lambda: set_all(True)).pack(side=tk.LEFT, padx=(0,6))
        ttk.Button(helper, text="Clear All", command=lambda: set_all(False)).pack(side=tk.LEFT)

        for col in self.LOGICAL_COLUMNS:
            v = tk.BooleanVar(value=(col in current_email_cols))
            vars_by_col[col] = v
            ttk.Checkbutton(box, text=col, variable=v).pack(anchor="w")

        def apply_and_close():
            current_display = list(self.tree["displaycolumns"])
            order_ref = current_display if current_display else list(self.LOGICAL_COLUMNS)
            selected = [c for c in order_ref if vars_by_col.get(c, tk.BooleanVar(value=True)).get()]
            self.cfg["email_columns"] = selected
            save_config(self.cfg)
            messagebox.showinfo(APP_NAME, f"Email columns updated ({len(selected)} selected).")
            dlg.destroy()

        ttk.Button(dlg, text="Apply", command=apply_and_close).pack(pady=8)

    def _customize_columns(self):
        dlg = tk.Toplevel(self)
        dlg.title("Customize Columns (Order)")
        dlg.geometry("380x420")
        ttk.Label(dlg, text="Reorder columns (S.No is fixed at first):").pack(pady=6)
        current = [c for c in self.tree["displaycolumns"] if c != "S.No"]
        var_list = tk.Variable(value=current)
        lb = tk.Listbox(dlg, listvariable=var_list, selectmode=tk.SINGLE, height=16)
        lb.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        btns = ttk.Frame(dlg); btns.pack(fill=tk.X, padx=10, pady=6)
        def move(offset: int):
            sel = lb.curselection()
            if not sel: return
            i = sel[0]; j = i + offset
            if j < 0 or j >= lb.size(): return
            items = list(lb.get(0, tk.END)); items[i], items[j] = items[j], items[i]
            var_list.set(items); lb.selection_clear(0, tk.END); lb.selection_set(j); lb.activate(j)
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
        dlg.geometry("360x520")
        ttk.Label(dlg, text="Choose which columns to display (S.No always visible):").pack(pady=6)
        current_visible = list(self.tree["displaycolumns"]) or list(self.LOGICAL_COLUMNS)
        box = ttk.Frame(dlg); box.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        vars_by_col: Dict[str, tk.BooleanVar] = {}
        helper = ttk.Frame(dlg); helper.pack(fill=tk.X, padx=10, pady=(0,6))
        def set_all(state: bool):
            for v in vars_by_col.values():
                v.set(state)
        ttk.Button(helper, text="Select All", command=lambda: set_all(True)).pack(side=tk.LEFT, padx=(0,6))
        ttk.Button(helper, text="Clear All", command=lambda: set_all(False)).pack(side=tk.LEFT)

        for col in self.LOGICAL_COLUMNS:
            if col == "S.No":
                ttk.Label(box, text="S.No (always visible)").pack(anchor="w")
                continue
            v = tk.BooleanVar(value=(col in current_visible))
            vars_by_col[col] = v
            ttk.Checkbutton(box, text=col, variable=v).pack(anchor="w")

        def apply_and_close():
            order = self.cfg.get("column_order", list(self.LOGICAL_COLUMNS))
            order = [c for c in order if c in self.LOGICAL_COLUMNS]
            if order and order[0] != "S.No":
                order = ["S.No"] + [c for c in order if c != "S.No"]
            visible = ["S.No"] + [c for c in order if c != "S.No" and vars_by_col.get(c, tk.BooleanVar(value=True)).get()]
            if not visible:
                visible = ["S.No"]
            self.tree["displaycolumns"] = visible
            self.cfg["visible_columns"] = visible
            save_config(self.cfg)
            try:
                self._autosize_columns()
            except Exception:
                pass
            dlg.destroy()

        ttk.Button(dlg, text="Apply", command=apply_and_close).pack(pady=8)

    # ---------- Run checks ----------
    def run_all_once(self):
        self._checks_async(self.instances)

    def run_selected_once(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row (Instance) to run.")
            return
        name = sel[0]
        inst = next((x for x in self.instances if x.name == name), None)
        if not inst:
            messagebox.showerror(APP_NAME, "Selected instance not found.")
            return
        self._checks_async([inst])

    def _set_check_status(self, name: str, status: str):
        if name in self.tree.get_children():
            vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
            idx = self.LOGICAL_COLUMNS.index("Check Status")
            if len(vals) <= idx:
                vals += [""] * (idx + 1 - len(vals))
            vals[idx] = status
            self.tree.item(name, values=vals)

    def _checks_async(self, instances: List[InstanceTarget]):
        for t in instances:
            self._set_check_status(t.name, "In Progress")

        def job(inst: InstanceTarget):
            res = self._check_one(inst)
            self.after(0, lambda n=inst.name, i=inst, h=res: self._apply_result(n, i, h))

        for inst in instances:
            threading.Thread(target=job, args=(inst,), daemon=True).start()

    def _check_one(self, inst: InstanceTarget) -> InstanceHealth:
        t0 = time.time()
        h = InstanceHealth()
        sqlcmd_path = self.sqlcmd_path_var.get().strip() or self.cfg.get("sqlcmd_path", "")
        try:
            # Version & CU
            cmd = build_sqlcmd_command(sqlcmd_path, inst.server, inst.auth, inst.username, _decrypt_password(inst.password_enc),
                                       q_version_and_cu())
            rc, out, err = run_sqlcmd(cmd)
            if rc == 0:
                rows = parse_scalar_list(out)
                if rows:
                    major, cu = rows[0][0], rows[0][1] if len(rows[0]) > 1 else "-"
                    h.version_year = map_major_to_year(major)
                    h.cu = cu or "-"
            else:
                raise RuntimeError(err or "version/CU query failed")

            # Services
            cmd = build_sqlcmd_command(sqlcmd_path, inst.server, inst.auth, inst.username, _decrypt_password(inst.password_enc),
                                       q_services())
            rc, out, err = run_sqlcmd(cmd)
            if rc == 0:
                rows = parse_scalar_list(out)
                for r in rows:
                    if len(r) < 2:
                        continue
                    svc, status = r[0], r[1]
                    svc_u = svc.upper()
                    if "SQL SERVER AGENT" in svc_u:
                        h.agent_status = status
                    elif "SQL SERVER" in svc_u:
                        h.instance_status = status

            # DB status
            cmd = build_sqlcmd_command(sqlcmd_path, inst.server, inst.auth, inst.username, _decrypt_password(inst.password_enc),
                                       q_db_status())
            rc, out, err = run_sqlcmd(cmd)
            if rc == 0:
                rows = parse_scalar_list(out)
                if rows and len(rows[0]) >= 2:
                    h.db_total = int(rows[0][0] or 0)
                    h.db_online = int(rows[0][1] or 0)

            # Oldest full backup
            cmd = build_sqlcmd_command(sqlcmd_path, inst.server, inst.auth, inst.username, _decrypt_password(inst.password_enc),
                                       q_oldest_full_backup())
            rc, out, err = run_sqlcmd(cmd)
            if rc == 0:
                rows = parse_scalar_list(out)
                if rows and rows[0] and rows[0][0]:
                    h.oldest_full_backup = rows[0][0]

            # Disk usage
            cmd = build_sqlcmd_command(sqlcmd_path, inst.server, inst.auth, inst.username, _decrypt_password(inst.password_enc),
                                       q_disk_usage())
            rc, out, err = run_sqlcmd(cmd)
            if rc == 0:
                rows = parse_scalar_list(out)
                usages = []
                for r in rows:
                    if len(r) >= 2:
                        mnt = r[0] or "-"
                        try:
                            pct = float(r[1])
                        except Exception:
                            continue
                        usages.append((mnt, pct))
                h.disk_usages = usages

            h.elapsed_ms = int((time.time() - t0) * 1000)
            return h
        except Exception as e:
            h.error = str(e)
            h.elapsed_ms = int((time.time() - t0) * 1000)
            return h

    def _apply_result(self, name: str, inst: InstanceTarget, h: InstanceHealth):
        def mark(ok: bool) -> str:
            return GOOD if ok else BAD

        inst_ok = h.instance_status.upper().startswith("RUN")
        agent_ok = h.agent_status.upper().startswith("RUN")
        db_ok = (h.db_total == h.db_online) and h.db_total > 0
        disks_str = "; ".join([f"{mnt} {pct:.1f}%" for mnt, pct in h.disk_usages]) if h.disk_usages else "-"
        max_used = max([pct for _, pct in h.disk_usages], default=-1.0)
        disks_ok = (max_used < 90.0) if max_used >= 0 else False

        vals = list(self.tree.item(name)["values"] or ["-"] * len(self.LOGICAL_COLUMNS))
        colidx = {c: i for i, c in enumerate(self.LOGICAL_COLUMNS)}
        vals[colidx["Version"]] = h.version_year or "-"
        vals[colidx["CU"]] = h.cu or "-"
        vals[colidx["Instance Status"]] = f"{mark(inst_ok)} {h.instance_status or '-'}"
        vals[colidx["Agent Status"]] = f"{mark(agent_ok)} {h.agent_status or '-'}"
        vals[colidx["DB Status"]] = f"{mark(db_ok)} {h.db_online}/{h.db_total}" if h.db_total else f"{BAD} 0/0"
        vals[colidx["Last Full Backup"]] = h.oldest_full_backup or "-"
        vals[colidx["Disk Size %"]] = f"{mark(disks_ok)} {disks_str}" if disks_str != "-" else f"{BAD} -"
        vals[colidx["Last Checked"]] = h.ts
        vals[colidx["Check Status"]] = "Complete"
        vals[colidx["Error"]] = h.error or ""
        self.tree.item(name, values=vals)

        # persist
        self.last_health[name] = {
            "version_year": h.version_year, "cu": h.cu, "instance_status": h.instance_status, "agent_status": h.agent_status,
            "db_total": h.db_total, "db_online": h.db_online, "oldest_full_backup": h.oldest_full_backup or "-",
            "disk_usages": h.disk_usages, "ts": h.ts, "error": h.error or ""
        }
        self.cfg["last_health"] = self.last_health
        save_config(self.cfg)

        self._renumber()
        self._autosize_columns()

    def _clear_all_rows(self):
        for iid in self.tree.get_children(""):
            vals = list(self.tree.item(iid)["values"])
            cleared = list(vals)
            for c in self.STATUS_COLUMNS:
                idx = self.LOGICAL_COLUMNS.index(c)
                cleared[idx] = "-"
            self.tree.item(iid, values=cleared)
        self.status_var.set("Cleared all rows (except S.No, SQL Server Instance, Environment).")

    def _clear_selected_row(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row to clear.")
            return
        iid = sel[0]
        vals = list(self.tree.item(iid)["values"])
        cleared = list(vals)
        for c in self.STATUS_COLUMNS:
            idx = self.LOGICAL_COLUMNS.index(c)
            cleared[idx] = "-"
        self.tree.item(iid, values=cleared)
        self.status_var.set(f"Cleared row: {iid}")

    # ---------- CRUD dialogs ----------
    def _add_dialog(self):
        InstanceEditor(self, on_save=self._add_instance)

    def _edit_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row to edit.")
            return
        name = sel[0]
        t = next((x for x in self.instances if x.name == name), None)
        if not t:
            messagebox.showerror(APP_NAME, "Instance not found.")
            return
        InstanceEditor(self, target=t, on_save=self._update_instance)

    def _remove_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        name = sel[0]
        self.instances = [i for i in self.instances if i.name != name]
        self.tree.delete(name)
        self._persist_instances()
        self._renumber()

    def _add_instance(self, i: InstanceTarget):
        if any(x.name == i.name for x in self.instances):
            messagebox.showerror(APP_NAME, "An instance with this name already exists.")
            return
        self.instances.append(i)
        self._persist_instances()
        values = ["-"] * len(self.LOGICAL_COLUMNS)
        values[0] = len(self.instances)
        values[1] = i.name
        values[2] = i.environment
        self.tree.insert("", tk.END, iid=i.name, values=tuple(values))
        self._renumber()
        self._autosize_columns()

    def _update_instance(self, i: InstanceTarget):
        found = False
        for idx, cur in enumerate(self.instances):
            if cur.name == i.name:
                self.instances[idx] = i
                found = True
                break
        if not found:
            self.instances.append(i)
        self._persist_instances()
        if i.name in self.tree.get_children(""):
            vals = list(self.tree.item(i.name)["values"])
            vals[1] = i.name
            vals[2] = i.environment
            self.tree.item(i.name, values=vals)
        self._autosize_columns()


# ---------- Instance Editor ----------
class InstanceEditor(tk.Toplevel):
    def __init__(self, app: SqlServerMonitorApp, target: Optional[InstanceTarget] = None, on_save=None):
        super().__init__(app)
        self.app = app
        self.on_save = on_save
        self.title("Add / Edit SQL Server Instance")
        self.resizable(False, False)

        self.name_var = tk.StringVar(value=target.name if target else "")
        self.env_var = tk.StringVar(value=target.environment if target else "NON-PROD")
        self.server_var = tk.StringVar(value=target.server if target else "")
        self.auth_var = tk.StringVar(value=target.auth if target else "windows")
        self.user_var = tk.StringVar(value=target.username if target else "")
        self.pass_var = tk.StringVar(value=_decrypt_password(target.password_enc) if target and target.password_enc else "")
        self.win_user_display = tk.StringVar(value=target.win_user_display if target and target.win_user_display else "")

        body = ttk.Frame(self, padding=10)
        body.pack(fill=tk.BOTH, expand=True)

        row = 0
        ttk.Label(body, text="Instance Name (display):").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(body, textvariable=self.name_var, width=34).grid(row=row, column=1, columnspan=3, sticky="w", padx=4, pady=4)

        row += 1
        ttk.Label(body, text="Environment:").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Combobox(body, textvariable=self.env_var, values=["NON-PROD", "PROD"], width=14, state="readonly").grid(row=row, column=1, sticky="w", padx=4, pady=4)

        row += 1
        ttk.Label(body, text="Server\\Instance or host,port:").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(body, textvariable=self.server_var, width=34).grid(row=row, column=1, columnspan=3, sticky="w", padx=4, pady=4)

        row += 1
        ttk.Label(body, text="Authentication:").grid(row=row, column=0, sticky="e", padx=4, pady=4)
        ttk.Radiobutton(body, text="Windows (uses current credentials)", variable=self.auth_var, value="windows", command=self._refresh).grid(row=row, column=1, sticky="w")
        ttk.Radiobutton(body, text="SQL Server", variable=self.auth_var, value="sql", command=self._refresh).grid(row=row, column=2, sticky="w")

        row += 1
        self.win_box = ttk.LabelFrame(body, text="Windows Authentication")
        self.win_box.grid(row=row, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(self.win_box, text="Display User (DOMAIN\\User):").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.win_box, textvariable=self.win_user_display, width=30).grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.win_box, text="Note: sqlcmd will use the current Windows session credentials (-E).").grid(row=1, column=0, columnspan=2, sticky="w", padx=4, pady=2)

        row += 1
        self.sql_box = ttk.LabelFrame(body, text="SQL Server Authentication")
        self.sql_box.grid(row=row, column=0, columnspan=4, sticky="ew", padx=2, pady=6)
        ttk.Label(self.sql_box, text="Username:").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        ttk.Entry(self.sql_box, textvariable=self.user_var, width=24).grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(self.sql_box, text="Password:").grid(row=0, column=2, sticky="e", padx=4, pady=4)
        ttk.Entry(self.sql_box, textvariable=self.pass_var, width=18, show="*").grid(row=0, column=3, sticky="w", padx=4, pady=4)

        btns = ttk.Frame(self, padding=(10,6))
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="Test Connection", command=self._test_connection).pack(side=tk.LEFT)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side=tk.RIGHT, padx=(6,0))
        ttk.Button(btns, text="Save", command=self._save).pack(side=tk.RIGHT)

        self._refresh()
        self.grab_set()
        self.transient(app)

    def _refresh(self):
        sql_auth = self.auth_var.get() == "sql"
        for child in self.sql_box.winfo_children():
            child.configure(state=("normal" if sql_auth else "disabled"))
        for child in self.win_box.winfo_children():
            child.configure(state=("normal" if not sql_auth else "disabled"))

    def _make_target(self) -> InstanceTarget:
        name = self.name_var.get().strip()
        env = self.env_var.get().strip() or "NON-PROD"
        server = self.server_var.get().strip()
        auth = self.auth_var.get()
        if auth == "sql":
            user = self.user_var.get().strip()
            pwd = self.pass_var.get()
            return InstanceTarget(name=name, server=server, environment=env, auth="sql",
                                  username=user or None, password_enc=_encrypt_password(pwd) if pwd else None,
                                  win_user_display=None)
        else:
            return InstanceTarget(name=name, server=server, environment=env, auth="windows",
                                  username=None, password_enc=None, win_user_display=self.win_user_display.get().strip() or None)

    def _test_connection(self):
        try:
            t = self._make_target()
            sqlcmd_path = self.app.sqlcmd_path_var.get().strip() or self.app.cfg.get("sqlcmd_path", "")
            cmd = build_sqlcmd_command(sqlcmd_path, t.server, t.auth, t.username, _decrypt_password(t.password_enc),
                                       "SET NOCOUNT ON; SELECT 1;")
            rc, out, err = run_sqlcmd(cmd)
            if rc == 0 and out.strip().startswith("1"):
                messagebox.showinfo(APP_NAME, f"Connection OK: {t.name}")
            else:
                raise RuntimeError(err or out or "Unknown error")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Connection failed:\n{e}")

    def _save(self):
        try:
            t = self._make_target()
            if not t.name:
                messagebox.showerror(APP_NAME, "Instance Name is required.")
                return
            if not t.server:
                messagebox.showerror(APP_NAME, "Server/Instance is required.")
                return
            if self.on_save:
                self.on_save(t)
            self.destroy()
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to save: {e}")


# Keep compatibility with the launcher expecting SqlServerPlaceholder
class SqlServerPlaceholder(SqlServerMonitorApp):
    pass
