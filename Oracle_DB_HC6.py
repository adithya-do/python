#!/usr/bin/env python3
"""
Oracle DB Health GUI Monitor — Version 4
Improvements:
1) Bottom horizontal scrollbar.
2) Mail settings moved to second toolbar row; persisted.
3) Removed Role, OpenMode columns.
4) Sessions shows CURRENT / LIMIT (v$session count / v$parameter('sessions')).
5) Right-click copy: cell, DB Name, Host, Error.
6) SMTP/email info persists.
7) New first column: S.No.
8) Persist last health results in config and reload on start.
9) Column "Check status": In Progress / Complete; per-DB streaming updates.
10) Run All vs Run Selected (single DB) checks.

Run:
    python oracle_db_health_gui_emojis_v4.py
"""
import json
import os
import smtplib
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# Oracle
try:
    import oracledb
except Exception:
    oracledb = None  # will raise on connect

APP_NAME = "Oracle DB Health GUI Monitor"
CONFIG_DIR = Path.home() / ".ora_gui_monitor"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "config.json"

ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
DEFAULT_INTERVAL_SEC = 300  # 5 minutes
MAX_WORKERS = 8

GOOD = "\u2705"  # ✅
BAD = "\u274C"   # ❌

@dataclass
class DbTarget:
    name: str
    dsn: str
    user: Optional[str] = None
    password: Optional[str] = None
    wallet_dir: Optional[str] = None
    mode: str = "thin"  # "thick" or "thin"
    environment: str = "NON-PROD"  # "NON-PROD" or "PROD"

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
    error: str = ""
    ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def default_config() -> Dict[str, Any]:
    return {
        "interval_sec": DEFAULT_INTERVAL_SEC,
        "targets": [],
        "client_lib_dir": ORACLE_CLIENT_LIB_DIR or "",
        "email": {
            "server": "",
            "port": 25,
            "from_addr": "",
            "to_addrs": "",
            "subject": "Oracle DB Health Report",
        },
        "last_health": {},
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
            return base
        except Exception:
            pass
    return default_config()

def save_config(cfg: Dict[str, Any]):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, default=str)
    except Exception as e:
        messagebox.showerror(APP_NAME, f"Failed to save config: {e}")

def init_oracle_client_if_needed(cfg: Dict[str, Any]):
    if oracledb is None:
        return
    lib_dir = cfg.get("client_lib_dir") or ORACLE_CLIENT_LIB_DIR
    if lib_dir:
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        except oracledb.ProgrammingError:
            pass
        except Exception as e:
            messagebox.showwarning(APP_NAME, f"Oracle client init issue: {e}\nProceeding in thin mode if possible.")

def _connect(target: DbTarget):
    if oracledb is None:
        raise RuntimeError("python-oracledb not installed. pip install python-oracledb")
    if target.mode.lower() == "thick" and ORACLE_CLIENT_LIB_DIR:
        try:
            oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
        except Exception:
            pass
    if target.wallet_dir:
        return oracledb.connect(config_dir=target.wallet_dir, dsn=target.dsn)
    if target.user and target.password:
        return oracledb.connect(user=target.user, password=target.password, dsn=target.dsn)
    return oracledb.connect(dsn=target.dsn)

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

def check_one(target: DbTarget, timeout_sec: int = 25) -> DbHealth:
    t0 = time.time()
    try:
        with _connect(target) as conn:
            conn.call_timeout = timeout_sec * 1000
            cur = conn.cursor()

            # Keep log_mode info
            try:
                cur.execute(SQLS["db"])
                _name, _open_mode, _role, log_mode = cur.fetchone()
                details = f"Log:{log_mode}"
            except Exception:
                details = ""

            cur.execute(SQLS["inst"])
            _inst_name, inst_status, host_name, inst_version, _startup_time = cur.fetchone()

            # Sessions current and limit
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

            # Worst TS% used
            worst_pct = None
            try:
                cur.execute(SQLS["tspace"])
                worst_pct = 0.0
                for _ts_name, pct_used in cur.fetchall():
                    if pct_used is not None and pct_used > (worst_pct or 0):
                        worst_pct = float(pct_used)
            except Exception:
                worst_pct = None

            # Backups
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
                error="",
            )
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        return DbHealth(
            status="DOWN",
            details=str(e),
            elapsed_ms=elapsed_ms,
            error=str(e),
        )

class MonitorApp(ttk.Frame):
    COLUMNS = (
        "S.No", "DB Name", "Environment", "Host", "Status", "Inst_status", "Sessions",
        "WorstTS%", "LastFull/Inc", "LastArch", "DB Version", "Ms", "LastChecked", "Check status", "Error"
    )

    def __init__(self, master, cfg: Dict[str, Any]):
        super().__init__(master)
        self.master.title(APP_NAME)
        self.pack(fill=tk.BOTH, expand=True)

        self.cfg = cfg
        self.interval_sec = int(cfg.get("interval_sec", DEFAULT_INTERVAL_SEC))
        self.targets: List[DbTarget] = [DbTarget(**t) for t in cfg.get("targets", [])]
        self.last_health: Dict[str, Dict[str, Any]] = cfg.get("last_health", {})
        self._stop_flag = threading.Event()
        self._running = False
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

        self._build_ui()
        init_oracle_client_if_needed(cfg)
        self._refresh_table_from_targets()
        self._load_last_health_into_rows()

        self.master.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---------- UI ----------
    def _build_ui(self):
        # Toolbar 1: controls
        t1 = ttk.Frame(self)
        t1.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(6,3))

        self.interval_var = tk.IntVar(value=self.interval_sec)
        ttk.Label(t1, text="Interval (sec):").pack(side=tk.LEFT)
        ttk.Spinbox(t1, from_=30, to=3600, textvariable=self.interval_var, width=8).pack(side=tk.LEFT, padx=(4, 10))

        ttk.Button(t1, text="Start (All)", command=self.start).pack(side=tk.LEFT)
        ttk.Button(t1, text="Stop", command=self.stop).pack(side=tk.LEFT, padx=(6, 10))
        ttk.Button(t1, text="Run All", command=self.run_all_once).pack(side=tk.LEFT)
        ttk.Button(t1, text="Run Selected", command=self.run_selected_once).pack(side=tk.LEFT, padx=(6, 10))

        ttk.Button(t1, text="Add DB", command=self._add_dialog).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(t1, text="Edit DB", command=self._edit_selected).pack(side=tk.LEFT)
        ttk.Button(t1, text="Remove DB", command=self._remove_selected).pack(side=tk.LEFT)

        ttk.Button(t1, text="Import JSON", command=self._import_json).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(t1, text="Export JSON", command=self._export_json).pack(side=tk.LEFT)

        ttk.Label(t1, text="Client lib dir:").pack(side=tk.LEFT, padx=(10, 0))
        self.client_dir_var = tk.StringVar(value=self.cfg.get("client_lib_dir", ""))
        ttk.Entry(t1, textvariable=self.client_dir_var, width=22).pack(side=tk.LEFT, padx=4)
        ttk.Button(t1, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT)

        # Toolbar 2: Mail settings
        t2 = ttk.Frame(self)
        t2.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0,6))

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
        ttk.Button(t2, text="Email Report", command=self._email_report).pack(side=tk.LEFT, padx=(6,0))

        # Tree + scrollbars
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.tree = ttk.Treeview(tree_frame, columns=self.COLUMNS, show="headings", height=20)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=xsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)

        col_widths = {
            "DB Version": 300, "Error": 360, "LastFull/Inc": 180, "LastArch": 180,
            "Environment": 120, "Host": 170, "DB Name": 170, "Check status": 120
        }
        for col in self.COLUMNS:
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c, False))
            width = col_widths.get(col, 120)
            self.tree.column(col, width=width, stretch=True, anchor="w")

        # Right-click copy menu
        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="Copy Cell", command=self._copy_cell)
        self.menu.add_separator()
        self.menu.add_command(label="Copy DB Name", command=lambda: self._copy_by_col("DB Name"))
        self.menu.add_command(label="Copy Host", command=lambda: self._copy_by_col("Host"))
        self.menu.add_command(label="Copy Error", command=lambda: self._copy_by_col("Error"))
        self.tree.bind("<Button-3>", self._show_context_menu)

        bottombar = ttk.Frame(self)
        bottombar.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottombar, textvariable=self.status_var).pack(side=tk.LEFT)

    # ---------- Context Menu ----------
    def _show_context_menu(self, event):
        iid = self.tree.identify_row(event.y)
        cid = self.tree.identify_column(event.x)
        if iid:
            self.tree.selection_set(iid)
            self._context_row = iid
            self._context_col = cid
            self.menu.tk_popup(event.x_root, event.y_root)

    def _copy_cell(self):
        try:
            row = getattr(self, "_context_row", None)
            colid = getattr(self, "_context_col", None)
            if not row or not colid:
                return
            col_index = int(colid.replace("#","")) - 1
            vals = self.tree.item(row)["values"]
            text = str(vals[col_index]) if col_index < len(vals) else ""
            self.clipboard_clear()
            self.clipboard_append(text)
        except Exception:
            pass

    def _copy_by_col(self, colname: str):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        vals = self.tree.item(iid)["values"]
        try:
            idx = self.COLUMNS.index(colname)
            text = str(vals[idx]) if idx < len(vals) else ""
            self.clipboard_clear()
            self.clipboard_append(text)
        except Exception:
            pass

    # ---------- Sorting ----------
    def _parse_sessions(self, s: str) -> Tuple[int,int]:
        try:
            a, b = s.split("/")
            return (int(a), int(b))
        except Exception:
            return (0, 0)

    def _parse_pct(self, s: str) -> float:
        try:
            return float(s.replace("%","").split()[-1])
        except Exception:
            return -1.0

    def _parse_datecell(self, s: str) -> float:
        parts = s.strip().split()
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
        return 1 if s.strip().startswith(GOOD) else 0

    def _inst_rank(self, s: str) -> int:
        return 1 if ("OPEN" in s.upper() and s.strip().startswith(GOOD)) else 0

    def _generic_key(self, col: str, s: str):
        if col == "S.No":
            try: return (int(s),)
            except: return (0,)
        if col == "Status":
            return (self._status_rank(s), s)
        if col == "Inst_status":
            return (self._inst_rank(s), s)
        if col in ("WorstTS%",):
            return (self._parse_pct(s),)
        if col in ("LastFull/Inc","LastArch","LastChecked"):
            return (self._parse_datecell(s),)
        if col == "Sessions":
            return self._parse_sessions(s)
        if col == "Ms":
            try: return (int(s),)
            except: return (-1,)
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

    # ---------- Helpers ----------
    def _renumber(self):
        for i, iid in enumerate(self.tree.get_children(""), start=1):
            vals = list(self.tree.item(iid)["values"])
            if vals:
                vals[0] = i  # S.No
                self.tree.item(iid, values=vals)

    def _pick_client_dir(self):
        d = filedialog.askdirectory(title="Select Oracle Client lib directory")
        if d:
            self.client_dir_var.set(d)
            self.cfg["client_lib_dir"] = d
            save_config(self.cfg)
            try:
                if oracledb is not None:
                    oracledb.init_oracle_client(lib_dir=d)
                messagebox.showinfo(APP_NAME, f"Oracle client initialized: {d}")
            except Exception as e:
                messagebox.showwarning(APP_NAME, f"Failed to init client: {e}")

    def _refresh_table_from_targets(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for idx, t in enumerate(self.targets, start=1):
            values = ["-"] * len(self.COLUMNS)
            values[0] = idx  # S.No
            values[1] = t.name
            values[2] = t.environment
            self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber()

    def _load_last_health_into_rows(self):
        for t in self.targets:
            hdict = self.last_health.get(t.name)
            if not hdict:
                continue
            self._apply_persisted_row(t.name, hdict)

    def _apply_persisted_row(self, name: str, hdict: Dict[str, Any]):
        vals = list(self.tree.item(name)["values"])
        def mark(ok: bool) -> str:
            return GOOD if ok else BAD
        status_cell = f"{mark(hdict.get('status','').upper() == 'UP')} {hdict.get('status','-')}"
        inst_cell = f"{mark((hdict.get('inst_status','') or '').upper() == 'OPEN')} {hdict.get('inst_status','-')}"
        sessions_cell = f"{hdict.get('sessions_curr',0)}/{hdict.get('sessions_limit',0)}"
        worst_ok = not (hdict.get('worst_ts_pct_used') is not None and float(hdict.get('worst_ts_pct_used')) >= 90.0)
        worst_val = '-' if hdict.get('worst_ts_pct_used') is None else f"{float(hdict.get('worst_ts_pct_used')):.1f}%"
        worst_cell = f"{mark(worst_ok)} {worst_val}"

        vals[3] = hdict.get("host","-")
        vals[4] = status_cell
        vals[5] = inst_cell
        vals[6] = sessions_cell
        vals[7] = worst_cell
        vals[8] = hdict.get("last_full_inc_backup_str", f"{BAD} -")
        vals[9] = hdict.get("last_arch_backup_str", f"{BAD} -")
        vals[10] = hdict.get("version","-")
        vals[11] = hdict.get("elapsed_ms",0)
        vals[12] = hdict.get("ts","-")
        vals[13] = "Complete"
        vals[14] = hdict.get("error","")
        self.tree.item(name, values=vals)

    # ---------- Monitoring ----------
    def start(self):
        if self._running:
            return
        self.interval_sec = self.interval_var.get()
        self._stop_flag.clear()
        self._running = True
        self.status_var.set(f"Monitoring every {self.interval_sec}s...")
        self.after(200, self._loop)

    def stop(self):
        self._stop_flag.set()
        self._running = False
        self.status_var.set("Stopped")

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
        if self._stop_flag.is_set():
            return
        self._checks_async(targets=self.targets)
        if not self._stop_flag.is_set():
            self.after(self.interval_sec * 1000, self._loop)

    def _checks_async(self, targets: List[DbTarget]):
        for t in targets:
            self._set_check_status(t.name, "In Progress")

        def job(t: DbTarget):
            try:
                res = check_one(t)
            except Exception as e:
                res = DbHealth(status="DOWN", details=str(e), error=str(e))
            self.after(0, lambda tn=t.name, tr=t, rh=res: self._apply_result(tn, tr, rh))

        for t in targets:
            self._executor.submit(job, t)

    def _set_check_status(self, name: str, status: str):
        if name in self.tree.get_children():
            vals = list(self.tree.item(name)["values"])
            if len(vals) >= 15:
                vals[13] = status
                self.tree.item(name, values=vals)

    def _apply_result(self, name: str, target: DbTarget, h: DbHealth):
        status_cell = f"{GOOD if h.status.upper() == 'UP' else BAD} {h.status}"
        inst_cell = f"{GOOD if (h.inst_status or '').upper() == 'OPEN' else BAD} {h.inst_status or '-'}"
        sessions_cell = f"{h.sessions_curr}/{h.sessions_limit}"
        worst_ok = not (h.worst_ts_pct_used is not None and h.worst_ts_pct_used >= 90.0)
        worst_val = '-' if h.worst_ts_pct_used is None else f"{h.worst_ts_pct_used:.1f}%"
        worst_cell = f"{GOOD if worst_ok else BAD} {worst_val}"

        def fmt_backup(dt: Optional[datetime], arch=False):
            if not dt:
                return f"{BAD} -"
            age_hours = (datetime.now(dt.tzinfo) - dt).total_seconds()/3600.0
            ok = (age_hours <= 12) if arch else ((age_hours/24.0) <= 3)
            return f"{GOOD if ok else BAD} {_dt_str(dt)}"

        last_full_cell = fmt_backup(h.last_full_inc_backup, arch=False)
        last_arch_cell = fmt_backup(h.last_arch_backup, arch=True)

        vals = list(self.tree.item(name)["values"])
        vals[3] = h.host or "-"
        vals[4] = status_cell
        vals[5] = inst_cell
        vals[6] = sessions_cell
        vals[7] = worst_cell
        vals[8] = last_full_cell
        vals[9] = last_arch_cell
        vals[10] = h.version or "-"
        vals[11] = h.elapsed_ms
        vals[12] = h.ts
        vals[13] = "Complete"
        vals[14] = h.error or ("" if h.status == "UP" else h.details)
        self.tree.item(name, values=vals)

        self.last_health[name] = {
            "status": h.status,
            "inst_status": h.inst_status,
            "sessions_curr": h.sessions_curr,
            "sessions_limit": h.sessions_limit,
            "worst_ts_pct_used": h.worst_ts_pct_used,
            "host": h.host,
            "elapsed_ms": h.elapsed_ms,
            "version": h.version,
            "ts": h.ts,
            "error": vals[14],
            "last_full_inc_backup_str": last_full_cell,
            "last_arch_backup_str": last_arch_cell,
        }
        self.cfg["last_health"] = self.last_health
        save_config(self.cfg)

        self.status_var.set(f"Updated {name} at {h.ts}")
        self._renumber()

    # ---------- CRUD ----------
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
        values = ["-"] * len(self.COLUMNS)
        values[0] = len(self.targets)
        values[1] = t.name
        values[2] = t.environment
        self.tree.insert("", tk.END, iid=t.name, values=tuple(values))
        self._renumber()

    def _update_target(self, t: DbTarget):
        for i, x in enumerate(self.targets):
            if x.name == t.name:
                self.targets[i] = t
                break
        self._persist_targets()
        vals = list(self.tree.item(t.name)["values"])
        vals[1] = t.name
        vals[2] = t.environment
        self.tree.item(t.name, values=vals)

    def _persist_targets(self):
        self.cfg["interval_sec"] = self.interval_var.get()
        self.cfg["targets"] = [asdict(t) for t in self.targets]
        self.cfg["client_lib_dir"] = self.client_dir_var.get()
        save_config(self.cfg)

    def _import_json(self):
        p = filedialog.askopenfilename(title="Import config.json", filetypes=[["JSON", "*.json"]])
        if not p:
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            self.cfg.update(cfg)
            if "email" in cfg:
                self.cfg["email"].update(cfg["email"] or {})
            self.interval_var.set(int(self.cfg.get("interval_sec", DEFAULT_INTERVAL_SEC)))
            self.client_dir_var.set(self.cfg.get("client_lib_dir", ""))
            self.targets = [DbTarget(**t) for t in self.cfg.get("targets", [])]
            self.last_health = self.cfg.get("last_health", {})
            save_config(self.cfg)
            self._refresh_table_from_targets()
            self._load_last_health_into_rows()
            messagebox.showinfo(APP_NAME, "Imported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to import: {e}")

    def _export_json(self):
        p = filedialog.asksaveasfilename(title="Export config.json", defaultextension=".json", initialfile="config.json")
        if not p:
            return
        try:
            export = {
                "interval_sec": self.interval_var.get(),
                "targets": [asdict(t) for t in self.targets],
                "client_lib_dir": self.client_dir_var.get(),
                "email": {
                    "server": self.smtp_server_var.get().strip(),
                    "port": int(self.smtp_port_var.get() or 25),
                    "from_addr": self.from_var.get().strip(),
                    "to_addrs": self.to_var.get().strip(),
                    "subject": self.cfg.get("email", {}).get("subject", "Oracle DB Health Report"),
                },
                "last_health": self.last_health,
            }
            with open(p, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo(APP_NAME, "Exported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to export: {e}")

    # ---------- Email ----------
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
            self._send_html_email(server, port, from_addr, [x.strip() for x in to_addrs.split(",") if x.strip()], subject, html)
            messagebox.showinfo(APP_NAME, "Email report sent.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to send email: {e}")

    def _build_html(self, rows: List[List]) -> str:
        headers = list(self.COLUMNS)
        def cell_style(text: str, col: str) -> str:
            ok = None
            if col in ("Status","Inst_status","WorstTS%","LastFull/Inc","LastArch"):
                t = str(text).strip()
                if t.startswith(GOOD):
                    ok = True
                elif t.startswith(BAD):
                    ok = False
            if col == "WorstTS%":
                try:
                    pct = float(str(text).split()[-1].replace("%",""))
                    ok = pct < 90.0
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
            for col, val in zip(headers, r):
                style = cell_style(val, col)
                tds.append(f"<td style='padding:4px 8px;border-bottom:1px solid #eee;{style}'>{val}</td>")
            body_rows.append("<tr>" + "".join(tds) + "</tr>")
        table = "<table style='border-collapse:collapse;font-family:Segoe UI,Arial,sans-serif;font-size:12px'>" + thead + "".join(body_rows) + "</table>"
        title = f"<h3>Oracle DB Health Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h3>"
        return "<html><body>"+title+table+"</body></html>"

    def _send_html_email(self, server: str, port: int, from_addr: str, to_addrs: List[str], subject: str, html: str):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        part = MIMEText(html, "html", "utf-8")
        msg.attach(part)
        with smtplib.SMTP(server, port, timeout=20) as s:
            s.sendmail(from_addr, to_addrs, msg.as_string())

    def _on_close(self):
        self._persist_targets()
        self.cfg["last_health"] = self.last_health
        save_config(self.cfg)
        self.master.destroy()

class DbEditor(tk.Toplevel):
    def __init__(self, parent: "MonitorApp", target: Optional[DbTarget] = None, on_save=None):
        super().__init__(parent)
        self.title("DB Target")
        self.resizable(False, False)
        self.on_save = on_save

        self.var_name = tk.StringVar(value=target.name if target else "")
        self.var_dsn = tk.StringVar(value=target.dsn if target else "")
        self.var_user = tk.StringVar(value=target.user if target else "")
        self.var_pwd = tk.StringVar(value=target.password if target else "")
        self.var_wallet = tk.StringVar(value=target.wallet_dir if target else "")
        self.var_mode = tk.StringVar(value=target.mode if target else "thick")
        self.var_env = tk.StringVar(value=target.environment if target else "NON-PROD")

        frm = ttk.Frame(self, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)

        def row_entry(lbl, var, show=None, browse=False):
            r = ttk.Frame(frm)
            r.pack(fill=tk.X, pady=4)
            ttk.Label(r, text=lbl, width=20).pack(side=tk.LEFT)
            e = ttk.Entry(r, textvariable=var, show=show, width=48)
            e.pack(side=tk.LEFT, padx=4)
            if browse:
                ttk.Button(r, text="...", width=3, command=lambda v=var: self._pick_dir(v)).pack(side=tk.LEFT)

        row_entry("DB Name:", self.var_name)
        row_entry("TNS Alias / EZConnect:", self.var_dsn)
        row_entry("User:", self.var_user)
        row_entry("Password:", self.var_pwd, show="*")
        row_entry("Wallet Dir:", self.var_wallet, browse=True)

        rmode = ttk.Frame(frm)
        rmode.pack(fill=tk.X, pady=4)
        ttk.Label(rmode, text="Mode:", width=20).pack(side=tk.LEFT)
        ttk.Radiobutton(rmode, text="Thick", value="thick", variable=self.var_mode).pack(side=tk.LEFT)
        ttk.Radiobutton(rmode, text="Thin", value="thin", variable=self.var_mode).pack(side=tk.LEFT)

        renv = ttk.Frame(frm)
        renv.pack(fill=tk.X, pady=4)
        ttk.Label(renv, text="Environment:", width=20).pack(side=tk.LEFT)
        self.env_combo = ttk.Combobox(renv, textvariable=self.var_env, state="readonly", values=["NON-PROD","PROD"], width=45)
        self.env_combo.pack(side=tk.LEFT, padx=4)

        btns = ttk.Frame(frm)
        btns.pack(fill=tk.X, pady=(10, 2))
        ttk.Button(btns, text="Save", command=self._save).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side=tk.RIGHT, padx=6)

        self.grab_set()
        self.transient(parent)
        self.wait_visibility()
        self.focus()

    def _pick_dir(self, var: tk.StringVar):
        d = filedialog.askdirectory(title="Select wallet directory")
        if d:
            var.set(d)

    def _save(self):
        name = self.var_name.get().strip()
        dsn = self.var_dsn.get().strip()
        if not name or not dsn:
            messagebox.showerror(APP_NAME, "DB Name and TNS Alias/EZConnect are required")
            return
        t = DbTarget(
            name=name,
            dsn=dsn,
            user=self.var_user.get().strip() or None,
            password=self.var_pwd.get().strip() or None,
            wallet_dir=self.var_wallet.get().strip() or None,
            mode=self.var_mode.get().strip() or "thick",
            environment=self.var_env.get().strip() or "NON-PROD",
        )
        if self.on_save:
            self.on_save(t)
        self.destroy()

def main():
    cfg = load_config()
    root = tk.Tk()
    try:
        if sys.platform.startswith("win"):
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)  # type: ignore
    except Exception:
        pass

    app = MonitorApp(root, cfg)
    root.geometry("1900x780")
    root.mainloop()

if __name__ == "__main__":
    main()
