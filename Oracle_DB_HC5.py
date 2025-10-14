#!/usr/bin/env python3
"""
Oracle DB Health GUI Monitor — Version 3
- Tkinter Treeview (no extra deps)
- Column order:
  DB Name, Environment, Host, Status, Inst_status, Role, OpenMode, Sessions,
  WorstTS%, LastFull/Inc, LastArch, DB Version, Ms, LastChecked, Error
- Changes vs v2:
  1) Added "Environment" column (NON-PROD or PROD) in Add/Edit DB; stored in config.
  2) OpenMode shows plain text (no emoji).
  3) Vertical scrollbar for the DB list.
  4) Click any column header to sort (type-aware: dates, percents, numbers, status, etc.).
  5) Email HTML report; editable SMTP/Exchange server + port; From/To fields; saves in config.
Setup:
    pip install python-oracledb
Run:
    python oracle_db_health_gui_emojis_v3.py
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
from typing import Dict, List, Optional, Tuple

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# Oracle driver
try:
    import oracledb
except Exception:
    oracledb = None  # will error on connect if missing

APP_NAME = "Oracle DB Health GUI Monitor"
CONFIG_DIR = Path.home() / ".ora_gui_monitor"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "config.json"

ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
DEFAULT_INTERVAL_SEC = 300  # 5 minutes
MAX_WORKERS = 8

# Emojis (explicit escapes for safe copy/paste)
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
    environment: str = "NON-PROD"  # or "PROD"

@dataclass
class DbHealth:
    status: str
    details: str
    version: str = ""
    role: str = ""
    open_mode: str = ""
    inst_status: str = ""
    sessions_active: int = 0
    sessions_total: int = 0
    worst_ts_pct_used: Optional[float] = None
    host: str = ""
    elapsed_ms: int = 0
    last_full_inc_backup: Optional[datetime] = None
    last_arch_backup: Optional[datetime] = None
    error: str = ""
    ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def default_config() -> Dict:
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
    }

def load_config() -> Dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            base = default_config()
            base.update({k:v for k,v in cfg.items() if k!="email"})
            if "email" in cfg:
                base["email"].update(cfg["email"])
            return base
        except Exception:
            pass
    return default_config()

def save_config(cfg: Dict):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        messagebox.showerror(APP_NAME, f"Failed to save config: {e}")

def init_oracle_client_if_needed(cfg: Dict):
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
    "sess": (
        "SELECT COUNT(*) total, SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END) active "
        "FROM v$session WHERE type='USER'"
    ),
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

            cur.execute(SQLS["db"])
            name, open_mode, role, log_mode = cur.fetchone()

            cur.execute(SQLS["inst"])
            inst_name, inst_status, host_name, inst_version, startup_time = cur.fetchone()

            sessions_total, sessions_active = 0, 0
            try:
                cur.execute(SQLS["sess"])
                total, active = cur.fetchone()
                sessions_total = int(total or 0)
                sessions_active = int(active or 0)
            except Exception:
                pass

            worst_pct = None
            try:
                cur.execute(SQLS["tspace"])
                worst_pct = 0.0
                for ts_name, pct_used in cur.fetchall():
                    if pct_used is not None and pct_used > (worst_pct or 0):
                        worst_pct = float(pct_used)
            except Exception:
                worst_pct = None

            last_df = None
            last_arch = None
            try:
                cur.execute(SQLS["bk_data"])
                r = cur.fetchone()
                last_df = r[0] if r else None
            except Exception:
                last_df = None
            try:
                cur.execute(SQLS["bk_arch"])
                r = cur.fetchone()
                last_arch = r[0] if r else None
            except Exception:
                last_arch = None

            elapsed_ms = int((time.time() - t0) * 1000)
            return DbHealth(
                status="UP",
                details=f"Log:{log_mode}",
                version=inst_version,
                role=role,
                open_mode=open_mode,
                inst_status=inst_status,
                sessions_active=sessions_active,
                sessions_total=sessions_total,
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
        "DB Name", "Environment", "Host", "Status", "Inst_status", "Role", "OpenMode", "Sessions",
        "WorstTS%", "LastFull/Inc", "LastArch", "DB Version", "Ms", "LastChecked", "Error"
    )

    def __init__(self, master, cfg: Dict):
        super().__init__(master)
        self.master.title(APP_NAME)
        self.pack(fill=tk.BOTH, expand=True)

        self.cfg = cfg
        self.interval_sec = int(cfg.get("interval_sec", DEFAULT_INTERVAL_SEC))
        self.targets: List[DbTarget] = [DbTarget(**t) for t in cfg.get("targets", [])]

        self._stop_flag = threading.Event()
        self._running = False
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

        self._build_ui()
        init_oracle_client_if_needed(cfg)
        self._refresh_table()

    # ---------- UI ----------
    def _build_ui(self):
        topbar = ttk.Frame(self)
        topbar.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)

        self.interval_var = tk.IntVar(value=self.interval_sec)
        ttk.Label(topbar, text="Interval (sec):").pack(side=tk.LEFT)
        ttk.Spinbox(topbar, from_=30, to=3600, textvariable=self.interval_var, width=8).pack(side=tk.LEFT, padx=(4, 10))

        ttk.Button(topbar, text="Start", command=self.start).pack(side=tk.LEFT)
        ttk.Button(topbar, text="Stop", command=self.stop).pack(side=tk.LEFT, padx=(6, 10))
        ttk.Button(topbar, text="Run Now", command=self.run_once).pack(side=tk.LEFT)

        ttk.Button(topbar, text="Add DB", command=self._add_dialog).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(topbar, text="Edit DB", command=self._edit_selected).pack(side=tk.LEFT)
        ttk.Button(topbar, text="Remove DB", command=self._remove_selected).pack(side=tk.LEFT)

        ttk.Button(topbar, text="Import JSON", command=self._import_json).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(topbar, text="Export JSON", command=self._export_json).pack(side=tk.LEFT)

        ttk.Label(topbar, text="Client lib dir:").pack(side=tk.LEFT, padx=(10, 0))
        self.client_dir_var = tk.StringVar(value=self.cfg.get("client_lib_dir", ""))
        ttk.Entry(topbar, textvariable=self.client_dir_var, width=24).pack(side=tk.LEFT, padx=4)
        ttk.Button(topbar, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT, padx=(0, 10))

        # Email settings
        email_cfg = self.cfg.get("email", {})
        ttk.Label(topbar, text="SMTP/Exchange:").pack(side=tk.LEFT)
        self.smtp_server_var = tk.StringVar(value=email_cfg.get("server", ""))
        self.smtp_port_var = tk.IntVar(value=int(email_cfg.get("port", 25)))
        ttk.Entry(topbar, textvariable=self.smtp_server_var, width=18).pack(side=tk.LEFT, padx=(4,2))
        ttk.Entry(topbar, textvariable=self.smtp_port_var, width=6).pack(side=tk.LEFT, padx=(2,6))

        ttk.Label(topbar, text="From:").pack(side=tk.LEFT)
        self.from_var = tk.StringVar(value=email_cfg.get("from_addr", ""))
        ttk.Entry(topbar, textvariable=self.from_var, width=20).pack(side=tk.LEFT, padx=(4,6))

        ttk.Label(topbar, text="To (comma):").pack(side=tk.LEFT)
        self.to_var = tk.StringVar(value=email_cfg.get("to_addrs", ""))
        ttk.Entry(topbar, textvariable=self.to_var, width=24).pack(side=tk.LEFT, padx=(4,6))

        ttk.Button(topbar, text="Save Mail", command=self._save_mail_settings).pack(side=tk.LEFT)
        ttk.Button(topbar, text="Email Report", command=self._email_report).pack(side=tk.LEFT, padx=(6,0))

        # Tree + vertical scrollbar
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.tree = ttk.Treeview(tree_frame, columns=self.COLUMNS, show="headings", height=18)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        col_widths = {
            "DB Version": 300, "Error": 340, "LastFull/Inc": 170, "LastArch": 170,
            "Environment": 110, "Host": 160, "DB Name": 160
        }
        for col in self.COLUMNS:
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c, False))
            width = col_widths.get(col, 120)
            self.tree.column(col, width=width, stretch=True)

        bottombar = ttk.Frame(self)
        bottombar.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottombar, textvariable=self.status_var).pack(side=tk.LEFT)

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
        # Examples: "✅ 2025-10-13 12:00:00", "❌ -", "2025-10-13 12:00:00", "-"
        parts = s.strip().split()
        if not parts or parts[-1] == "-":
            return float("-inf")
        # last two tokens may be date and time
        try:
            if len(parts) >= 2 and ":" in parts[-1]:
                dt = datetime.strptime(f"{parts[-2]} {parts[-1]}", "%Y-%m-%d %H:%M:%S")
                return dt.timestamp()
            # fallback single token date
            dt = datetime.strptime(parts[-1], "%Y-%m-%d")
            return dt.timestamp()
        except Exception:
            return float("-inf")

    def _status_rank(self, s: str) -> int:
        return 1 if s.strip().startswith(GOOD) else 0

    def _inst_rank(self, s: str) -> int:
        return 1 if ("OPEN" in s.upper() and s.strip().startswith(GOOD)) else 0

    def _generic_key(self, col: str, s: str):
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
        return (s.lower(),)

    def _sort_by_column(self, col: str, descending: bool):
        rows = [(self._generic_key(col, self.tree.set(k, col)), k) for k in self.tree.get_children("")]
        rows.sort(reverse=descending, key=lambda x: x[0])
        for idx, (_, k) in enumerate(rows):
            self.tree.move(k, "", idx)
        # toggle order
        self.tree.heading(col, command=lambda c=col: self._sort_by_column(c, not descending))

    # ---------- Actions ----------
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

    def _refresh_table(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for t in self.targets:
            values = ["-"] * len(self.COLUMNS)
            values[0] = t.name
            values[1] = t.environment
            self.tree.insert("", tk.END, iid=t.name, values=tuple(values))

    # --- Monitoring ---
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

    def run_once(self):
        self._do_checks()

    def _loop(self):
        if self._stop_flag.is_set():
            return
        self._do_checks()
        if not self._stop_flag.is_set():
            self.after(self.interval_sec * 1000, self._loop)

    def _do_checks(self):
        if not self.targets:
            self.status_var.set("No targets configured")
            return
        self.status_var.set("Checking...")
        for t in self.targets:
            try:
                res = check_one(t)
            except Exception as e:
                res = DbHealth(status="DOWN", details=str(e), error=str(e))
            self._update_row(t, res)
        self.status_var.set(f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _fmt_sessions(self, h: DbHealth) -> str:
        return f"{h.sessions_active}/{h.sessions_total}" if h.sessions_total else "-"

    def _fmt_worst_ts(self, h: DbHealth) -> str:
        return f"{h.worst_ts_pct_used:.1f}%" if h.worst_ts_pct_used is not None else "-"

    def _mark(self, ok: bool) -> str:
        return GOOD if ok else BAD

    def _fmt_backup_cell(self, when: Optional[datetime], arch: bool=False) -> str:
        if not when:
            return f"{self._mark(False)} -"
        age_hours = (datetime.now(when.tzinfo) - when).total_seconds()/3600.0
        ok = (age_hours <= 12) if arch else ((age_hours/24.0) <= 3)
        return f"{self._mark(ok)} {_dt_str(when)}"

    def _update_row(self, target: DbTarget, h: DbHealth):
        name = target.name
        status_cell = f"{self._mark(h.status.upper() == 'UP')} {h.status}"
        inst_cell = f"{self._mark((h.inst_status or '').upper() == 'OPEN')} {h.inst_status or '-'}"
        open_cell = h.open_mode or "-"  # plain text
        worst_ok = not (h.worst_ts_pct_used is not None and h.worst_ts_pct_used >= 90.0)
        worst_cell = f"{self._mark(worst_ok)} {self._fmt_worst_ts(h)}"

        vals = (
            name,
            target.environment,
            h.host or "-",
            status_cell,
            inst_cell,
            h.role or "-",
            open_cell,
            self._fmt_sessions(h),
            worst_cell,
            self._fmt_backup_cell(h.last_full_inc_backup, arch=False),
            self._fmt_backup_cell(h.last_arch_backup, arch=True),
            h.version or "-",
            h.elapsed_ms,
            h.ts,
            h.error or ("" if h.status=="UP" else h.details)
        )
        if name in self.tree.get_children():
            self.tree.item(name, values=vals)
        else:
            self.tree.insert("", tk.END, iid=name, values=vals)

    # --- CRUD ---
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

    def _add_target(self, t: DbTarget):
        if any(x.name == t.name for x in self.targets):
            messagebox.showerror(APP_NAME, "A target with this name already exists.")
            return
        self.targets.append(t)
        self._persist_targets()
        self._refresh_table()

    def _update_target(self, t: DbTarget):
        for i, x in enumerate(self.targets):
            if x.name == t.name:
                self.targets[i] = t
                break
        self._persist_targets()
        self._refresh_table()

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
            self.interval_var.set(int(self.cfg.get("interval_sec", DEFAULT_INTERVAL_SEC)))
            self.client_dir_var.set(self.cfg.get("client_lib_dir", ""))
            self.targets = [DbTarget(**t) for t in self.cfg.get("targets", [])]
            save_config(self.cfg)
            self._refresh_table()
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
            }
            with open(p, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo(APP_NAME, "Exported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to export: {e}")

    # --- Email ---
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
        def cell_style(text: str, col: str) -> str:
            ok = None
            if col in ("Status","Inst_status","WorstTS%","LastFull/Inc","LastArch"):
                t = text.strip()
                if t.startswith(GOOD):
                    ok = True
                elif t.startswith(BAD):
                    ok = False
            if col == "OpenMode":
                ok = ("OPEN" in text.upper())
            if col == "WorstTS%":
                try:
                    pct = float(text.split()[-1].replace("%",""))
                    ok = pct < 90.0
                except Exception:
                    pass
            if ok is True:
                return "background-color:#e6ffe6;color:#064b00;font-weight:bold;"
            if ok is False:
                return "background-color:#ffe6e6;color:#7a0000;font-weight:bold;"
            return ""

        headers = list(self.COLUMNS)
        thead = "<tr>" + "".join(f"<th style='padding:6px 10px;border-bottom:1px solid #ccc;text-align:left'>{h}</th>" for h in headers) + "</tr>"
        body_rows = []
        for r in rows:
            tds = []
            for col, val in zip(headers, r):
                style = cell_style(str(val), col)
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
            # Extend here for AUTH/STARTTLS if needed
            s.sendmail(from_addr, to_addrs, msg.as_string())

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
    root.geometry("1800x700")
    root.mainloop()

if __name__ == "__main__":
    main()
