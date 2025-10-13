#!/usr/bin/env python3
"""
Oracle DB Health GUI Monitor
- Cross-platform Python GUI using Tkinter
- Uses python-oracledb (thick mode recommended) so it can leverage an installed Oracle Client
- Monitors multiple databases on a schedule (default: every 5 minutes)
- Reads/writes a JSON config at: ~/.ora_gui_monitor/config.json
- Supports TNS aliases (sqlnet.ora/tnsnames.ora) or EZConnect strings
- Color-coded health results with basic metrics

Prereqs
-------
python -m pip install python-oracledb

You must have Oracle Client installed and accessible (e.g., Instant Client)
Set the location via ORACLE_CLIENT_LIB_DIR env var or edit ORACLE_CLIENT_LIB_DIR below.

Run
---
python oracle_db_health_gui.py

"""
import json
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# Oracle
import oracledb

APP_NAME = "Oracle DB Health GUI Monitor"
CONFIG_DIR = Path.home() / ".ora_gui_monitor"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "config.json"

# If you want to hardcode the client lib dir, set it here:
ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")

DEFAULT_INTERVAL_SEC = 300  # 5 minutes
MAX_WORKERS = 8

@dataclass
class DbTarget:
    name: str
    dsn: str  # TNS alias or EZConnect
    user: Optional[str] = None
    password: Optional[str] = None
    wallet_dir: Optional[str] = None  # Optional: for mTLS / TCPS wallet
    mode: str = "thin"  # "thick" or "thin"; thick recommended if using full client

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
    ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def load_config() -> Dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # default skeleton
    return {
        "interval_sec": DEFAULT_INTERVAL_SEC,
        "targets": [
            # Example entry
            # {
            #   "name": "ORCL19",
            #   "dsn": "orclpdb1_high",  # or "host:1521/service"
            #   "user": "system",
            #   "password": "<secure>",
            #   "wallet_dir": null,
            #   "mode": "thick"
            # }
        ],
        "client_lib_dir": ORACLE_CLIENT_LIB_DIR or ""
    }


def save_config(cfg: Dict):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        messagebox.showerror(APP_NAME, f"Failed to save config: {e}")


def init_oracle_client_if_needed(cfg: Dict):
    lib_dir = cfg.get("client_lib_dir") or ORACLE_CLIENT_LIB_DIR
    # Use thick mode init only if a lib_dir is provided
    if lib_dir:
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        except oracledb.ProgrammingError:
            # Already initialized or using thin mode
            pass
        except Exception as e:
            messagebox.showwarning(APP_NAME, f"Oracle client init issue: {e}\nProceeding in thin mode if possible.")


def _connect(target: DbTarget):
    # Prefer thick mode if requested and client libs available
    if target.mode.lower() == "thick" and ORACLE_CLIENT_LIB_DIR:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
    # Wallet-based (TCPS) connection if wallet_dir provided
    if target.wallet_dir:
        return oracledb.connect(config_dir=target.wallet_dir, dsn=target.dsn)
    # Username/password
    if target.user and target.password:
        return oracledb.connect(user=target.user, password=target.password, dsn=target.dsn)
    # Otherwise attempt external authentication (OS auth) if configured
    return oracledb.connect(dsn=target.dsn)


SQLS = {
    "db": "SELECT name, open_mode, database_role, log_mode FROM v$database",
    "inst": "SELECT instance_name, status, host_name, version, startup_time FROM v$instance",
    "vers": "SELECT banner_full FROM v$version WHERE banner_full LIKE 'Oracle%'",
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
}


def check_one(target: DbTarget, timeout_sec: int = 25) -> DbHealth:
    t0 = time.time()
    try:
        # Establish connection
        with _connect(target) as conn:
            conn.call_timeout = timeout_sec * 1000
            cur = conn.cursor()

            # Version
            version = ""
            try:
                cur.execute(SQLS["vers"]) 
                row = cur.fetchone()
                if row:
                    version = row[0]
            except Exception:
                version = ""

            # DB
            cur.execute(SQLS["db"])
            name, open_mode, role, log_mode = cur.fetchone()

            # Instance
            cur.execute(SQLS["inst"]) 
            inst_name, inst_status, host_name, inst_version, startup_time = cur.fetchone()

            # Sessions
            sessions_total, sessions_active = 0, 0
            try:
                cur.execute(SQLS["sess"]) 
                total, active = cur.fetchone()
                sessions_total = int(total or 0)
                sessions_active = int(active or 0)
            except Exception:
                pass

            # Tablespaces worst usage
            worst_pct = None
            try:
                cur.execute(SQLS["tspace"]) 
                worst_pct = 0.0
                for ts_name, pct_used in cur.fetchall():
                    if pct_used is not None and pct_used > (worst_pct or 0):
                        worst_pct = float(pct_used)
            except Exception:
                worst_pct = None

            elapsed_ms = int((time.time() - t0) * 1000)
            return DbHealth(
                status="UP",
                details=f"Log:{log_mode}",
                version=version or inst_version,
                role=role,
                open_mode=open_mode,
                inst_status=inst_status,
                sessions_active=sessions_active,
                sessions_total=sessions_total,
                worst_ts_pct_used=worst_pct,
                host=host_name,
                elapsed_ms=elapsed_ms,
            )
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        return DbHealth(
            status="DOWN",
            details=str(e),
            elapsed_ms=elapsed_ms,
        )


class MonitorApp(ttk.Frame):
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

    # --- UI ---
    def _build_ui(self):
        topbar = ttk.Frame(self)
        topbar.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

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

        # Client lib
        ttk.Label(topbar, text="Client lib dir:").pack(side=tk.LEFT, padx=(10, 0))
        self.client_dir_var = tk.StringVar(value=self.cfg.get("client_lib_dir", ""))
        ttk.Entry(topbar, textvariable=self.client_dir_var, width=28).pack(side=tk.LEFT, padx=4)
        ttk.Button(topbar, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT)

        # Tree
        columns = (
            "Name", "Status", "InstStatus", "Role", "OpenMode", "Sessions", "WorstTS%", "Host", "Version", "Ms", "LastChecked"
        )
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=18)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=110 if col != "Version" else 320, stretch=True)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Styles for row tags
        style = ttk.Style()
        style.configure("Green.Treeview", background="#e8f5e9")
        style.configure("Yellow.Treeview", background="#fff8e1")
        style.configure("Red.Treeview", background="#ffebee")

        self.tree.tag_configure("UP_OK", background="#e8f5e9")
        self.tree.tag_configure("UP_WARN", background="#fff8e1")
        self.tree.tag_configure("DOWN", background="#ffebee")

        bottombar = ttk.Frame(self)
        bottombar.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(bottombar, textvariable=self.status_var).pack(side=tk.LEFT)

    def _pick_client_dir(self):
        d = filedialog.askdirectory(title="Select Oracle Client lib directory")
        if d:
            self.client_dir_var.set(d)
            self.cfg["client_lib_dir"] = d
            save_config(self.cfg)
            try:
                oracledb.init_oracle_client(lib_dir=d)
                messagebox.showinfo(APP_NAME, f"Oracle client initialized: {d}")
            except Exception as e:
                messagebox.showwarning(APP_NAME, f"Failed to init client: {e}")

    def _refresh_table(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for t in self.targets:
            self.tree.insert("", tk.END, iid=t.name, values=(t.name, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"))

    # --- Monitoring ---
    def start(self):
        if self._running:
            return
        self.interval_sec = self.interval_var.get()
        self._stop_flag.clear()
        self._running = True
        self.status_var.set(f"Monitoring every {self.interval_sec}s…")
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
        # schedule next
        if not self._stop_flag.is_set():
            self.after(self.interval_sec * 1000, self._loop)

    def _do_checks(self):
        if not self.targets:
            self.status_var.set("No targets configured")
            return
        self.status_var.set("Checking…")
        results: Dict[str, DbHealth] = {}
        futures = []
        for t in self.targets:
            futures.append(self._executor.submit(check_one, t))
        for t, fut in zip(self.targets, futures):
            try:
                res = fut.result(timeout=self.interval_var.get())
            except Exception as e:
                res = DbHealth(status="DOWN", details=str(e))
            results[t.name] = res
            self._update_row(t.name, res)
        self.status_var.set(f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _update_row(self, name: str, h: DbHealth):
        sessions = f"{h.sessions_active}/{h.sessions_total}" if h.sessions_total else "-"
        worst_ts = f"{h.worst_ts_pct_used:.1f}%" if h.worst_ts_pct_used is not None else "-"
        vals = (
            name,
            h.status,
            h.inst_status or "-",
            h.role or "-",
            h.open_mode or "-",
            sessions,
            worst_ts,
            h.host or "-",
            (h.version[:140] + "…") if len(h.version) > 140 else (h.version or "-"),
            h.elapsed_ms,
            h.ts,
        )
        tag = "DOWN"
        if h.status == "UP":
            # Warn if Inst not OPEN or TS > 85% or too many active sessions relative to total (> 60%)
            warn = False
            if (h.open_mode and "OPEN" not in h.open_mode.upper()) or (h.inst_status and h.inst_status.upper() != "OPEN"):
                warn = True
            if h.worst_ts_pct_used is not None and h.worst_ts_pct_used >= 85.0:
                warn = True
            if h.sessions_total and h.sessions_active / max(h.sessions_total, 1) >= 0.6:
                warn = True
            tag = "UP_WARN" if warn else "UP_OK"
        if name in self.tree.get_children():
            self.tree.item(name, values=vals, tags=(tag,))
        else:
            self.tree.insert("", tk.END, iid=name, values=vals, tags=(tag,))
        # Tooltip-like status if DOWN
        if h.status == "DOWN" and h.details:
            self.status_var.set(f"{name} DOWN: {h.details}")

    # --- CRUD on targets ---
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
        self.cfg["targets"] = [t.__dict__ for t in self.targets]
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
                "targets": [t.__dict__ for t in self.targets],
                "client_lib_dir": self.client_dir_var.get(),
            }
            with open(p, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo(APP_NAME, "Exported configuration.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Failed to export: {e}")


class DbEditor(tk.Toplevel):
    def __init__(self, parent: MonitorApp, target: Optional[DbTarget] = None, on_save=None):
        super().__init__(parent)
        self.title("DB Target")
        self.resizable(False, False)
        self.on_save = on_save
        self.parent = parent

        self.var_name = tk.StringVar(value=target.name if target else "")
        self.var_dsn = tk.StringVar(value=target.dsn if target else "")
        self.var_user = tk.StringVar(value=target.user if target else "")
        self.var_pwd = tk.StringVar(value=target.password if target else "")
        self.var_wallet = tk.StringVar(value=target.wallet_dir if target else "")
        self.var_mode = tk.StringVar(value=target.mode if target else "thick")

        frm = ttk.Frame(self, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)

        def row(lbl, var, show=None, browse=False):
            r = ttk.Frame(frm)
            r.pack(fill=tk.X, pady=4)
            ttk.Label(r, text=lbl, width=12).pack(side=tk.LEFT)
            e = ttk.Entry(r, textvariable=var, show=show, width=48)
            e.pack(side=tk.LEFT, padx=4)
            if browse:
                btn = ttk.Button(r, text="…", width=3, command=lambda: self._pick_dir(var))
                btn.pack(side=tk.LEFT)

        row("Name:", self.var_name)
        row("DSN:", self.var_dsn)
        row("User:", self.var_user)
        row("Password:", self.var_pwd, show="*")
        row("Wallet Dir:", self.var_wallet, browse=True)

        rmode = ttk.Frame(frm)
        rmode.pack(fill=tk.X, pady=4)
        ttk.Label(rmode, text="Mode:", width=12).pack(side=tk.LEFT)
        ttk.Radiobutton(rmode, text="Thick", value="thick", variable=self.var_mode).pack(side=tk.LEFT)
        ttk.Radiobutton(rmode, text="Thin", value="thin", variable=self.var_mode).pack(side=tk.LEFT)

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
            messagebox.showerror(APP_NAME, "Name and DSN are required")
            return
        t = DbTarget(
            name=name,
            dsn=dsn,
            user=self.var_user.get().strip() or None,
            password=self.var_pwd.get().strip() or None,
            wallet_dir=self.var_wallet.get().strip() or None,
            mode=self.var_mode.get().strip() or "thick",
        )
        if self.on_save:
            self.on_save(t)
        self.destroy()


def main():
    cfg = load_config()
    root = tk.Tk()
    # Improve default scaling on HiDPI
    try:
        if sys.platform.startswith("win"):
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)  # type: ignore
    except Exception:
        pass

    app = MonitorApp(root, cfg)
    root.geometry("1400x580")
    root.mainloop()


if __name__ == "__main__":
    main()
