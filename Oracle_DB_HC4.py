#!/usr/bin/env python3
"""
Oracle DB Health GUI Monitor (tksheet grid, per-cell red/green bold text)
- Tkinter + tksheet to allow PER-CELL styling
- Uses python-oracledb with Oracle Client (thick recommended)
- Checks every N seconds (default 300)
- Config: ~/.ora_gui_monitor/config.json
- Columns (order fixed):
  DB Name, Host, Status, Inst_status, Role, OpenMode, Sessions, WorstTS%,
  LastFull/Inc, LastArch, DB Version, Ms, LastChecked, Error
- Coloring rules (per cell, text only):
  * Status: GREEN if UP else RED (bold)
  * Inst_status: GREEN if OPEN else RED (bold)
  * OpenMode: GREEN if contains OPEN else RED (bold)
  * WorstTS%: RED if >= 90.0 else GREEN (bold)
  * LastArch: RED if older than 12 hours; GREEN otherwise (bold; missing value = RED)
  * LastFull/Inc: RED if older than 3 days; GREEN otherwise (bold; missing value = RED)
Install once:
    pip install python-oracledb tksheet
Run:
    python oracle_db_health_gui_tksheet.py
"""
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import tkinter.font as tkfont

# Grid (per-cell styling)
try:
    from tksheet import Sheet
except Exception as e:
    raise SystemExit(
        "tksheet is required for per-cell colors. Install with: pip install tksheet\n"
        "Error: {}".format(e)
    )

# Oracle
try:
    import oracledb
except Exception:
    oracledb = None  # defer error until connect

APP_NAME = "Oracle DB Health GUI Monitor"
CONFIG_DIR = Path.home() / ".ora_gui_monitor"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "config.json"

ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
DEFAULT_INTERVAL_SEC = 300
MAX_WORKERS = 8

@dataclass
class DbTarget:
    name: str
    dsn: str
    user: Optional[str] = None
    password: Optional[str] = None
    wallet_dir: Optional[str] = None
    mode: str = "thin"

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

def load_config() -> Dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"interval_sec": DEFAULT_INTERVAL_SEC, "targets": [], "client_lib_dir": ORACLE_CLIENT_LIB_DIR or ""}

def save_config(cfg: Dict):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        messagebox.showerror(APP_NAME, "Failed to save config: {}".format(e))

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
            messagebox.showwarning(APP_NAME, "Oracle client init issue: {}\nProceeding in thin mode if possible.".format(e))

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
    "db": (
        """
        SELECT name, open_mode, database_role, log_mode
        FROM v$database
        """
    ),
    "inst": (
        """
        SELECT instance_name, status, host_name, version, startup_time
        FROM v$instance
        """
    ),
    "sess": (
        """
        SELECT COUNT(*) total,
               SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END) active
        FROM v$session
        WHERE type='USER'
        """
    ),
    "tspace": (
        """
        SELECT ts.tablespace_name,
               ROUND((1 - NVL(fs.free_mb,0)/ts.size_mb)*100,2) pct_used
        FROM (
            SELECT tablespace_name, SUM(bytes)/1024/1024 size_mb
            FROM dba_data_files
            GROUP BY tablespace_name
        ) ts
        LEFT JOIN (
            SELECT tablespace_name, SUM(bytes)/1024/1024 free_mb
            FROM dba_free_space
            GROUP BY tablespace_name
        ) fs
        ON ts.tablespace_name = fs.tablespace_name
        """
    ),
    "bk_data": (
        """
        SELECT MAX(bp.completion_time)
        FROM v$backup_set bs
        JOIN v$backup_piece bp
          ON bs.set_stamp = bp.set_stamp AND bs.set_count = bp.set_count
        WHERE bs.backup_type = 'D'
        """
    ),
    "bk_arch": (
        """
        SELECT MAX(bp.completion_time)
        FROM v$backup_set bs
        JOIN v$backup_piece bp
          ON bs.set_stamp = bp.set_stamp AND bs.set_count = bp.set_count
        WHERE bs.backup_type = 'L'
        """
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

            cur.execute(SQLS["db"])  # name, open_mode, role, log_mode
            name, open_mode, role, log_mode = cur.fetchone()

            cur.execute(SQLS["inst"])  # inst_name, inst_status, host_name, version, startup_time
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
                details="Log:{}".format(log_mode),
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
    COLS = [
        "DB Name", "Host", "Status", "Inst_status", "Role", "OpenMode", "Sessions",
        "WorstTS%", "LastFull/Inc", "LastArch", "DB Version", "Ms", "LastChecked", "Error"
    ]

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

        ttk.Label(topbar, text="Client lib dir:").pack(side=tk.LEFT, padx=(10, 0))
        self.client_dir_var = tk.StringVar(value=self.cfg.get("client_lib_dir", ""))
        ttk.Entry(topbar, textvariable=self.client_dir_var, width=28).pack(side=tk.LEFT, padx=4)
        ttk.Button(topbar, text="Browse", command=self._pick_client_dir).pack(side=tk.LEFT)

        # Fonts for emphasis
        try:
            base_font = tkfont.nametofont("TkDefaultFont")
            self.bold_font = base_font.copy()
            self.bold_font.configure(weight="bold")
        except Exception:
            self.bold_font = None

        # Sheet grid
        self.sheet = Sheet(self, headers=self.COLS)
        self.sheet.enable_bindings((
            "single_select",
            "row_select",
            "drag_select",
            "column_width_resize",
            "arrowkeys",
            "copy",
        ))
        self.sheet.grid(row=1, column=0, sticky="nsew", padx=8, pady=8)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)

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
                if oracledb is not None:
                    oracledb.init_oracle_client(lib_dir=d)
                messagebox.showinfo(APP_NAME, "Oracle client initialized: {}".format(d))
            except Exception as e:
                messagebox.showwarning(APP_NAME, "Failed to init client: {}".format(e))

    def _refresh_table(self):
        data = []
        for t in self.targets:
            row = [t.name, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]
            data.append(row)
        self.sheet.set_sheet_data(data, reset_col_positions=True, reset_row_positions=True)
        self.sheet.refresh()

    # Monitoring
    def start(self):
        if self._running:
            return
        self.interval_sec = self.interval_var.get()
        self._stop_flag.clear()
        self._running = True
        self.status_var.set("Monitoring every {}s...".format(self.interval_sec))
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
        for idx, t in enumerate(self.targets):
            try:
                res = check_one(t)
            except Exception as e:
                res = DbHealth(status="DOWN", details=str(e), error=str(e))
            self._update_row(idx, res)
        self.status_var.set("Last run: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def _set_cell(self, row: int, col_name: str, value: str):
        col = self.COLS.index(col_name)
        self.sheet.set_cell_data(row, col, value, redraw=False)

    def _color_cell(self, row: int, col_name: str, ok: bool):
        col = self.COLS.index(col_name)
        fg = "#1b5e20" if ok else "#b71c1c"  # green or red
        font = self.bold_font if self.bold_font else None
        self.sheet.highlight_cells(row=row, column=col, fg=fg, font=font, redraw=False)

    def _update_row(self, row: int, h: DbHealth):
        # Fill basic values
        self._set_cell(row, "DB Name", self.targets[row].name)
        self._set_cell(row, "Host", h.host or "-")
        self._set_cell(row, "Status", h.status)
        self._set_cell(row, "Inst_status", h.inst_status or "-")
        self._set_cell(row, "Role", h.role or "-")
        self._set_cell(row, "OpenMode", h.open_mode or "-")
        self._set_cell(row, "Sessions", "{}/{}".format(h.sessions_active, h.sessions_total) if h.sessions_total else "-")
        self._set_cell(row, "WorstTS%", "{:.1f}%".format(h.worst_ts_pct_used) if h.worst_ts_pct_used is not None else "-")
        self._set_cell(row, "LastFull/Inc", _dt_str(h.last_full_inc_backup))
        self._set_cell(row, "LastArch", _dt_str(h.last_arch_backup))
        self._set_cell(row, "DB Version", h.version or "-")
        self._set_cell(row, "Ms", str(h.elapsed_ms))
        self._set_cell(row, "LastChecked", h.ts)
        self._set_cell(row, "Error", h.error or ("" if h.status=="UP" else h.details))

        # Apply per-cell coloring rules (bold red or green)
        self._color_cell(row, "Status", h.status.upper() == "UP")
        self._color_cell(row, "Inst_status", (h.inst_status or "").upper() == "OPEN")
        self._color_cell(row, "OpenMode", "OPEN" in (h.open_mode or "").upper())
        worst_ok = not (h.worst_ts_pct_used is not None and h.worst_ts_pct_used >= 90.0)
        self._color_cell(row, "WorstTS%", worst_ok)

        if h.last_arch_backup:
            hours = (datetime.now(h.last_arch_backup.tzinfo) - h.last_arch_backup).total_seconds() / 3600.0
            self._color_cell(row, "LastArch", hours <= 12)
        else:
            self._color_cell(row, "LastArch", False)

        if h.last_full_inc_backup:
            days = (datetime.now(h.last_full_inc_backup.tzinfo) - h.last_full_inc_backup).total_seconds() / 86400.0
            self._color_cell(row, "LastFull/Inc", days <= 3)
        else:
            self._color_cell(row, "LastFull/Inc", False)

        self.sheet.refresh()

    # CRUD
    def _add_dialog(self):
        DbEditor(self, on_save=self._add_target)

    def _edit_selected(self):
        sel = self.sheet.get_selected_rows()
        if not sel:
            messagebox.showinfo(APP_NAME, "Select a row to edit.")
            return
        row = sel[0]
        t = self.targets[row]
        DbEditor(self, target=t, on_save=self._update_target)

    def _remove_selected(self):
        sel = self.sheet.get_selected_rows()
        if not sel:
            return
        row = sel[0]
        del self.targets[row]
        self._persist_targets()
        self._refresh_table()

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
            messagebox.showerror(APP_NAME, "Failed to import: {}".format(e))

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
            messagebox.showerror(APP_NAME, "Failed to export: {}".format(e))

class DbEditor(tk.Toplevel):
    def __init__(self, parent: "MonitorApp", target: Optional[DbTarget] = None, on_save=None):
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
            ttk.Label(r, text=lbl, width=20).pack(side=tk.LEFT)
            e = ttk.Entry(r, textvariable=var, show=show, width=48)
            e.pack(side=tk.LEFT, padx=4)
            if browse:
                btn = ttk.Button(r, text="...", width=3, command=lambda: self._pick_dir(var))
                btn.pack(side=tk.LEFT)

        row("DB Name:", self.var_name)
        row("TNS Alias / EZConnect:", self.var_dsn)
        row("User:", self.var_user)
        row("Password:", self.var_pwd, show="*")
        row("Wallet Dir:", self.var_wallet, browse=True)

        rmode = ttk.Frame(frm)
        rmode.pack(fill=tk.X, pady=4)
        ttk.Label(rmode, text="Mode:", width=20).pack(side=tk.LEFT)
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
            messagebox.showerror(APP_NAME, "DB Name and TNS Alias/EZConnect are required")
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
    try:
        if sys.platform.startswith("win"):
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)  # type: ignore
    except Exception:
        pass

    app = MonitorApp(root, cfg)
    root.geometry("1700x650")
    root.mainloop()

if __name__ == "__main__":
    main()
