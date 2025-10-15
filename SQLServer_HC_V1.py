import json
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog

# Optional secure password storage
try:
    import keyring
except Exception:
    keyring = None

APP_NAME = "SQL Server Health Monitor (sqlcmd)"

# --- Store config next to the script ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
SERVERS_PATH = os.path.join(CONFIG_DIR, "servers.json")
SETTINGS_PATH = os.path.join(CONFIG_DIR, "settings.json")

DEFAULT_SETTINGS = {
    "refresh_minutes": 5,
    "max_workers": 10,
    "sqlcmd_path": "sqlcmd",     # path or 'sqlcmd' if on PATH
    "separator": "|"
}

# ---------- Queries ----------
Q_CORE = """
SET NOCOUNT ON;
SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50)),
       CAST(SERVERPROPERTY('ProductUpdateLevel') AS nvarchar(50));
"""
Q_DB_COUNTS = """
SET NOCOUNT ON;
SELECT COUNT(*), SUM(CASE WHEN state_desc='ONLINE' THEN 1 ELSE 0 END)
FROM sys.databases;
"""
Q_AGENT = """
SET NOCOUNT ON;
SELECT TOP 1 status_desc
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%';
"""
Q_BACKUP = """
SET NOCOUNT ON;
;WITH lastfull AS (
  SELECT bs.database_name, MAX(bs.backup_finish_date) AS last_full_backup_finish_date
  FROM msdb.dbo.backupset bs
  WHERE bs.type='D'
  GROUP BY bs.database_name
)
SELECT MIN(last_full_backup_finish_date)
FROM lastfull;
"""
Q_DISK = """
SET NOCOUNT ON;
;WITH vols AS (
  SELECT DISTINCT
    vs.volume_mount_point,
    vs.total_bytes,
    vs.available_bytes,
    CAST(100.0 - (100.0 * vs.available_bytes / NULLIF(vs.total_bytes,0)) AS decimal(5,2)) AS used_pct
  FROM sys.master_files mf
  CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
)
SELECT TOP 1 volume_mount_point, total_bytes, available_bytes, used_pct
FROM vols
ORDER BY used_pct DESC;
"""

COLUMNS = [
    "S.No","SQL Server Instance","Environment","Version","CU","Instance Status",
    "Agent Status","totaldatabases/online databases","Oldest date of Last full backup of db",
    "Disk size with %","Last checked","Check Status","Error"
]

# ---- Config helpers ----
def ensure_paths():
    os.makedirs(CONFIG_DIR, exist_ok=True)
    if not os.path.isfile(SERVERS_PATH):
        with open(SERVERS_PATH, "w", encoding="utf-8") as f:
            json.dump({"servers": []}, f, indent=2)
    if not os.path.isfile(SETTINGS_PATH):
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_SETTINGS, f, indent=2)

def load_servers():
    ensure_paths()
    with open(SERVERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_servers(data):
    with open(SERVERS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def load_settings():
    ensure_paths()
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        s = json.load(f)
    for k, v in DEFAULT_SETTINGS.items():
        s.setdefault(k, v)
    return s

def save_settings(s):
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)

# ---- sqlcmd runner ----
def _sqlcmd_run(settings, instance, auth, username, password, query, sep):
    sqlcmd = settings.get("sqlcmd_path", "sqlcmd") or "sqlcmd"
    args = [sqlcmd, "-S", instance, "-W", "-h", "-1", "-s", sep, "-b", "-r", "1", "-Q", query]
    if auth == "windows":
        args.insert(2, "-E")
    else:
        args.extend(["-U", username or ""])
        args.extend(["-P", password or ""])
    try:
        res = subprocess.run(args, capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace")
    except FileNotFoundError:
        raise RuntimeError("sqlcmd not found. Set a valid path in Settings.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("sqlcmd timed out")

    if res.returncode != 0:
        msg = res.stderr.strip() or res.stdout.strip() or f"sqlcmd exited {res.returncode}"
        raise RuntimeError(msg)

    lines = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
    rows = [ln.split(sep) for ln in lines]
    return rows

def _get_password(instance, username):
    if not keyring: return None
    try: return keyring.get_password(f"{APP_NAME}:{instance}", username)
    except Exception: return None

def _set_password(instance, username, password):
    if not keyring: return False
    try:
        keyring.set_password(f"{APP_NAME}:{instance}", username, password)
        return True
    except Exception:
        return False

# ---- Health check ----
def check_instance(settings, server):
    instance = (server.get("instance") or "").strip()
    env = server.get("environment", "")
    auth = server.get("auth", "windows")
    user = server.get("username") if auth == "sql" else None
    save_pwd = bool(server.get("save_pwd")) if auth == "sql" else False

    row = {
        "S.No": 0,
        "SQL Server Instance": instance,
        "Environment": env,
        "Version": "",
        "CU": "",
        "Instance Status": "Down",
        "Agent Status": "Unknown",
        "totaldatabases/online databases": "",
        "Oldest date of Last full backup of db": "",
        "Disk size with %": "",
        "Last checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Check Status": "CRIT",
        "Error": ""
    }
    try:
        pwd = _get_password(instance, user) if auth == "sql" else None
        if auth == "sql" and (not pwd):
            raise RuntimeError("No stored SQL password. Edit the server and tick 'Save password', then provide it when prompted.")

        sep = settings.get("separator", "|")

        core = _sqlcmd_run(settings, instance, auth, user, pwd, Q_CORE, sep)
        row["Instance Status"] = "Up"
        if core and len(core[0]) >= 2:
            row["Version"] = core[0][0].strip()
            row["CU"] = (core[0][1] or "").strip()

        dbc = _sqlcmd_run(settings, instance, auth, user, pwd, Q_DB_COUNTS, sep)
        totaldb = int(dbc[0][0]) if dbc and dbc[0][0].isdigit() else 0
        onlinedb = int(dbc[0][1]) if dbc and dbc[0][1].isdigit() else 0
        row["totaldatabases/online databases"] = f"{totaldb}/{onlinedb}"

        try:
            ag = _sqlcmd_run(settings, instance, auth, user, pwd, Q_AGENT, sep)
            row["Agent Status"] = ag[0][0].strip() if ag and ag[0] and ag[0][0] else "Unknown"
        except Exception:
            row["Agent Status"] = "Unknown"

        bkp = _sqlcmd_run(settings, instance, auth, user, pwd, Q_BACKUP, sep)
        if bkp and bkp[0] and bkp[0][0]:
            row["Oldest date of Last full backup of db"] = bkp[0][0].strip()

        worst_pct = None
        try:
            dsk = _sqlcmd_run(settings, instance, auth, user, pwd, Q_DISK, sep)
            if dsk and len(dsk[0]) >= 4:
                mp, total_bytes, available_bytes, used_pct = dsk[0]
                try:
                    tot = float(total_bytes); av = float(available_bytes); used = float(used_pct)
                    tot_gb = round(tot / (1024**3), 2) if tot else 0.0
                    used_gb = round((tot - av) / (1024**3), 2) if tot else 0.0
                    row["Disk size with %"] = f"{mp} {used_gb}/{tot_gb} GB ({used}%)"
                    worst_pct = used
                except Exception:
                    pass
        except Exception:
            pass

        notes, crit, warn = [], False, False
        if row["Instance Status"] != "Up":
            crit = True; notes.append("Instance Down")
        if row["Agent Status"] == "Stopped":
            warn = True; notes.append("Agent Stopped")
        if totaldb > onlinedb:
            warn = True; notes.append("Some DBs not ONLINE")

        if row["Oldest date of Last full backup of db"]:
            ts = row["Oldest date of Last full backup of db"]
            from datetime import datetime as dt
            parsed = None
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try: parsed = dt.strptime(ts, fmt); break
                except: pass
            if parsed:
                days = (dt.now() - parsed).days
                if days >= 4: crit = True; notes.append(f"Oldest full backup {days}d")
                elif days >= 2: warn = True; notes.append(f"Oldest full backup {days}d")
            else:
                warn = True; notes.append("Backup date parse")
        else:
            warn = True; notes.append("No full backups found")

        if worst_pct is not None:
            if worst_pct >= 92: crit = True; notes.append(f"Disk {worst_pct}%")
            elif worst_pct >= 85: warn = True; notes.append(f"Disk {worst_pct}%")

        row["Check Status"] = "CRIT" if crit else ("WARN" if warn else "OK")
        row["Error"] = "; ".join(notes)

        if auth == "sql" and save_pwd and pwd:
            _set_password(instance, user, pwd)

        return row
    except Exception as e:
        row["Error"] = str(e)
        row["Check Status"] = "CRIT"
        return row

# ---- Dialogs ----
class ServerDialog(tk.Toplevel):
    def __init__(self, master, server=None):
        super().__init__(master)
        self.title("Server")
        self.resizable(False, False)
        self.result = None
        if master: self.transient(master)

        data = server or {"instance":"", "environment":"", "auth":"windows", "username":"", "save_pwd": False}

        r = 0
        ttk.Label(self, text="Instance (SERVER or SERVER\\INSTANCE or tcp:server,port):").grid(row=r, column=0, sticky="w", padx=8, pady=(10,2)); r+=1
        self.e_instance = ttk.Entry(self, width=44); self.e_instance.insert(0, data.get("instance",""))
        self.e_instance.grid(row=r, column=0, columnspan=2, padx=8, pady=2, sticky="we"); r+=1

        ttk.Label(self, text="Environment:").grid(row=r, column=0, sticky="w", padx=8, pady=(6,2)); r+=1
        self.e_env = ttk.Entry(self, width=20); self.e_env.insert(0, data.get("environment",""))
        self.e_env.grid(row=r, column=0, padx=8, pady=2, sticky="w"); r+=1

        ttk.Label(self, text="Authentication:").grid(row=r, column=0, sticky="w", padx=8, pady=(6,2)); r+=1
        self.auth_var = tk.StringVar(value=data.get("auth","windows"))
        ttk.Radiobutton(self, text="Windows (AD) - current user", variable=self.auth_var, value="windows", command=self._toggle).grid(row=r, column=0, sticky="w", padx=8); r+=1
        ttk.Radiobutton(self, text="SQL Login", variable=self.auth_var, value="sql", command=self._toggle).grid(row=r, column=0, sticky="w", padx=8); r+=1

        self.frm_sql = ttk.Frame(self)
        ttk.Label(self.frm_sql, text="SQL Username:").grid(row=0, column=0, sticky="w")
        self.e_user = ttk.Entry(self.frm_sql, width=26); self.e_user.insert(0, data.get("username",""))
        self.e_user.grid(row=0, column=1, sticky="w", padx=6, pady=2)
        self.save_pwd_var = tk.BooleanVar(value=bool(data.get("save_pwd", False)))
        ttk.Checkbutton(self.frm_sql, text="Save password (Credential Manager)", variable=self.save_pwd_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=(2,8))
        self.frm_sql.grid(row=r, column=0, padx=8, pady=2, sticky="we"); r+=1

        btns = ttk.Frame(self)
        ttk.Button(btns, text="OK", command=self._ok).grid(row=0, column=0, padx=6)
        ttk.Button(btns, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=6)
        btns.grid(row=r, column=0, pady=10)

        self._toggle()
        self.update_idletasks()
        self.grab_set()
        self.e_instance.focus_set()

    def _toggle(self):
        is_sql = (self.auth_var.get() == "sql")
        state = "normal" if is_sql else "disabled"
        self.frm_sql.configure(state=state)
        for w in self.frm_sql.winfo_children(): w.configure(state=state)

    def _ok(self):
        instance = self.e_instance.get().strip()
        env = self.e_env.get().strip()
        auth = self.auth_var.get()
        user = self.e_user.get().strip() if auth == "sql" else ""
        if not instance:
            messagebox.showerror("Error", "Instance is required.")
            return
        if auth == "sql" and not user:
            messagebox.showerror("Error", "SQL username is required for SQL auth.")
            return
        self.result = {
            "instance": instance, "environment": env, "auth": auth,
            "username": user if auth == "sql" else "", "save_pwd": bool(self.save_pwd_var.get()) if auth == "sql" else False
        }
        self.destroy()

class SettingsDialog(tk.Toplevel):
    def __init__(self, master, settings):
        super().__init__(master)
        self.title("Settings")
        self.resizable(False, False)
        if master: self.transient(master)
        self.settings = dict(settings)

        def row(lbl, key, width=28, ewidth=28):
            frm = ttk.Frame(self); ttk.Label(frm, text=lbl, width=width).pack(side="left")
            var = tk.StringVar(value=str(self.settings.get(key, "")))
            ent = ttk.Entry(frm, textvariable=var, width=ewidth); ent.pack(side="left")
            frm.pack(fill="x", padx=10, pady=4); return var

        self.v_sqlcmd  = row("sqlcmd path (or 'sqlcmd'):", "sqlcmd_path", ewidth=36)
        self.v_refresh = row("Auto refresh minutes:", "refresh_minutes", ewidth=8)
        self.v_workers = row("Max parallel workers:", "max_workers", ewidth=8)

        btns = ttk.Frame(self)
        ttk.Button(btns, text="Save", command=self._save).grid(row=0, column=0, padx=6)
        ttk.Button(btns, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=6)
        btns.pack(pady=8)
        self.grab_set()

    def _save(self):
        try:
            s = self.settings
            s["sqlcmd_path"] = self.v_sqlcmd.get().strip() or "sqlcmd"
            s["refresh_minutes"] = max(1, int(self.v_refresh.get()))
            s["max_workers"] = max(1, int(self.v_workers.get()))
            save_settings(s); self.destroy()
        except Exception as e:
            messagebox.showerror("Error", str(e))

# ---- App ----
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1300x720")
        self.settings = load_settings()
        self.servers = load_servers()
        self.data_rows = []
        self._timer_id = None

        bar = ttk.Frame(self)
        ttk.Button(bar, text="Refresh Now", command=self.refresh).pack(side="left", padx=5, pady=6)
        ttk.Button(bar, text="Export CSV", command=self.export_csv).pack(side="left", padx=5)
        ttk.Button(bar, text="Manage Servers", command=self.manage_servers).pack(side="left", padx=5)
        ttk.Button(bar, text="Settings", command=self.open_settings).pack(side="left", padx=5)
        self.status_lbl = ttk.Label(bar, text=""); self.status_lbl.pack(side="left", padx=15)
        bar.pack(fill="x")

        self.tree = ttk.Treeview(self, columns=COLUMNS, show="headings")
        for col in COLUMNS:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=170 if col not in ("S.No","CU") else 70, anchor="w")
        self.tree.pack(fill="both", expand=True)
        self.tree.tag_configure("CRIT", background="#FDE7E9")
        self.tree.tag_configure("WARN", background="#FFF8E1")
        self.tree.tag_configure("OK",   background="#E8F5E9")

        self.after(500, self.refresh)
        self._schedule_next()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        if self._timer_id: self.after_cancel(self._timer_id)
        self.destroy()

    def _schedule_next(self):
        mins = int(self.settings.get("refresh_minutes", 5))
        self._timer_id = self.after(mins * 60 * 1000, self._auto_tick)

    def _auto_tick(self):
        self.refresh()
        self._schedule_next()

    def set_status(self, txt):
        self.status_lbl.config(text=txt)
        self.status_lbl.update_idletasks()

    def refresh(self):
        srvs = self.servers.get("servers", [])
        if not srvs:
            messagebox.showinfo("Add servers", "No servers configured. Click 'Manage Servers' to add.")
            return
        self.set_status("Checking...")
        self.tree.delete(*self.tree.get_children()); self.data_rows = []

        def work():
            rows = []
            with ThreadPoolExecutor(max_workers=int(self.settings.get("max_workers", 10))) as ex:
                futs = {ex.submit(check_instance, self.settings, s): s for s in srvs}
                done = 0
                for f in as_completed(futs):
                    rows.append(f.result()); done += 1
                    self.set_status(f"Checked {done}/{len(srvs)}")
            rows.sort(key=lambda r: (r.get("Environment",""), r.get("SQL Server Instance","")))
            for i, r in enumerate(rows, start=1): r["S.No"] = i
            self.data_rows = rows
            self.after(0, self._bind_rows)

        threading.Thread(target=work, daemon=True).start()

    def _bind_rows(self):
        for r in self.data_rows:
            vals = [r.get(c, "") for c in COLUMNS]
            tag = r.get("Check Status", "OK")
            self.tree.insert("", "end", values=vals, tags=(tag,))
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.set_status(f"Instances: {len(self.data_rows)} | Refreshed: {ts}")

    def export_csv(self):
        if not self.data_rows: return
        path = filedialog.asksaveasfilename(
            title="Export CSV", defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"SqlHealth_{datetime.now():%Y%m%d_%H%M%S}.csv"
        )
        if not path: return
        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f); w.writerow(COLUMNS)
                for r in self.data_rows: w.writerow([r.get(c,"") for c in COLUMNS])
            messagebox.showinfo("Export", f"Exported to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export", str(e))

    def manage_servers(self):
        dlg = tk.Toplevel(self); dlg.title("Manage Servers"); dlg.geometry("720x380"); dlg.transient(self); dlg.grab_set()

        cols = ("instance","environment","auth","username","save_pwd")
        tv = ttk.Treeview(dlg, columns=cols, show="headings", selectmode="browse")
        for c in cols:
            tv.heading(c, text=c)
            tv.column(c, width=150 if c!="instance" else 260, anchor="w")
        tv.pack(fill="both", expand=True, padx=8, pady=8)

        def reload_list():
            tv.delete(*tv.get_children())
            for s in self.servers.get("servers", []):
                tv.insert("", "end", values=(s.get("instance",""), s.get("environment",""),
                                             s.get("auth",""), s.get("username",""),
                                             "Yes" if s.get("save_pwd") else "No"))

        def add_server():
            sd = ServerDialog(dlg)
            self.wait_window(sd)
            if sd.result:
                if sd.result["auth"] == "sql" and sd.result.get("save_pwd") and keyring:
                    pwd = simpledialog.askstring("Password",
                        f"Enter password for {sd.result['username']}@{sd.result['instance']}",
                        show="*", parent=dlg)
                    if pwd: _set_password(sd.result["instance"], sd.result["username"], pwd)
                self.servers.setdefault("servers", []).append(sd.result)
                save_servers(self.servers); reload_list()

        def edit_server():
            sel = tv.selection()
            if not sel: return
            idx = tv.index(sel[0])
            current = self.servers.get("servers", [])[idx]
            sd = ServerDialog(dlg, current)
            self.wait_window(sd)
            if sd.result:
                if sd.result["auth"] == "sql" and sd.result.get("save_pwd") and keyring:
                    if messagebox.askyesno("Password", "Update stored password now?", parent=dlg):
                        pwd = simpledialog.askstring("Password",
                              f"Enter password for {sd.result['username']}@{sd.result['instance']}",
                              show="*", parent=dlg)
                        if pwd: _set_password(sd.result["instance"], sd.result["username"], pwd)
                self.servers["servers"][idx] = sd.result
                save_servers(self.servers); reload_list()

        def delete_server():
            sel = tv.selection()
            if not sel: return
            idx = tv.index(sel[0])
            inst = self.servers.get("servers", [])[idx].get("instance","")
            if messagebox.askyesno("Delete", f"Remove '{inst}'?", parent=dlg):
                self.servers["servers"].pop(idx)
                save_servers(self.servers); reload_list()

        btns = ttk.Frame(dlg)
        ttk.Button(btns, text="Add", command=add_server).pack(side="left", padx=5)
        ttk.Button(btns, text="Edit", command=edit_server).pack(side="left", padx=5)
        ttk.Button(btns, text="Delete", command=delete_server).pack(side="left", padx=5)
        ttk.Button(btns, text="Close", command=dlg.destroy).pack(side="right", padx=5)
        btns.pack(fill="x", padx=8, pady=(0,8))

        reload_list()

    def open_settings(self):
        sd = SettingsDialog(self, self.settings)
        self.wait_window(sd)
        self.settings = load_settings()
        if self._timer_id:
            self.after_cancel(self._timer_id)
        self._schedule_next()

if __name__ == "__main__":
    App().mainloop()
