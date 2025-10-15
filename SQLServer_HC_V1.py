import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

# Third-party
import pyodbc
try:
    import keyring
except ImportError:
    keyring = None  # optional; we handle gracefully

APP_NAME = "SQL Server Health Monitor"
CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
SERVERS_PATH = os.path.join(CONFIG_DIR, "servers.json")
SETTINGS_PATH = os.path.join(CONFIG_DIR, "settings.json")

DEFAULT_SETTINGS = {
    "refresh_minutes": 5,      # not auto-used; manual Refresh button provided; you can wire a timer later
    "backup_warn_days": 2,
    "backup_crit_days": 4,
    "disk_warn_pct": 85,
    "disk_crit_pct": 92,
    "max_workers": 12,
    "odbc_driver": "ODBC Driver 18 for SQL Server",  # 18 preferred; change to 17 if needed
    "trust_server_cert": True
}

# -------------- Utilities: config load/save ----------------
def ensure_paths():
    if not os.path.isdir(CONFIG_DIR):
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
    # backfill defaults if keys missing
    for k, v in DEFAULT_SETTINGS.items():
        s.setdefault(k, v)
    return s

def save_settings(settings):
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)

# -------------- DB queries ----------------
Q_CORE = """
SELECT
  CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50)) AS ProductVersion,
  CAST(SERVERPROPERTY('ProductUpdateLevel') AS nvarchar(50)) AS ProductUpdateLevel
"""

Q_DB_COUNTS = """
SELECT
  COUNT(*) AS totaldb,
  SUM(CASE WHEN state_desc='ONLINE' THEN 1 ELSE 0 END) AS onlinedb
FROM sys.databases
"""

Q_AGENT = """
SELECT TOP 1 status_desc
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%'
"""

Q_BACKUP = """
;WITH lastfull AS (
  SELECT bs.database_name, MAX(bs.backup_finish_date) AS last_full_backup_finish_date
  FROM msdb.dbo.backupset bs
  WHERE bs.type = 'D'
  GROUP BY bs.database_name
)
SELECT MIN(last_full_backup_finish_date) AS oldest_full_backup
FROM lastfull
"""

Q_DISK = """
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
ORDER BY used_pct DESC
"""

def make_conn_str(settings, instance, auth, username=None, password=None):
    driver = settings.get("odbc_driver", "ODBC Driver 18 for SQL Server")
    trust = settings.get("trust_server_cert", True)
    trust_part = "Yes" if trust else "No"
    if auth == "windows":
        return (
            f"DRIVER={{{driver}}};SERVER={instance};DATABASE=master;"
            f"Trusted_Connection=Yes;TrustServerCertificate={trust_part};"
        )
    else:
        if username is None or password is None:
            raise ValueError("Username/password required for SQL authentication.")
        return (
            f"DRIVER={{{driver}}}};SERVER={instance};DATABASE=master;"
            f"UID={username};PWD={password};TrustServerCertificate={trust_part};"
        )

def get_sql_password_from_keyring(instance, username):
    if keyring is None:
        return None
    return keyring.get_password(f"{APP_NAME}:{instance}", username)

def set_sql_password_in_keyring(instance, username, password):
    if keyring is None:
        return False
    try:
        keyring.set_password(f"{APP_NAME}:{instance}", username, password)
        return True
    except Exception:
        return False

def test_instance(settings, server):
    """
    server dict:
      {
        "instance": "HOST\\NAME",
        "environment": "PROD",
        "auth": "windows"|"sql",
        "username": "user" (only for sql),
        "save_pwd": true|false
      }
    """
    instance = server.get("instance", "").strip()
    env = server.get("environment", "")
    auth = server.get("auth", "windows")
    username = server.get("username")
    show_error = ""

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
        pwd = None
        if auth == "sql":
            # Try keyring first
            pwd = get_sql_password_from_keyring(instance, username) if username else None

        conn_str = make_conn_str(settings, instance, auth, username, pwd) if auth == "sql" else make_conn_str(settings, instance, "windows")
        cn = pyodbc.connect(conn_str, timeout=15, autocommit=True)
        row["Instance Status"] = "Up"

        # Core
        cur = cn.cursor()
        ver, cu = "", ""
        for r in cur.execute(Q_CORE):
            ver = (r.ProductVersion or "").strip()
            cu  = (r.ProductUpdateLevel or "").strip()
        row["Version"] = ver
        row["CU"] = cu

        # DB counts
        totaldb, onlinedb = 0, 0
        for r in cur.execute(Q_DB_COUNTS):
            totaldb = int(r.totaldb or 0); onlinedb = int(r.onlinedb or 0)
        row["totaldatabases/online databases"] = f"{totaldb}/{onlinedb}"

        # Agent
        try:
            ag = None
            for r in cur.execute(Q_AGENT):
                ag = r.status_desc
                break
            row["Agent Status"] = ag if ag else "Unknown"
        except Exception:
            row["Agent Status"] = "Unknown"

        # Oldest full backup
        oldest = None
        for r in cur.execute(Q_BACKUP):
            oldest = r.oldest_full_backup
        if oldest:
            # format nice
            if isinstance(oldest, datetime):
                row["Oldest date of Last full backup of db"] = oldest.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # pyodbc may return date-like; cast
                row["Oldest date of Last full backup of db"] = str(oldest)
        else:
            row["Oldest date of Last full backup of db"] = ""

        # Disk worst usage
        worst_pct = None
        try:
            r = cur.execute(Q_DISK).fetchone()
            if r:
                mp = r.volume_mount_point
                tot = float(r.total_bytes or 0.0)
                av  = float(r.available_bytes or 0.0)
                used = float(r.used_pct or 0.0)
                tot_gb = round(tot/ (1024**3), 2) if tot else 0.0
                used_gb = round((tot-av)/ (1024**3), 2) if tot else 0.0
                row["Disk size with %"] = f"{mp} {used_gb}/{tot_gb} GB ({used}%)"
                worst_pct = used
        except Exception:
            pass

        # Status logic
        s = settings
        crit, warn = False, False
        notes = []

        if row["Instance Status"] != "Up":
            crit = True; notes.append("Instance Down")

        if row["Agent Status"] == "Stopped":
            warn = True; notes.append("Agent Stopped")

        if totaldb > onlinedb:
            warn = True; notes.append("Some DBs not ONLINE")

        # backup age
        if row["Oldest date of Last full backup of db"]:
            try:
                dt = datetime.strptime(row["Oldest date of Last full backup of db"], "%Y-%m-%d %H:%M:%S")
                days = (datetime.now() - dt).days
                if days >= s["backup_crit_days"]:
                    crit = True; notes.append(f"Oldest full backup {days}d")
                elif days >= s["backup_warn_days"]:
                    warn = True; notes.append(f"Oldest full backup {days}d")
            except Exception:
                warn = True; notes.append("Backup date parse")
        else:
            warn = True; notes.append("No full backups found")

        if worst_pct is not None:
            if worst_pct >= s["disk_crit_pct"]:
                crit = True; notes.append(f"Disk {worst_pct}%")
            elif worst_pct >= s["disk_warn_pct"]:
                warn = True; notes.append(f"Disk {worst_pct}%")

        row["Check Status"] = "CRIT" if crit else ("WARN" if warn else "OK")
        row["Error"] = "; ".join(notes)
        cn.close()
        return row

    except pyodbc.Error as e:
        row["Error"] = str(e)
        row["Check Status"] = "CRIT"
        return row
    except Exception as e:
        row["Error"] = str(e)
        row["Check Status"] = "CRIT"
        return row

# -------------- GUI ----------------
COLUMNS = [
    "S.No",
    "SQL Server Instance",
    "Environment",
    "Version",
    "CU",
    "Instance Status",
    "Agent Status",
    "totaldatabases/online databases",
    "Oldest date of Last full backup of db",
    "Disk size with %",
    "Last checked",
    "Check Status",
    "Error"
]

class ServerDialog(tk.Toplevel):
    def __init__(self, master, server=None):
        super().__init__(master)
        self.title("Server")
        self.resizable(False, False)
        self.result = None

        # defaults
        data = server or {"instance":"", "environment":"", "auth":"windows", "username":"", "save_pwd": False}
        row = 0

        ttk.Label(self, text="Instance (SERVER or SERVER\\INSTANCE):").grid(row=row, column=0, sticky="w", padx=8, pady=(10,2)); row+=1
        self.e_instance = ttk.Entry(self, width=40)
        self.e_instance.insert(0, data.get("instance",""))
        self.e_instance.grid(row=row, column=0, columnspan=2, padx=8, pady=2, sticky="we"); row+=1

        ttk.Label(self, text="Environment:").grid(row=row, column=0, sticky="w", padx=8, pady=(6,2)); row+=1
        self.e_env = ttk.Entry(self, width=20)
        self.e_env.insert(0, data.get("environment",""))
        self.e_env.grid(row=row, column=0, padx=8, pady=2, sticky="w"); row+=1

        ttk.Label(self, text="Authentication:").grid(row=row, column=0, sticky="w", padx=8, pady=(6,2)); row+=1
        self.auth_var = tk.StringVar(value=data.get("auth","windows"))
        rb1 = ttk.Radiobutton(self, text="Windows (AD) - current user", variable=self.auth_var, value="windows", command=self._toggle_auth)
        rb2 = ttk.Radiobutton(self, text="SQL Login", variable=self.auth_var, value="sql", command=self._toggle_auth)
        rb1.grid(row=row, column=0, sticky="w", padx=8); row+=1
        rb2.grid(row=row, column=0, sticky="w", padx=8); row+=1

        self.frm_sql = ttk.Frame(self)
        ttk.Label(self.frm_sql, text="SQL Username:").grid(row=0, column=0, sticky="w")
        self.e_user = ttk.Entry(self.frm_sql, width=25)
        self.e_user.insert(0, data.get("username",""))
        self.e_user.grid(row=0, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(self.frm_sql, text="SQL Password:").grid(row=1, column=0, sticky="w")
        self.e_pwd = ttk.Entry(self.frm_sql, width=25, show="*")
        self.e_pwd.grid(row=1, column=1, sticky="w", padx=6, pady=2)

        self.save_pwd_var = tk.BooleanVar(value=bool(data.get("save_pwd", False)))
        self.chk_save = ttk.Checkbutton(self.frm_sql, text="Save password to Windows Credential Manager", variable=self.save_pwd_var)
        self.chk_save.grid(row=2, column=0, columnspan=2, sticky="w", pady=(4,6))

        self.frm_sql.grid(row=row, column=0, padx=8, pady=2, sticky="we"); row+=1

        btns = ttk.Frame(self)
        ttk.Button(btns, text="OK", command=self._ok).grid(row=0, column=0, padx=6)
        ttk.Button(btns, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=6)
        btns.grid(row=row, column=0, pady=10)

        self._toggle_auth()
        self.grab_set()
        self.e_instance.focus_set()

    def _toggle_auth(self):
        is_sql = (self.auth_var.get() == "sql")
        self.frm_sql.configure(state=("normal" if is_sql else "disabled"))
        for child in self.frm_sql.winfo_children():
            child.configure(state=("normal" if is_sql else "disabled"))

    def _ok(self):
        instance = self.e_instance.get().strip()
        env = self.e_env.get().strip()
        auth = self.auth_var.get()
        user = self.e_user.get().strip() if auth == "sql" else ""
        pwd = self.e_pwd.get() if auth == "sql" else ""
        save_pwd = self.save_pwd_var.get() if auth == "sql" else False

        if not instance:
            messagebox.showerror("Error", "Instance is required.")
            return
        if auth == "sql" and not user:
            messagebox.showerror("Error", "SQL username is required.")
            return

        # Optionally save password
        if auth == "sql" and save_pwd and pwd:
            if keyring is None:
                messagebox.showwarning("Keyring unavailable", "keyring package is not installed; password cannot be saved. It will still be used for this session.")
            else:
                ok = set_sql_password_in_keyring(instance, user, pwd)
                if not ok:
                    messagebox.showwarning("Keyring", "Failed to save password in keyring.")

        self.result = {
            "instance": instance,
            "environment": env,
            "auth": auth,
            "username": user if auth == "sql" else "",
            "save_pwd": bool(save_pwd) if auth == "sql" else False
        }
        self.destroy()

class SettingsDialog(tk.Toplevel):
    def __init__(self, master, settings):
        super().__init__(master)
        self.title("Settings")
        self.resizable(False, False)
        self.settings = dict(settings)

        def add_row(lbl, key, width=10):
            frame = ttk.Frame(self)
            ttk.Label(frame, text=lbl, width=28).pack(side="left")
            var = tk.StringVar(value=str(self.settings.get(key, "")))
            ent = ttk.Entry(frame, textvariable=var, width=width)
            ent.pack(side="left")
            frame.pack(fill="x", padx=10, pady=4)
            return var

        self.v_warn_days = add_row("Backup WARN days:", "backup_warn_days")
        self.v_crit_days = add_row("Backup CRIT days:", "backup_crit_days")
        self.v_warn_pct  = add_row("Disk WARN %:", "disk_warn_pct")
        self.v_crit_pct  = add_row("Disk CRIT %:", "disk_crit_pct")
        self.v_workers   = add_row("Max parallel workers:", "max_workers")
        self.v_driver    = add_row("ODBC Driver:", "odbc_driver", width=24)

        self.var_trust = tk.BooleanVar(value=bool(self.settings.get("trust_server_cert", True)))
        ttk.Checkbutton(self, text="Trust Server Certificate", variable=self.var_trust).pack(anchor="w", padx=10, pady=(4,8))

        btns = ttk.Frame(self)
        ttk.Button(btns, text="Save", command=self._save).grid(row=0, column=0, padx=6)
        ttk.Button(btns, text="Cancel", command=self.destroy).grid(row=0, column=1, padx=6)
        btns.pack(pady=8)

        self.grab_set()

    def _save(self):
        try:
            s = self.settings
            s["backup_warn_days"] = int(self.v_warn_days.get())
            s["backup_crit_days"] = int(self.v_crit_days.get())
            s["disk_warn_pct"]    = int(self.v_warn_pct.get())
            s["disk_crit_pct"]    = int(self.v_crit_pct.get())
            s["max_workers"]      = int(self.v_workers.get())
            s["odbc_driver"]      = self.v_driver.get().strip()
            s["trust_server_cert"]= bool(self.var_trust.get())
            save_settings(s)
            self.destroy()
        except Exception as e:
            messagebox.showerror("Error", str(e))

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1300x700")
        self.settings = load_settings()
        self.servers = load_servers()  # dict with "servers":[...]
        self.data_rows = []
        self._lock = threading.Lock()

        # Toolbar
        bar = ttk.Frame(self)
        ttk.Button(bar, text="Refresh", command=self.refresh).pack(side="left", padx=5, pady=5)
        ttk.Button(bar, text="Export CSV", command=self.export_csv).pack(side="left", padx=5)
        ttk.Button(bar, text="Manage Servers", command=self.manage_servers).pack(side="left", padx=5)
        ttk.Button(bar, text="Settings", command=self.open_settings).pack(side="left", padx=5)
        self.status_lbl = ttk.Label(bar, text="")
        self.status_lbl.pack(side="left", padx=15)
        bar.pack(fill="x")

        # Table
        self.tree = ttk.Treeview(self, columns=COLUMNS, show="headings")
        for col in COLUMNS:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=160 if col not in ("S.No","CU") else 70, anchor="w")
        self.tree.pack(fill="both", expand=True)

        # Row tags for colors
        style = ttk.Style(self)
        style.map("Treeview")  # allow default
        self.tree.tag_configure("CRIT", background="#FDE7E9")
        self.tree.tag_configure("WARN", background="#FFF8E1")
        self.tree.tag_configure("OK",   background="#E8F5E9")

        # Initial refresh
        self.after(200, self.refresh)

    def set_status(self, txt):
        self.status_lbl.config(text=txt)
        self.status_lbl.update_idletasks()

    def refresh(self):
        srvs = self.servers.get("servers", [])
        if not srvs:
            messagebox.showinfo("Add servers", "No servers configured. Click 'Manage Servers' to add.")
            return
        self.set_status("Checking...")
        self.tree.delete(*self.tree.get_children())
        self.data_rows = []

        def work():
            rows = []
            with ThreadPoolExecutor(max_workers=self.settings["max_workers"]) as ex:
                futures = {ex.submit(test_instance, self.settings, s): s for s in srvs}
                i = 0
                for fut in as_completed(futures):
                    row = fut.result()
                    rows.append(row)
                    i += 1
                    self.set_status(f"Checked {i}/{len(srvs)}")
            # Sort and add S.No
            rows.sort(key=lambda r: (r.get("Environment",""), r.get("SQL Server Instance","")))
            for idx, r in enumerate(rows, start=1):
                r["S.No"] = idx
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
        if not self.data_rows:
            return
        path = filedialog.asksaveasfilename(
            title="Export CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"SqlHealth_{datetime.now():%Y%m%d_%H%M%S}.csv"
        )
        if not path:
            return
        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(COLUMNS)
                for r in self.data_rows:
                    w.writerow([r.get(c,"") for c in COLUMNS])
            messagebox.showinfo("Export", f"Exported to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export", str(e))

    def manage_servers(self):
        dlg = tk.Toplevel(self)
        dlg.title("Manage Servers")
        dlg.geometry("650x350")

        cols = ("instance","environment","auth","username","save_pwd")
        tv = ttk.Treeview(dlg, columns=cols, show="headings", selectmode="browse")
        for c in cols:
            tv.heading(c, text=c)
            tv.column(c, width=130 if c!="instance" else 220, anchor="w")
        tv.pack(fill="both", expand=True, padx=8, pady=8)

        def reload_list():
            tv.delete(*tv.get_children())
            for s in self.servers.get("servers", []):
                tv.insert("", "end", values=(s.get("instance",""),
                                             s.get("environment",""),
                                             s.get("auth",""),
                                             s.get("username",""),
                                             "Yes" if s.get("save_pwd") else "No"))

        def add_server():
            sd = ServerDialog(self)
            self.wait_window(sd)
            if sd.result:
                # if sql and save_pwd unchecked but password typed, we still used it for now
                self.servers.setdefault("servers", []).append(sd.result)
                save_servers(self.servers)
                reload_list()

        def edit_server():
            sel = tv.selection()
            if not sel:
                return
            idx = tv.index(sel[0])
            current = self.servers.get("servers", [])[idx]
            sd = ServerDialog(self, current)
            self.wait_window(sd)
            if sd.result:
                self.servers["servers"][idx] = sd.result
                save_servers(self.servers)
                reload_list()

        def delete_server():
            sel = tv.selection()
            if not sel:
                return
            idx = tv.index(sel[0])
            inst = self.servers.get("servers", [])[idx].get("instance","")
            if messagebox.askyesno("Delete", f"Remove '{inst}' from monitoring?"):
                self.servers["servers"].pop(idx)
                save_servers(self.servers)
                reload_list()

        btns = ttk.Frame(dlg)
        ttk.Button(btns, text="Add", command=add_server).pack(side="left", padx=5)
        ttk.Button(btns, text="Edit", command=edit_server).pack(side="left", padx=5)
        ttk.Button(btns, text="Delete", command=delete_server).pack(side="left", padx=5)
        ttk.Button(btns, text="Close", command=dlg.destroy).pack(side="right", padx=5)
        btns.pack(fill="x", pady=(0,8), padx=8)

        reload_list()

    def open_settings(self):
        sd = SettingsDialog(self, self.settings)
        self.wait_window(sd)
        # reload settings in case updated
        self.settings = load_settings()

if __name__ == "__main__":
    App().mainloop()
