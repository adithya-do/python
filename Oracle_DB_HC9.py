#!/usr/bin/env python3
"""
Oracle DB Health GUI Monitor — Version 7
Adds:
1) Clear Status button.
2) Customizable visible columns (data collection unchanged).
3) Customizable email columns.
4) Password encryption in config (DPAPI on Windows; base64 fallback elsewhere).
"""
import json, os, smtplib, sys, threading, time, base64
from dataclasses import dataclass, field, asdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter import font as tkfont

try:
    import oracledb
except Exception:
    oracledb = None

APP_NAME = "Oracle DB Health GUI Monitor"
CONFIG_DIR = Path.home() / ".ora_gui_monitor"; CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "config.json"
ORACLE_CLIENT_LIB_DIR = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
DEFAULT_INTERVAL_SEC = 300
GOOD, BAD = "✅", "❌"

# ---------- Password encryption (DPAPI on Windows) ----------
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
    return _win_protect(plain.encode("utf-8")) if sys.platform.startswith("win") else base64.b64encode(plain.encode("utf-8")).decode("ascii")

def _decrypt_password(enc: Optional[str]) -> Optional[str]:
    if not enc: return None
    try:
        return (_win_unprotect(enc).decode("utf-8") if sys.platform.startswith("win") else base64.b64decode(enc.encode("ascii")).decode("utf-8"))
    except Exception:
        return None

# ---------- Data models ----------
@dataclass
class DbTarget:
    name: str
    dsn: str
    user: Optional[str] = None
    password: Optional[str] = None           # runtime only
    password_enc: Optional[str] = None       # persisted
    wallet_dir: Optional[str] = None
    mode: str = "thin"                       
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

# ---------- Config helpers ----------
def default_config() -> Dict[str, Any]:
    cols = ["S.No","DB Name","Environment","Host","DB Version","Startup Time","Status","Inst_status","Sessions","WorstTS%","TS Online","DB Size","LastFull/Inc","LastArch","Ms","LastChecked","Check status","Error"]
    return {
        "interval_sec": DEFAULT_INTERVAL_SEC,
        "targets": [],
        "client_lib_dir": ORACLE_CLIENT_LIB_DIR or "",
        "email": {"server": "","port": 25,"from_addr": "","to_addrs": "","subject": "Oracle DB Health Report"},
        "last_health": {},
        "auto_run": False,
        "column_order": cols[:],
        "visible_columns": cols[:],
        "email_columns": cols[:],
        "column_widths": {},
    }

def _hydrate_target(d: Dict[str, Any]) -> DbTarget:
    pwd = d.get("password")
    penc = d.get("password_enc") or (_encrypt_password(pwd) if pwd else None)
    return DbTarget(
        name=d.get("name",""), dsn=d.get("dsn",""), user=d.get("user"),
        password=_decrypt_password(penc) if penc else None, password_enc=penc,
        wallet_dir=d.get("wallet_dir"), mode=d.get("mode","thin"), environment=d.get("environment","NON-PROD"),
    )

def _serialize_target(t: DbTarget) -> Dict[str, Any]:
    enc = t.password_enc or (_encrypt_password(t.password) if t.password else None)
    return {"name": t.name,"dsn": t.dsn,"user": t.user,"password_enc": enc,"wallet_dir": t.wallet_dir,"mode": t.mode,"environment": t.environment}

def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            base = default_config()
            for k, v in cfg.items():
                if k == "email": base["email"].update(v or {})
                else: base[k] = v
            base.setdefault("last_health", {}); base.setdefault("auto_run", False)
            base.setdefault("column_order", default_config()["column_order"]); base.setdefault("column_widths", {})
            base.setdefault("visible_columns", default_config()["visible_columns"]); base.setdefault("email_columns", default_config()["email_columns"])
            # migrate any plaintext passwords
            if base.get("targets"):
                new_t = []; changed = False
                for t in base["targets"]:
                    tt = _hydrate_target(t)
                    if t.get("password"): changed = True
                    new_t.append(_serialize_target(tt))
                if changed:
                    base["targets"] = new_t; CONFIG_PATH.write_text(json.dumps(base, indent=2, default=str), encoding="utf-8")
            return base
        except Exception:
            pass
    return default_config()

def save_config(cfg: Dict[str, Any]):
    ts = []
    for t in cfg.get("targets", []):
        ts.append(_serialize_target(_hydrate_target(t) if isinstance(t, dict) else t))
    out = dict(cfg); out["targets"] = ts
    CONFIG_PATH.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")

# ---------- Oracle ----------
def init_oracle_client_if_needed(cfg: Dict[str, Any]):
    if oracledb is None: return
    lib_dir = cfg.get("client_lib_dir") or ORACLE_CLIENT_LIB_DIR
    if lib_dir:
        try: oracledb.init_oracle_client(lib_dir=lib_dir)
        except Exception: pass

def _connect(target: DbTarget):
    if oracledb is None: raise RuntimeError("python-oracledb not installed. pip install python-oracledb")
    if target.mode.lower()=="thick" and (cfg:=load_config()).get("client_lib_dir"):
        try: oracledb.init_oracle_client(lib_dir=cfg.get("client_lib_dir"))
        except Exception: pass
    if target.wallet_dir: return oracledb.connect(config_dir=target.wallet_dir, dsn=target.dsn)
    if target.user and (target.password or target.password_enc):
        pwd = target.password or _decrypt_password(target.password_enc or "")
        return oracledb.connect(user=target.user, password=pwd, dsn=target.dsn)
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
    "ts_online": "SELECT COUNT(*) total, SUM(CASE WHEN UPPER(status)='ONLINE' THEN 1 ELSE 0 END) tonline FROM dba_tablespaces",
    "db_size": "SELECT ROUND(SUM(bytes)/1024/1024/1024,1) FROM dba_data_files",
    "bk_data": (
        "SELECT MAX(bp.completion_time) FROM v$backup_set bs JOIN v$backup_piece bp "
        "ON bs.set_stamp=bp.set_stamp AND bs.set_count=bp.set_count WHERE bs.backup_type='D'"
    ),
    "bk_arch": (
        "SELECT MAX(bp.completion_time) FROM v$backup_set bs JOIN v$backup_piece bp "
        "ON bs.set_stamp=bp.set_stamp AND bs.set_count=bp.set_count WHERE bs.backup_type='L'"
    ),
}

def _dt_str(dt: Optional[datetime]) -> str: return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "-"

def check_one(target: DbTarget, timeout_sec: int = 25) -> DbHealth:
    t0 = time.time()
    try:
        with _connect(target) as conn:
            conn.call_timeout = timeout_sec * 1000
            cur = conn.cursor()
            details=""
            try:
                cur.execute(SQLS["db"]); _name, _open_mode, _role, log_mode = cur.fetchone()
                details=f"Log:{log_mode}"
            except Exception: pass
            cur.execute(SQLS["inst"]); _inst_name, inst_status, host_name, inst_version, startup_time = cur.fetchone()
            sessions_curr=sessions_limit=0
            try: cur.execute(SQLS["sess_curr"]); sessions_curr=int(cur.fetchone()[0])
            except Exception: pass
            try: cur.execute(SQLS["sess_limit"]); sessions_limit=int(cur.fetchone()[0])
            except Exception: pass
            worst_pct=None
            try:
                cur.execute(SQLS["tspace"]); worst_pct=0.0
                for _ts, pct in cur.fetchall():
                    if pct is not None and pct>(worst_pct or 0): worst_pct=float(pct)
            except Exception: worst_pct=None
            ts_total=ts_online=None
            try:
                cur.execute(SQLS["ts_online"]); total, tonline = cur.fetchone()
                ts_total=int(total or 0); ts_online=int(tonline or 0)
            except Exception: pass
            db_size_gb=None
            try: cur.execute(SQLS["db_size"]); r=cur.fetchone(); db_size_gb=float(r[0]) if r and r[0] is not None else None
            except Exception: pass
            last_df=last_arch=None
            try: cur.execute(SQLS["bk_data"]); r=cur.fetchone(); last_df=r[0] if r else None
            except Exception: pass
            try: cur.execute(SQLS["bk_arch"]); r=cur.fetchone(); last_arch=r[0] if r else None
            except Exception: pass
            elapsed_ms=int((time.time()-t0)*1000)
            return DbHealth(status="UP", details=details, version=inst_version, inst_status=inst_status,
                            sessions_curr=sessions_curr, sessions_limit=sessions_limit, worst_ts_pct_used=worst_pct,
                            host=host_name, elapsed_ms=elapsed_ms, last_full_inc_backup=last_df, last_arch_backup=last_arch,
                            startup_time=startup_time, ts_online=ts_online, ts_total=ts_total, db_size_gb=db_size_gb, error="")
    except Exception as e:
        elapsed_ms=int((time.time()-t0)*1000)
        return DbHealth(status="DOWN", details=str(e), elapsed_ms=elapsed_ms, error=str(e))

# ---------- GUI ----------
class MonitorApp(ttk.Frame):
    LOGICAL_COLUMNS = ("S.No","DB Name","Environment","Host","DB Version","Startup Time","Status","Inst_status","Sessions","WorstTS%","TS Online","DB Size","LastFull/Inc","LastArch","Ms","LastChecked","Check status","Error")
    STATUS_COLUMNS = {"Status","Inst_status","Sessions","WorstTS%","TS Online","DB Size","LastFull/Inc","LastArch","Ms","LastChecked","Check status","Error"}

    def __init__(self, master, cfg: Dict[str, Any]):
        super().__init__(master); self.master.title(APP_NAME); self.pack(fill=tk.BOTH, expand=True)
        self.cfg=cfg; self.interval_sec=int(cfg.get("interval_sec", DEFAULT_INTERVAL_SEC))
        self.targets: List[DbTarget]=[_hydrate_target(t) if isinstance(t,dict) else t for t in cfg.get("targets",[])]
        self.last_health: Dict[str, Dict[str, Any]]=cfg.get("last_health",{}); self._auto_flag=False
        self._build_ui(); init_oracle_client_if_needed(cfg); self._refresh_table_from_targets(); self._load_last_health_into_rows()
        if cfg.get("auto_run"): self.auto_var.set(True); self._start_auto()
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        t1=ttk.Frame(self); t1.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(6,3))
        self.interval_var=tk.IntVar(value=self.interval_sec)
        ttk.Label(t1,text="Interval (sec):").pack(side=tk.LEFT); ttk.Spinbox(t1,from_=30,to=3600,textvariable=self.interval_var,width=8).pack(side=tk.LEFT,padx=(4,10))
        self.auto_var=tk.BooleanVar(value=self.cfg.get("auto_run",False))
        ttk.Checkbutton(t1,text="Auto-run",variable=self.auto_var,command=self._toggle_auto).pack(side=tk.LEFT,padx=(0,8))
        ttk.Button(t1,text="Run All",command=self.run_all_once).pack(side=tk.LEFT)
        ttk.Button(t1,text="Run Selected",command=self.run_selected_once).pack(side=tk.LEFT,padx=(6,10))
        ttk.Button(t1,text="Clear Status",command=self._clear_status_all).pack(side=tk.LEFT,padx=(6,10))
        ttk.Button(t1,text="Add DB",command=self._add_dialog).pack(side=tk.LEFT,padx=(10,0)); ttk.Button(t1,text="Edit DB",command=self._edit_selected).pack(side=tk.LEFT); ttk.Button(t1,text="Remove DB",command=self._remove_selected).pack(side=tk.LEFT)
        ttk.Button(t1,text="Import JSON",command=self._import_json).pack(side=tk.LEFT,padx=(10,0)); ttk.Button(t1,text="Export JSON",command=self._export_json).pack(side=tk.LEFT)
        ttk.Button(t1,text="Customize Columns",command=self._customize_columns).pack(side=tk.LEFT,padx=(10,0))
        ttk.Button(t1,text="Select Columns",command=self._select_columns_dialog).pack(side=tk.LEFT,padx=(6,0))
        ttk.Button(t1,text="Email Columns",command=self._select_email_columns_dialog).pack(side=tk.LEFT,padx=(6,0))
        ttk.Label(t1,text="Client lib dir:").pack(side=tk.LEFT,padx=(10,0)); self.client_dir_var=tk.StringVar(value=self.cfg.get("client_lib_dir","")); ttk.Entry(t1,textvariable=self.client_dir_var,width=22).pack(side=tk.LEFT,padx=4); ttk.Button(t1,text="Browse",command=self._pick_client_dir).pack(side=tk.LEFT)

        t2=ttk.Frame(self); t2.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0,6))
        email_cfg=self.cfg.get("email",{})
        ttk.Label(t2,text="SMTP/Exchange:").pack(side=tk.LEFT); self.smtp_server_var=tk.StringVar(value=email_cfg.get("server","")); self.smtp_port_var=tk.IntVar(value=int(email_cfg.get("port",25)))
        ttk.Entry(t2,textvariable=self.smtp_server_var,width=22).pack(side=tk.LEFT,padx=(4,2)); ttk.Entry(t2,textvariable=self.smtp_port_var,width=6).pack(side=tk.LEFT,padx=(2,6))
        ttk.Label(t2,text="From:").pack(side=tk.LEFT); self.from_var=tk.StringVar(value=email_cfg.get("from_addr","")); ttk.Entry(t2,textvariable=self.from_var,width=24).pack(side=tk.LEFT,padx=(4,6))
        ttk.Label(t2,text="To (comma):").pack(side=tk.LEFT); self.to_var=tk.StringVar(value=email_cfg.get("to_addrs","")); ttk.Entry(t2,textvariable=self.to_var,width=32).pack(side=tk.LEFT,padx=(4,6))
        ttk.Button(t2,text="Save Mail",command=self._save_mail_settings).pack(side=tk.LEFT,padx=(6,0)); ttk.Button(t2,text="Email Report",command=self._email_report).pack(side=tk.LEFT,padx=(6,0))

        tree_frame=ttk.Frame(self); tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.tree=ttk.Treeview(tree_frame,columns=self.LOGICAL_COLUMNS,show="headings",height=20)
        vsb=ttk.Scrollbar(tree_frame,orient="vertical",command=self.tree.yview); xsb=ttk.Scrollbar(tree_frame,orient="horizontal",command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set,xscrollcommand=xsb.set); vsb.pack(side=tk.RIGHT,fill=tk.Y); self.tree.pack(side=tk.TOP,fill=tk.BOTH,expand=True); xsb.pack(side=tk.BOTTOM,fill=tk.X)
        for col in self.LOGICAL_COLUMNS:
            self.tree.heading(col,text=col,command=lambda c=col: self._sort_by_column(c,False))
            self.tree.column(col,width=120,stretch=True,anchor="w")
        order=self.cfg.get("column_order",list(self.LOGICAL_COLUMNS)); order=[c for c in order if c in self.LOGICAL_COLUMNS]
        for c in self.LOGICAL_COLUMNS:
            if c not in order: order.append(c)
        vis=self.cfg.get("visible_columns",order[:]); vis=[c for c in vis if c in self.LOGICAL_COLUMNS]
        if "S.No" not in vis: vis=["S.No"]+[c for c in vis if c!="S.No"]
        if order[0]!="S.No": order=["S.No"]+[c for c in order if c!="S.No"]
        display=[c for c in order if c in vis]; self.tree["displaycolumns"]=display
        for col,w in self.cfg.get("column_widths",{}).items():
            try: self.tree.column(col,width=int(w))
            except Exception: pass

        self.menu=tk.Menu(self,tearoff=0); self.menu.add_command(label="Copy Cell",command=self._copy_cell); self.menu.add_separator()
        self.menu.add_command(label="Copy DB Name",command=lambda: self._copy_by_col("DB Name")); self.menu.add_command(label="Copy Host",command=lambda: self._copy_by_col("Host")); self.menu.add_command(label="Copy Error",command=lambda: self._copy_by_col("Error"))
        self.tree.bind("<Button-3>",self._show_context_menu); self.tree.bind("<ButtonRelease-1>",lambda e: self._persist_column_layout())

        bottombar=ttk.Frame(self); bottombar.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)
        self.status_var=tk.StringVar(value="Idle"); ttk.Label(bottombar,textvariable=self.status_var).pack(side=tk.LEFT)
        self._font=tkfont.nametofont("TkDefaultFont")

    # Auto-run
    def _toggle_auto(self):
        if self.auto_var.get(): self._start_auto()
        else: self._stop_auto()
        self.cfg["auto_run"]=self.auto_var.get(); save_config(self.cfg)
    def _start_auto(self):
        if getattr(self,"_auto_flag",False): return
        self._auto_flag=True; self.status_var.set(f"Auto-running every {self.interval_var.get()}s..."); self.after(200,self._loop)
    def _stop_auto(self):
        self._auto_flag=False; self.status_var.set("Auto-run stopped")

    # Column utilities
    def _persist_column_layout(self):
        widths={col:self.tree.column(col,"width") for col in self.LOGICAL_COLUMNS}; self.cfg["column_widths"]=widths
        visible=list(self.tree["displaycolumns"])
        full=self.cfg.get("column_order",list(self.LOGICAL_COLUMNS)); new_full=[]; seen=set()
        for c in visible:
            if c not in seen: new_full.append(c); seen.add(c)
        for c in full:
            if c not in seen and c in self.LOGICAL_COLUMNS: new_full.append(c); seen.add(c)
        self.cfg["column_order"]=new_full; self.cfg["visible_columns"]=visible; save_config(self.cfg)

    def _autosize_columns(self):
        pad=24; visible=list(self.tree["displaycolumns"])
        for col in visible:
            header_w=self._font.measure(col); max_w=header_w
            for iid in self.tree.get_children(""):
                vals=self.tree.item(iid)["values"]
                try:
                    idx=self.LOGICAL_COLUMNS.index(col); txt=str(vals[idx]) if idx<len(vals) else ""
                    tw=self._font.measure(txt); max_w=max(max_w,tw)
                except Exception: pass
            new_w=max(max_w+pad,90); cur=self.tree.column(col,"width")
            if cur<new_w: self.tree.column(col,width=new_w)

    def _customize_columns(self):
        dlg=tk.Toplevel(self); dlg.title("Customize Columns (Order)"); dlg.geometry("380x420")
        ttk.Label(dlg,text="Reorder columns (S.No is fixed at first):").pack(pady=6)
        current=[c for c in self.tree["displaycolumns"] if c!="S.No"]; var=tk.Variable(value=current); lb=tk.Listbox(dlg,listvariable=var,selectmode=tk.SINGLE,height=16); lb.pack(fill=tk.BOTH,expand=True,padx=10,pady=6)
        btn=ttk.Frame(dlg); btn.pack(fill=tk.X,padx=10,pady=6)
        def move(off:int):
            sel=lb.curselection(); 
            if not sel: return
            i=sel[0]; j=i+off
            if j<0 or j>=lb.size(): return
            items=list(lb.get(0,tk.END)); items[i],items[j]=items[j],items[i]; var.set(items); lb.selection_clear(0,tk.END); lb.selection_set(j); lb.activate(j)
        ttk.Button(btn,text="Up",command=lambda:move(-1)).pack(side=tk.LEFT,padx=4); ttk.Button(btn,text="Down",command=lambda:move(1)).pack(side=tk.LEFT,padx=4)
        def apply_and_close():
            items=list(lb.get(0,tk.END)); order=["S.No"]+items; self.tree["displaycolumns"]=order; self.cfg["visible_columns"]=order; self._persist_column_layout(); dlg.destroy()
        ttk.Button(btn,text="Apply",command=apply_and_close).pack(side=tk.RIGHT,padx=4); ttk.Button(btn,text="Cancel",command=dlg.destroy).pack(side=tk.RIGHT,padx=4)

    def _select_columns_dialog(self):
        dlg=tk.Toplevel(self); dlg.title("Select Visible Columns"); dlg.geometry("360x500")
        ttk.Label(dlg,text="Choose columns to display (S.No always visible):").pack(pady=6)
        current=set(self.cfg.get("visible_columns",list(self.LOGICAL_COLUMNS))); vars_by_col={}; box=ttk.Frame(dlg); box.pack(fill=tk.BOTH,expand=True,padx=10,pady=6)
        for col in self.LOGICAL_COLUMNS:
            if col=="S.No": ttk.Label(box,text="S.No (always visible)").pack(anchor="w"); continue
            v=tk.BooleanVar(value=(col in current)); vars_by_col[col]=v; ttk.Checkbutton(box,text=col,variable=v).pack(anchor="w")
        def apply_and_close():
            order=self.cfg.get("column_order",list(self.LOGICAL_COLUMNS)); order=[c for c in order if c in self.LOGICAL_COLUMNS]
            vis=["S.No"]+[c for c in order if c!="S.No" and vars_by_col.get(c,tk.BooleanVar(value=True)).get()]
            self.tree["displaycolumns"]=vis; self.cfg["visible_columns"]=vis; save_config(self.cfg); dlg.destroy()
        ttk.Button(dlg,text="Apply",command=apply_and_close).pack(pady=8)

    def _select_email_columns_dialog(self):
        dlg=tk.Toplevel(self); dlg.title("Select Email Columns"); dlg.geometry("360x520")
        ttk.Label(dlg,text="Choose columns for email report:").pack(pady=6)
        current=set(self.cfg.get("email_columns",list(self.LOGICAL_COLUMNS))); vars_by_col={}; box=ttk.Frame(dlg); box.pack(fill=tk.BOTH,expand=True,padx=10,pady=6)
        for col in self.LOGICAL_COLUMNS:
            v=tk.BooleanVar(value=(col in current)); vars_by_col[col]=v; ttk.Checkbutton(box,text=col,variable=v).pack(anchor="w")
        def apply_and_close():
            order=self.cfg.get("column_order",list(self.LOGICAL_COLUMNS)); cols=[c for c in order if vars_by_col.get(c).get()]
            if "S.No" in cols: cols=["S.No"]+[c for c in cols if c!="S.No"]
            self.cfg["email_columns"]=cols or list(self.LOGICAL_COLUMNS); save_config(self.cfg); dlg.destroy()
        ttk.Button(dlg,text="Apply",command=apply_and_close).pack(pady=8)

    # Context menu & copy
    def _show_context_menu(self,event):
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

    # Sorting helpers
    def _parse_sessions(self,s:str)->Tuple[int,int]:
        t=str(s).strip(); t=t.split()[-1] if " " in t else t
        try: a,b=t.split("/"); return (int(a),int(b))
        except Exception: return (0,0)
    def _parse_pct(self,s:str)->float:
        try: return float(str(s).replace("%","").split()[-1])
        except Exception: return -1.0
    def _parse_datecell(self,s:str)->float:
        p=str(s).strip().split()
        if not p or p[-1]=="-": return float("-inf")
        try:
            if len(p)>=2 and ":" in p[-1]: return datetime.strptime(f"{p[-2]} {p[-1]}","%Y-%m-%d %H:%M:%S").timestamp()
            return datetime.strptime(p[-1],"%Y-%m-%d").timestamp()
        except Exception: return float("-inf")
    def _status_rank(self,s:str)->int: return 1 if str(s).strip().startswith(GOOD) else 0
    def _inst_rank(self,s:str)->int: return 1 if ("OPEN" in str(s).upper() and str(s).strip().startswith(GOOD)) else 0
    def _ts_online_rank(self,s:str)->Tuple[int,int]:
        t=str(s).strip(); t=t.split()[-1] if " " in t else t
        try: a,b=t.split("/"); return (int(a),int(b))
        except Exception: return (0,0)
    def _generic_key(self,col:str,s:str):
        if col=="S.No":
            try: return (int(s),)
            except: return (0,)
        if col=="Status": return (self._status_rank(s), s)
        if col=="Inst_status": return (self._inst_rank(s), s)
        if col=="WorstTS%": return (self._parse_pct(s),)
        if col in ("LastFull/Inc","LastArch","LastChecked","Startup Time"): return (self._parse_datecell(s),)
        if col=="Sessions":
            curr,limit=self._parse_sessions(s); return (curr/limit if limit else -1.0,curr,limit)
        if col=="TS Online":
            on,tot=self._ts_online_rank(s); return (on,tot)
        if col=="DB Size":
            try: return (float(str(s).split()[0]),)
            except: return (-1.0,)
        if col=="Ms":
            try: return (int(s),)
            except: return (-1,)
        if col=="Check status":
            order={"In Progress":0,"Complete":1}; return (order.get(s,2),s)
        return (str(s).lower(),)
    def _sort_by_column(self,col:str,descending:bool):
        rows=[(self._generic_key(col,self.tree.set(k,col)),k) for k in self.tree.get_children("")]
        rows.sort(reverse=descending,key=lambda x:x[0])
        for i,(_,k) in enumerate(rows): self.tree.move(k,"",i)
        self._renumber(); self.tree.heading(col,command=lambda c=col:self._sort_by_column(c,not descending))

    # Helpers
    def _renumber(self):
        for i,iid in enumerate(self.tree.get_children(""),start=1):
            vals=list(self.tree.item(iid)["values"]); 
            if vals: vals[0]=i; self.tree.item(iid,values=vals)
    def _pick_client_dir(self):
        d=filedialog.askdirectory(title="Select Oracle Client lib directory")
        if d: self.client_dir_var.set(d); self.cfg["client_lib_dir"]=d; save_config(self.cfg)
    def _refresh_table_from_targets(self):
        for i in self.tree.get_children(): self.tree.delete(i)
        for idx,t in enumerate(self.targets,start=1):
            values=["-"]*len(self.LOGICAL_COLUMNS); values[0]=idx; values[1]=t.name; values[2]=t.environment
            self.tree.insert("",tk.END,iid=t.name,values=tuple(values))
        self._renumber(); self._autosize_columns()
    def _load_last_health_into_rows(self):
        for t in self.targets:
            h=self.last_health.get(t.name); 
            if h: self._apply_persisted_row(t.name,h)
        self._autosize_columns()
    def _apply_persisted_row(self,name:str,h:Dict[str,Any]):
        vals=list(self.tree.item(name)["values"])
        mark=lambda ok: GOOD if ok else BAD
        status_cell=f"{mark(h.get('status','').upper()=='UP')} {h.get('status','-')}"
        inst_cell=f"{mark((h.get('inst_status','') or '').upper()=='OPEN')} {h.get('inst_status','-')}"
        sc=int(h.get('sessions_curr',0)); sl=int(h.get('sessions_limit',0) or 0)
        sessions_cell=f"{mark((sl==0) or (sc<0.95*sl))} {sc}/{sl}" if sl else f"{BAD} 0/0"
        worst=h.get('worst_ts_pct_used'); worst_ok=not (worst is not None and float(worst)>=90.0); worst_val='-' if worst is None else f"{float(worst):.1f}%"
        worst_cell=f"{mark(worst_ok)} {worst_val}"
        startup_str=h.get('startup_time_str','-')
        on=int(h.get('ts_online',0) or 0); tot=int(h.get('ts_total',0) or 0); ts_cell=f"{mark(tot==on and tot>0)} {on}/{tot}" if tot else f"{BAD} 0/0"
        db_size_cell=f"{h.get('db_size_gb','-')} GB" if h.get('db_size_gb') is not None else "-"
        idx={c:i for i,c in enumerate(self.LOGICAL_COLUMNS)}
        vals[idx["Host"]]=h.get("host","-"); vals[idx["Status"]]=status_cell; vals[idx["Inst_status"]]=inst_cell; vals[idx["Sessions"]]=sessions_cell
        vals[idx["WorstTS%"]]=worst_cell; vals[idx["LastFull/Inc"]]=h.get("last_full_inc_backup_str",f"{BAD} -"); vals[idx["LastArch"]]=h.get("last_arch_backup_str",f"{BAD} -")
        vals[idx["DB Version"]]=h.get("version","-"); vals[idx["Startup Time"]]=startup_str; vals[idx["TS Online"]]=ts_cell; vals[idx["DB Size"]]=db_size_cell
        vals[idx["Ms"]]=h.get("elapsed_ms",0); vals[idx["LastChecked"]]=h.get("ts","-"); vals[idx["Check status"]]="Complete"; vals[idx["Error"]]=h.get("error","")
        self.tree.item(name,values=vals)

    # Monitoring
    def run_all_once(self): self._checks_async(targets=self.targets)
    def run_selected_once(self):
        sel=self.tree.selection()
        if not sel: messagebox.showinfo(APP_NAME,"Select a row (DB) to run."); return
        name=sel[0]; target=next((x for x in self.targets if x.name==name),None)
        if not target: messagebox.showerror(APP_NAME,"Selected DB not found."); return
        self._checks_async(targets=[target])
    def _loop(self):
        if not self.auto_var.get(): return
        self._checks_async(targets=self.targets); self.after(self.interval_var.get()*1000,self._loop)
    def _checks_async(self,targets:List[DbTarget]):
        for t in targets: self._set_check_status(t.name,"In Progress")
        def job(t:DbTarget):
            try: res=check_one(t)
            except Exception as e: res=DbHealth(status="DOWN",details=str(e),error=str(e))
            self.after(0,lambda tn=t.name,tr=t,rh=res: self._apply_result(tn,tr,rh))
        for t in targets: threading.Thread(target=job,args=(t,),daemon=True).start()
    def _set_check_status(self,name:str,status:str):
        if name in self.tree.get_children():
            vals=list(self.tree.item(name)["values"]); idx=self.LOGICAL_COLUMNS.index("Check status")
            if len(vals)<=idx: vals+=[""]*(idx+1-len(vals))
            vals[idx]=status; self.tree.item(name,values=vals)
    def _apply_result(self,name:str,target:DbTarget,h:DbHealth):
        status_cell=f"{GOOD if h.status.upper()=='UP' else BAD} {h.status}"
        inst_cell=f"{GOOD if (h.inst_status or '').upper()=='OPEN' else BAD} {h.inst_status or '-'}"
        if h.sessions_limit and h.sessions_limit>0:
            sess_ok=h.sessions_curr<0.95*h.sessions_limit; sessions_cell=f"{GOOD if sess_ok else BAD} {h.sessions_curr}/{h.sessions_limit}"
        else: sessions_cell=f"{BAD} 0/0"
        worst_ok=not (h.worst_ts_pct_used is not None and h.worst_ts_pct_used>=90.0); worst_val='-' if h.worst_ts_pct_used is None else f"{h.worst_ts_pct_used:.1f}%"
        worst_cell=f"{GOOD if worst_ok else BAD} {worst_val}"
        def fmt_backup(dt:Optional[datetime],arch=False):
            if not dt: return f"{BAD} -"
            age=(datetime.now(dt.tzinfo)-dt).total_seconds()/3600.0; ok=(age<=12) if arch else ((age/24.0)<=3)
            return f"{GOOD if ok else BAD} {_dt_str(dt)}"
        last_full_cell=fmt_backup(h.last_full_inc_backup,False); last_arch_cell=fmt_backup(h.last_arch_backup,True)
        startup_str=_dt_str(h.startup_time)
        if (h.ts_total or 0)>0: ts_ok=(h.ts_online==h.ts_total); ts_cell=f"{GOOD if ts_ok else BAD} {h.ts_online}/{h.ts_total}"
        else: ts_cell=f"{BAD} 0/0"
        db_size_cell=f"{h.db_size_gb:.1f} GB" if h.db_size_gb is not None else "-"
        vals=list(self.tree.item(name)["values"] or ["-"]*len(self.LOGICAL_COLUMNS)); idx={c:i for i,c in enumerate(self.LOGICAL_COLUMNS)}
        vals[idx["Host"]]=h.host or "-"; vals[idx["Status"]]=status_cell; vals[idx["Inst_status"]]=inst_cell; vals[idx["Sessions"]]=sessions_cell
        vals[idx["WorstTS%"]]=worst_cell; vals[idx["LastFull/Inc"]]=last_full_cell; vals[idx["LastArch"]]=last_arch_cell
        vals[idx["DB Version"]]=h.version or "-"; vals[idx["Startup Time"]]=startup_str; vals[idx["TS Online"]]=ts_cell; vals[idx["DB Size"]]=db_size_cell
        vals[idx["Ms"]]=h.elapsed_ms; vals[idx["LastChecked"]]=h.ts; vals[idx["Check status"]]="Complete"; vals[idx["Error"]]=h.error or ("" if h.status=="UP" else h.details)
        self.tree.item(name,values=vals)
        self.last_health[name]={"status":h.status,"inst_status":h.inst_status,"sessions_curr":h.sessions_curr,"sessions_limit":h.sessions_limit,"worst_ts_pct_used":h.worst_ts_pct_used,"host":h.host,"elapsed_ms":h.elapsed_ms,"version":h.version,"startup_time_str":startup_str,"ts_online":h.ts_online,"ts_total":h.ts_total,"db_size_gb":h.db_size_gb,"ts":h.ts,"error":vals[idx["Error"]],"last_full_inc_backup_str":last_full_cell,"last_arch_backup_str":last_arch_cell}
        self.cfg["last_health"]=self.last_health; save_config(self.cfg)
        self.status_var.set(f"Updated {name} at {h.ts}"); self._renumber(); self._autosize_columns()

    # Clear Status
    def _clear_status_all(self):
        colidx={c:i for i,c in enumerate(self.LOGICAL_COLUMNS)}
        for iid in self.tree.get_children(""):
            vals=list(self.tree.item(iid)["values"])
            for col in self.STATUS_COLUMNS:
                i=colidx[col]; vals[i]=0 if col=="Ms" else "-"
            self.tree.item(iid,values=vals)
        self.status_var.set("Cleared status for all rows.")

    # CRUD
    def _add_dialog(self): DbEditor(self,on_save=self._add_target)
    def _edit_selected(self):
        sel=self.tree.selection()
        if not sel: messagebox.showinfo(APP_NAME,"Select a row to edit."); return
        name=sel[0]; t=next((x for x in self.targets if x.name==name),None)
        if not t: messagebox.showerror(APP_NAME,"Target not found."); return
        DbEditor(self,target=t,on_save=self._update_target)
    def _remove_selected(self):
        sel=self.tree.selection();
        if not sel: return
        name=sel[0]; self.targets=[t for t in self.targets if t.name!=name]; self.tree.delete(name); self._persist_targets(); self._renumber()
    def _add_target(self,t:DbTarget):
        if t.password and not t.password_enc: t.password_enc=_encrypt_password(t.password)
        if any(x.name==t.name for x in self.targets): messagebox.showerror(APP_NAME,"A target with this name already exists."); return
        self.targets.append(t); self._persist_targets()
        values=["-"]*len(self.LOGICAL_COLUMNS); values[0]=len(self.targets); values[1]=t.name; values[2]=t.environment
        self.tree.insert("",tk.END,iid=t.name,values=tuple(values)); self._renumber(); self._autosize_columns()
    def _update_target(self,t:DbTarget):
        if t.password and not t.password_enc: t.password_enc=_encrypt_password(t.password)
        for i,x in enumerate(self.targets):
            if x.name==t.name: self.targets[i]=t; break
        else: self.targets.append(t)
        self._persist_targets()
        if t.name in self.tree.get_children(""):
            vals=list(self.tree.item(t.name)["values"]); vals[1]=t.name; vals[2]=t.environment; self.tree.item(t.name,values=vals)
        self._autosize_columns()
    def _persist_targets(self):
        self.cfg["interval_sec"]=self.interval_var.get(); self.cfg["targets"]=[_serialize_target(t) for t in self.targets]; self.cfg["client_lib_dir"]=self.client_dir_var.get(); self.cfg["auto_run"]=self.auto_var.get(); save_config(self.cfg)

    # Import/Export & Email
    def _import_json(self):
        p=filedialog.askopenfilename(title="Import config.json",filetypes=[["JSON","*.json"]]); 
        if not p: return
        try:
            cfg=json.loads(Path(p).read_text(encoding="utf-8")); self.cfg.update(cfg)
            if "email" in cfg: self.cfg["email"].update(cfg["email"] or {})
            self.interval_var.set(int(self.cfg.get("interval_sec",DEFAULT_INTERVAL_SEC))); self.client_dir_var.set(self.cfg.get("client_lib_dir","")); self.auto_var.set(bool(self.cfg.get("auto_run",False)))
            self.targets=[_hydrate_target(t) for t in self.cfg.get("targets",[])]; self.last_health=self.cfg.get("last_health",{})
            order=[c for c in self.cfg.get("column_order",list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
            if order and order[0]!="S.No": order=["S.No"]+[c for c in order if c!="S.No"]
            visible=[c for c in self.cfg.get("visible_columns",order) if c in self.LOGICAL_COLUMNS]
            if visible and visible[0]!="S.No": visible=["S.No"]+[c for c in visible if c!="S.No"]
            self.tree["displaycolumns"]=visible
            if "column_widths" in self.cfg:
                for col,w in self.cfg["column_widths"].items():
                    try: self.tree.column(col,width=int(w))
                    except: pass
            save_config(self.cfg); self._refresh_table_from_targets(); self._load_last_health_into_rows(); messagebox.showinfo(APP_NAME,"Imported configuration.")
        except Exception as e: messagebox.showerror(APP_NAME,f"Failed to import: {e}")
    def _export_json(self):
        p=filedialog.asksaveasfilename(title="Export config.json",defaultextension=".json",initialfile="config.json"); 
        if not p: return
        try:
            export={"interval_sec":self.interval_var.get(),"targets":[_serialize_target(t) for t in self.targets],"client_lib_dir":self.client_dir_var.get(),
                    "email":{"server":self.smtp_server_var.get().strip(),"port":int(self.smtp_port_var.get() or 25),"from_addr":self.from_var.get().strip(),"to_addrs":self.to_var.get().strip(),"subject":self.cfg.get("email",{}).get("subject","Oracle DB Health Report")},
                    "last_health":self.last_health,"auto_run":self.auto_var.get(),
                    "column_order":list(self.cfg.get("column_order",self.LOGICAL_COLUMNS)),"visible_columns":list(self.tree["displaycolumns"]),"email_columns":list(self.cfg.get("email_columns",self.LOGICAL_COLUMNS)),
                    "column_widths":{c:self.tree.column(c,"width") for c in self.LOGICAL_COLUMNS}}
            Path(p).write_text(json.dumps(export,indent=2),encoding="utf-8"); messagebox.showinfo(APP_NAME,"Exported configuration.")
        except Exception as e: messagebox.showerror(APP_NAME,f"Failed to export: {e}")
    def _save_mail_settings(self):
        self.cfg.setdefault("email",{}); self.cfg["email"]["server"]=self.smtp_server_var.get().strip()
        try: self.cfg["email"]["port"]=int(self.smtp_port_var.get() or 25)
        except Exception: self.cfg["email"]["port"]=25
        self.cfg["email"]["from_addr"]=self.from_var.get().strip(); self.cfg["email"]["to_addrs"]=self.to_var.get().strip(); save_config(self.cfg); messagebox.showinfo(APP_NAME,"Mail settings saved.")
    def _email_report(self):
        email_cfg=self.cfg.get("email",{}); server=self.smtp_server_var.get().strip() or email_cfg.get("server",""); port=int(self.smtp_port_var.get() or email_cfg.get("port",25))
        from_addr=self.from_var.get().strip() or email_cfg.get("from_addr",""); to_addrs=self.to_var.get().strip() or email_cfg.get("to_addrs",""); subject=email_cfg.get("subject","Oracle DB Health Report")
        if not (server and from_addr and to_addrs): messagebox.showerror(APP_NAME,"Set SMTP server, From, and To addresses first."); return
        rows=[self.tree.item(i)["values"] for i in self.tree.get_children("")]; html=self._build_html(rows)
        try: self._send_html_email(server,port,from_addr,[x.strip() for x in to_addrs.split(",") if x.strip()],subject,html); messagebox.showinfo(APP_NAME,"Email report sent.")
        except Exception as e: messagebox.showerror(APP_NAME,f"Failed to send email: {e}")
    def _build_html(self,rows:List[List])->str:
        headers=[c for c in self.cfg.get("email_columns",list(self.LOGICAL_COLUMNS)) if c in self.LOGICAL_COLUMNS]
        if not headers: headers=list(self.LOGICAL_COLUMNS)
        def cell_style(text:str,col:str)->str:
            ok=None
            if col in ("Status","Inst_status","WorstTS%","LastFull/Inc","LastArch","Sessions","TS Online"):
                t=str(text).strip(); ok=True if t.startswith(GOOD) else (False if t.startswith(BAD) else None)
            if col=="WorstTS%":
                try: pct=float(str(text).split()[-1].replace("%","")); ok=(pct<90.0)
                except Exception: pass
            if ok is True: return "background-color:#e6ffe6;color:#064b00;font-weight:bold;"
            if ok is False: return "background-color:#ffe6e6;color:#7a0000;font-weight:bold;"
            return ""
        thead="<tr>"+ "".join(f"<th style='padding:6px 10px;border-bottom:1px solid #ccc;text-align:left'>{h}</th>" for h in headers) +"</tr>"
        body=[]
        for r in rows:
            tds=[]
            for col in headers:
                try: idx=self.LOGICAL_COLUMNS.index(col); val=r[idx]
                except Exception: val=""
                style=cell_style(val,col); tds.append(f"<td style='padding:4px 8px;border-bottom:1px solid #eee;{style}'>{val}</td>")
            body.append("<tr>"+ "".join(tds) +"</tr>")
        table="<table style='border-collapse:collapse;font-family:Segoe UI,Arial,sans-serif;font-size:12px'>"+ thead + "".join(body) +"</table>"
        title=f"<h3>Oracle DB Health Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h3>"; return "<html><body>"+title+table+"</body></html>"
    def _send_html_email(self,server:str,port:int,from_addr:str,to_addrs:List[str],subject:str,html:str):
        msg=MIMEMultipart("alternative"); msg["Subject"]=subject; msg["From"]=from_addr; msg["To"]=", ".join(to_addrs); msg.attach(MIMEText(html,"html","utf-8"))
        with smtplib.SMTP(server,port,timeout=20) as s: s.sendmail(from_addr,to_addrs,msg.as_string())
    def _on_close(self):
        self._persist_targets(); self.cfg["last_health"]=self.last_health; self._persist_column_layout(); save_config(self.cfg); self.master.destroy()

class DbEditor(tk.Toplevel):
    def __init__(self,parent:"MonitorApp",target:Optional[DbTarget]=None,on_save=None):
        super().__init__(parent); self.title("DB Target"); self.resizable(False,False); self.on_save=on_save
        self.var_name=tk.StringVar(value=target.name if target else ""); self.var_dsn=tk.StringVar(value=target.dsn if target else "")
        self.var_user=tk.StringVar(value=target.user if target else ""); initial_pwd=target.password if (target and target.password) else ""
        self.var_pwd=tk.StringVar(value=initial_pwd); self.var_wallet=tk.StringVar(value=target.wallet_dir if target else "")
        self.var_mode=tk.StringVar(value=target.mode if target else "thick"); self.var_env=tk.StringVar(value=target.environment if target else "NON-PROD")
        frm=ttk.Frame(self,padding=10); frm.pack(fill=tk.BOTH,expand=True)
        def row(lbl,var,show=None,browse=False):
            r=ttk.Frame(frm); r.pack(fill=tk.X,pady=4); ttk.Label(r,text=lbl,width=22).pack(side=tk.LEFT); e=ttk.Entry(r,textvariable=var,show=show,width=50); e.pack(side=tk.LEFT,padx=4)
            if browse: ttk.Button(r,text="...",width=3,command=lambda v=var:self._pick_dir(v)).pack(side=tk.LEFT)
        row("DB Name:",self.var_name); row("TNS Alias / EZConnect:",self.var_dsn); row("User:",self.var_user); row("Password:",self.var_pwd,show="*"); row("Wallet Dir:",self.var_wallet,browse=True)
        rmode=ttk.Frame(frm); rmode.pack(fill=tk.X,pady=4); ttk.Label(rmode,text="Mode:",width=22).pack(side=tk.LEFT); ttk.Radiobutton(rmode,text="Thick",value="thick",variable=self.var_mode).pack(side=tk.LEFT); ttk.Radiobutton(rmode,text="Thin",value="thin",variable=self.var_mode).pack(side=tk.LEFT)
        renv=ttk.Frame(frm); renv.pack(fill=tk.X,pady=4); ttk.Label(renv,text="Environment:",width=22).pack(side=tk.LEFT); self.env_combo=ttk.Combobox(renv,textvariable=self.var_env,state="readonly",values=["NON-PROD","PROD"],width=47); self.env_combo.pack(side=tk.LEFT,padx=4)
        btns=ttk.Frame(frm); btns.pack(fill=tk.X,pady=(10,2)); ttk.Button(btns,text="Save",command=self._save).pack(side=tk.RIGHT); ttk.Button(btns,text="Cancel",command=self.destroy).pack(side=tk.RIGHT,padx=6)
        self.grab_set(); self.transient(parent); self.wait_visibility(); self.focus()
    def _pick_dir(self,var:tk.StringVar):
        d=filedialog.askdirectory(title="Select wallet directory"); 
        if d: var.set(d)
    def _save(self):
        name=self.var_name.get().strip(); dsn=self.var_dsn.get().strip()
        if not name or not dsn: messagebox.showerror(APP_NAME,"DB Name and TNS Alias/EZConnect are required"); return
        pwd=self.var_pwd.get().strip() or None
        t=DbTarget(name=name, dsn=dsn, user=self.var_user.get().strip() or None, password=pwd, password_enc=_encrypt_password(pwd) if pwd else None, wallet_dir=self.var_wallet.get().strip() or None, mode=self.var_mode.get().strip() or "thick", environment=self.var_env.get().strip() or "NON-PROD")
        if self.on_save: self.on_save(t); self.destroy()

def main():
    cfg=load_config(); root=tk.Tk()
    try:
        if sys.platform.startswith("win"):
            from ctypes import windll; windll.shcore.SetProcessDpiAwareness(1)  # type: ignore
    except Exception: pass
    app=MonitorApp(root,cfg); root.geometry("2000x820"); root.mainloop()

if __name__=="__main__":
    main()
