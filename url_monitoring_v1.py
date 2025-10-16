#!/usr/bin/env python3
"""
url_monitor_gui.py

Simple GUI monitor for URLs / web applications with optional login (none/basic/form).
Saves monitor definitions to config/url_monitor.json in the script directory.

Requires: requests
Install: pip install requests
"""

import json
import os
import threading
import time
import traceback
from datetime import datetime
from queue import Queue, Empty
from typing import Dict, Any

import requests
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

# ---- Config ----
SCRIPT_DIR = os.path.dirname(__file__)
CONFIG_DIR = os.path.join(SCRIPT_DIR, "config")
CONFIG_FILE = os.path.join(CONFIG_DIR, "url_monitor.json")
DEFAULT_INTERVAL = 30  # seconds

# ensure config folder exists
os.makedirs(CONFIG_DIR, exist_ok=True)

# ---- Model ----
class Monitor:
    def __init__(self, data: Dict[str, Any]):
        self.name = data.get("name", "Unnamed")
        self.url = data.get("url", "")
        self.enabled = bool(data.get("enabled", True))
        self.interval = int(data.get("interval", DEFAULT_INTERVAL))
        self.auth_type = data.get("auth_type", "none")  # 'none', 'basic', 'form'
        self.username = data.get("username", "")
        self.password = data.get("password", "")
        self.login_url = data.get("login_url", "")
        self.form_username_field = data.get("form_username_field", "username")
        self.form_password_field = data.get("form_password_field", "password")
        self.login_method = data.get("login_method", "POST")
        self.last_status = data.get("last_status", "Unknown")
        self.last_checked = data.get("last_checked", None)
        self.last_detail = data.get("last_detail", "")
        self.session_cookies = None  # runtime only
        self.id = data.get("id", None)
        self.app_type = data.get("app_type", "")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "url": self.url,
            "enabled": self.enabled,
            "interval": self.interval,
            "auth_type": self.auth_type,
            "username": self.username,
            "password": self.password,
            "login_url": self.login_url,
            "form_username_field": self.form_username_field,
            "form_password_field": self.form_password_field,
            "login_method": self.login_method,
            "last_status": self.last_status,
            "last_checked": self.last_checked,
            "last_detail": self.last_detail,
            "app_type": self.app_type,
        }

# ---- Persistence ----
def load_monitors() -> Dict[str, "Monitor"]:
    monitors: Dict[str, Monitor] = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            for d in arr:
                m = Monitor(d)
                if not m.id:
                    m.id = f"m-{len(monitors)+1}-{int(time.time())}"
                monitors[m.id] = m
        except Exception:
            print("Error loading url_monitor.json:", traceback.format_exc())
    return monitors

def save_monitors(monitors: Dict[str, "Monitor"]):
    arr = [m.to_dict() for m in monitors.values()]
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, indent=2, ensure_ascii=False)

# ---- Network check logic ----
def perform_check(monitor: "Monitor", timeout=15):
    s = requests.Session()
    start = time.time()
    try:
        auth = None
        if monitor.auth_type == "basic":
            auth = (monitor.username, monitor.password) if monitor.username else None

        if monitor.auth_type == "form":
            if not monitor.login_url:
                return False, "Login URL not set", "login_url missing for form auth"
            payload = {
                monitor.form_username_field: monitor.username,
                monitor.form_password_field: monitor.password,
            }
            if monitor.login_method.upper() == "POST":
                r_login = s.post(monitor.login_url, data=payload, timeout=timeout, allow_redirects=True)
            else:
                r_login = s.get(monitor.login_url, params=payload, timeout=timeout, allow_redirects=True)
            if not (200 <= r_login.status_code < 400):
                return False, f"Login failed ({r_login.status_code})", f"Login response: {r_login.status_code} {r_login.reason}\nBody snippet: {r_login.text[:400]}"

        r = s.get(monitor.url, auth=auth, timeout=timeout)
        elapsed = time.time() - start
        if 200 <= r.status_code < 400:
            return True, f"OK ({r.status_code})", f"HTTP {r.status_code}, elapsed: {elapsed:.2f}s"
        else:
            return False, f"Error ({r.status_code})", f"HTTP {r.status_code} {r.reason}\nBody snippet: {r.text[:800]}"
    except Exception as ex:
        tb = traceback.format_exc()
        return False, "Exception", f"{repr(ex)}\n{tb}"

# ---- GUI ----
class URLMonitorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("URL / App Monitor")
        self.monitors: Dict[str, Monitor] = load_monitors()
        self.threads: Dict[str, threading.Thread] = {}
        self.stop_flags: Dict[str, threading.Event] = {}
        self.queue = Queue()

        self.create_widgets()
        self.populate_table()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(500, self.process_queue)

    def create_widgets(self):
        frm = ttk.Frame(self.root, padding=8)
        frm.pack(fill="both", expand=True)

        columns = ("name", "url", "type", "auth", "interval", "status", "last_checked", "detail")
        self.tree = ttk.Treeview(frm, columns=columns, show="headings", selectmode="browse")
        for col in columns:
            self.tree.heading(col, text=col.replace("_", " ").title())
            self.tree.column(col, width=120, anchor="w")
        self.tree.column("url", width=280)
        self.tree.column("detail", width=300)
        self.tree.pack(fill="both", expand=True, side="top")

        btn_frm = ttk.Frame(frm)
        btn_frm.pack(fill="x", pady=6)
        ttk.Button(btn_frm, text="Add Monitor", command=self.add_monitor).pack(side="left")
        ttk.Button(btn_frm, text="Edit Monitor", command=self.edit_selected).pack(side="left")
        ttk.Button(btn_frm, text="Remove Monitor", command=self.remove_selected).pack(side="left")
        ttk.Button(btn_frm, text="Start Selected", command=self.start_selected).pack(side="left")
        ttk.Button(btn_frm, text="Stop Selected", command=self.stop_selected).pack(side="left")
        ttk.Button(btn_frm, text="Start All", command=self.start_all).pack(side="left")
        ttk.Button(btn_frm, text="Stop All", command=self.stop_all).pack(side="left")
        ttk.Button(btn_frm, text="Test Selected", command=self.test_selected).pack(side="left")
        ttk.Button(btn_frm, text="Import JSON", command=self.import_json).pack(side="left")
        ttk.Button(btn_frm, text="Export JSON", command=self.export_json).pack(side="left")

        self.status_var = tk.StringVar(value=f"Ready — using {CONFIG_FILE}")
        ttk.Label(self.root, textvariable=self.status_var, relief="sunken", anchor="w").pack(fill="x", side="bottom")

    def populate_table(self):
        self.tree.delete(*self.tree.get_children())
        for mid, m in self.monitors.items():
            self.tree.insert("", "end", iid=mid, values=self.row_values(m))
            self.update_row_color(mid, m.last_status)

    def row_values(self, m: Monitor):
        return (
            m.name,
            m.url,
            m.app_type,
            m.auth_type,
            str(m.interval),
            m.last_status if m.last_status else "Unknown",
            m.last_checked or "",
            (m.last_detail[:200] + "...") if m.last_detail and len(m.last_detail) > 200 else (m.last_detail or ""),
        )

    def update_row(self, mid):
        m = self.monitors[mid]
        if not self.tree.exists(mid):
            self.tree.insert("", "end", iid=mid, values=self.row_values(m))
        else:
            self.tree.item(mid, values=self.row_values(m))
        self.update_row_color(mid, m.last_status)

    def update_row_color(self, mid, last_status):
        try:
            if last_status and last_status.startswith("OK"):
                bg = "#ccffcc"
            elif last_status in (None, "", "Unknown"):
                bg = "#ffffcc"
            else:
                bg = "#ffcccc"
            self.tree.tag_configure(mid, background=bg)
            self.tree.item(mid, tags=(mid,))
        except Exception:
            pass

    # ---- Monitor management ----
    def add_monitor(self):
        editor = MonitorEditor(self.root)
        data = editor.show()
        if data:
            m = Monitor(data)
            m.id = f"m-{len(self.monitors)+1}-{int(time.time())}"
            self.monitors[m.id] = m
            save_monitors(self.monitors)
            self.populate_table()
            self.status_var.set(f"Added monitor {m.name}")

    def edit_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Edit Monitor", "Select a monitor first")
            return
        mid = sel[0]
        m = self.monitors[mid]
        editor = MonitorEditor(self.root, initial=m.to_dict())
        data = editor.show()
        if data:
            updated = Monitor(data)
            updated.id = mid
            self.monitors[mid] = updated
            save_monitors(self.monitors)
            self.populate_table()
            self.status_var.set(f"Updated monitor {updated.name}")

    def remove_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Remove Monitor", "Select a monitor first")
            return
        mid = sel[0]
        if messagebox.askyesno("Confirm", f"Remove monitor '{self.monitors[mid].name}'?"):
            self.stop_monitor(mid)
            del self.monitors[mid]
            save_monitors(self.monitors)
            self.populate_table()
            self.status_var.set("Monitor removed")

    # ---- Start / Stop ----
    def monitor_loop(self, mid: str, stop_event: threading.Event):
        m = self.monitors[mid]
        while not stop_event.is_set():
            if not m.enabled:
                time.sleep(1)
                continue
            ok, status_text, detail = perform_check(monitor=m)
            timestamp = datetime.utcnow().isoformat() + "Z"
            m.last_status = status_text
            m.last_checked = timestamp
            m.last_detail = detail
            self.queue.put(("update", mid))
            try:
                save_monitors(self.monitors)
            except Exception:
                pass
            for _ in range(max(1, int(m.interval))):
                if stop_event.is_set():
                    break
                time.sleep(1)
        self.queue.put(("stopped", mid))

    def start_monitor(self, mid: str):
        if mid in getattr(self, "threads", {}) and self.threads[mid].is_alive():
            return
        stop_event = threading.Event()
        thread = threading.Thread(target=self.monitor_loop, args=(mid, stop_event), daemon=True)
        self.stop_flags[mid] = stop_event
        self.threads[mid] = thread
        thread.start()
        self.status_var.set(f"Started monitor {self.monitors[mid].name}")

    def stop_monitor(self, mid: str):
        if mid in self.stop_flags:
            self.stop_flags[mid].set()
        self.status_var.set(f"Stopping monitor {self.monitors[mid].name}")

    def start_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Start Monitor", "Select a monitor first")
            return
        self.start_monitor(sel[0])

    def stop_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Stop Monitor", "Select a monitor first")
            return
        self.stop_monitor(sel[0])

    def start_all(self):
        for mid in list(self.monitors.keys()):
            self.start_monitor(mid)

    def stop_all(self):
        for mid in list(self.stop_flags.keys()):
            self.stop_monitor(mid)

    # ---- Test / Import / Export ----
    def test_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Test Monitor", "Select a monitor first")
            return
        mid = sel[0]
        m = self.monitors[mid]
        self.status_var.set(f"Testing {m.name} ...")

        def _run_test():
            ok, status, detail = perform_check(monitor=m, timeout=20)
            m.last_status = status
            m.last_checked = datetime.utcnow().isoformat() + "Z"
            m.last_detail = detail
            save_monitors(self.monitors)
            self.queue.put(("testdone", mid))
        threading.Thread(target=_run_test, daemon=True).start()

    def import_json(self):
        fn = filedialog.askopenfilename(
            title="Import JSON",
            initialdir=CONFIG_DIR,
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not fn:
            return
        try:
            with open(fn, "r", encoding="utf-8") as f:
                arr = json.load(f)
            for d in arr:
                m = Monitor(d)
                if not m.id:
                    m.id = f"m-{len(self.monitors)+1}-{int(time.time())}"
                while m.id in self.monitors:
                    m.id = f"{m.id}-{int(time.time())}"
                self.monitors[m.id] = m
            save_monitors(self.monitors)
            self.populate_table()
            messagebox.showinfo("Import JSON", "Import complete")
        except Exception as ex:
            messagebox.showerror("Import JSON", f"Failed to import: {ex}")

    def export_json(self):
        fn = filedialog.asksaveasfilename(
            title="Export JSON",
            defaultextension=".json",
            initialdir=CONFIG_DIR,
            initialfile="url_monitor.json",
            filetypes=[("JSON files", "*.json")],
        )
        if not fn:
            return
        try:
            arr = [m.to_dict() for m in self.monitors.values()]
            with open(fn, "w", encoding="utf-8") as f:
                json.dump(arr, f, indent=2, ensure_ascii=False)
            messagebox.showinfo("Export JSON", "Exported successfully")
        except Exception as ex:
            messagebox.showerror("Export JSON", f"Failed to export: {ex}")

    # ---- Queue & shutdown ----
    def process_queue(self):
        try:
            while True:
                action, mid = self.queue.get_nowait()
                if action in ("update", "testdone"):
                    self.update_row(mid)
                    self.status_var.set(f"{action} - {self.monitors[mid].name}: {self.monitors[mid].last_status}")
                elif action == "stopped":
                    self.status_var.set(f"Stopped {self.monitors[mid].name}")
                self.queue.task_done()
        except Empty:
            pass
        self.root.after(500, self.process_queue)

    def on_close(self):
        if messagebox.askokcancel("Quit", "Stop monitoring and quit?"):
            for mid, ev in self.stop_flags.items():
                ev.set()
            time.sleep(0.2)
            self.root.destroy()

# ---- Monitor editor dialog ----
class MonitorEditor(simpledialog.Dialog):
    def __init__(self, parent, initial=None):
        self.initial = initial or {}
        super().__init__(parent, title="Monitor Editor")

    def body(self, master):
        row = 0
        ttk.Label(master, text="Name").grid(row=row, column=0, sticky="w")
        self.name_var = tk.StringVar(value=self.initial.get("name", ""))
        ttk.Entry(master, textvariable=self.name_var, width=50).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="URL to check").grid(row=row, column=0, sticky="w")
        self.url_var = tk.StringVar(value=self.initial.get("url", ""))
        ttk.Entry(master, textvariable=self.url_var, width=50).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Application Type (optional)").grid(row=row, column=0, sticky="w")
        self.app_type_var = tk.StringVar(value=self.initial.get("app_type", ""))
        ttk.Entry(master, textvariable=self.app_type_var).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Auth Type").grid(row=row, column=0, sticky="w")
        self.auth_var = tk.StringVar(value=self.initial.get("auth_type", "none"))
        ttk.Combobox(master, textvariable=self.auth_var, values=("none", "basic", "form"), state="readonly").grid(row=row, column=1, sticky="w"); row += 1

        ttk.Label(master, text="Username").grid(row=row, column=0, sticky="w")
        self.username_var = tk.StringVar(value=self.initial.get("username", ""))
        ttk.Entry(master, textvariable=self.username_var).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Password").grid(row=row, column=0, sticky="w")
        self.password_var = tk.StringVar(value=self.initial.get("password", ""))
        ttk.Entry(master, textvariable=self.password_var, show="*").grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Login URL (for form auth)").grid(row=row, column=0, sticky="w")
        self.login_url_var = tk.StringVar(value=self.initial.get("login_url", ""))
        ttk.Entry(master, textvariable=self.login_url_var, width=50).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Form username field").grid(row=row, column=0, sticky="w")
        self.form_user_field_var = tk.StringVar(value=self.initial.get("form_username_field", "username"))
        ttk.Entry(master, textvariable=self.form_user_field_var).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Form password field").grid(row=row, column=0, sticky="w")
        self.form_pwd_field_var = tk.StringVar(value=self.initial.get("form_password_field", "password"))
        ttk.Entry(master, textvariable=self.form_pwd_field_var).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Login method (GET/POST)").grid(row=row, column=0, sticky="w")
        self.login_method_var = tk.StringVar(value=self.initial.get("login_method", "POST"))
        ttk.Combobox(master, textvariable=self.login_method_var, values=("POST", "GET"), state="readonly").grid(row=row, column=1, sticky="w"); row += 1

        ttk.Label(master, text="Interval (seconds)").grid(row=row, column=0, sticky="w")
        self.interval_var = tk.IntVar(value=int(self.initial.get("interval", DEFAULT_INTERVAL)))
        ttk.Entry(master, textvariable=self.interval_var).grid(row=row, column=1, sticky="we"); row += 1

        ttk.Label(master, text="Enabled").grid(row=row, column=0, sticky="w")
        self.enabled_var = tk.BooleanVar(value=bool(self.initial.get("enabled", True)))
        ttk.Checkbutton(master, variable=self.enabled_var).grid(row=row, column=1, sticky="w"); row += 1

        help_txt = ("Note: credentials are stored in plaintext in config/url_monitor.json\n"
                    "If you need secure storage, integrate OS keyring or a secrets vault.")
        ttk.Label(master, text=help_txt, foreground="gray").grid(row=row, column=0, columnspan=2, sticky="w"); row += 1
        return None

    def apply(self):
        data = {
            "name": self.name_var.get().strip(),
            "url": self.url_var.get().strip(),
            "app_type": self.app_type_var.get().strip(),
            "auth_type": self.auth_var.get(),
            "username": self.username_var.get(),
            "password": self.password_var.get(),
            "login_url": self.login_url_var.get().strip(),
            "form_username_field": self.form_user_field_var.get().strip(),
            "form_password_field": self.form_pwd_field_var.get().strip(),
            "login_method": self.login_method_var.get().strip(),
            "interval": int(self.interval_var.get()),
            "enabled": bool(self.enabled_var.get()),
        }
        if not data["name"] or not data["url"]:
            messagebox.showerror("Validation", "Name and URL are required")
            self.result = None
            return
        self.result = data

    def show(self):
        self.wait_window()
        return getattr(self, "result", None)

# ---- Entry point ----
def main():
    root = tk.Tk()
    app = URLMonitorApp(root)
    root.geometry("1100x600")
    root.mainloop()

if __name__ == "__main__":
    main()
