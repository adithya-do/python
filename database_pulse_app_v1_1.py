#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database Pulse — App Launcher (v1.1)
- Title bar: "Database Pulse"
- Top banner (light orange) with left buttons: Home, Oracle Database, SQL Server (future)
- App opens on Landing; modules are embedded in the same window
"""

import sys
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Optional

APP_NAME = "Database Pulse"
APP_VERSION = "Database Pulse v1.0"

# Lazy imports inside methods to avoid circulars during packaging
def _lazy_import_oracle():
    import dp_oracle_module as mod
    return mod

def _lazy_import_sqlserver():
    import dp_sqlserver_module as mod
    return mod

class Landing(ttk.Frame):
    def __init__(self, master, version_text: str):
        super().__init__(master)
        self.configure(padding=20)
        # center container
        center = tk.Frame(self)
        center.grid(row=0, column=0, sticky="nsew")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # Icons and title
        icon = tk.Label(center, text="🗄️  💓", font=("Segoe UI Emoji", 48))
        icon.pack(pady=(0, 8))

        title = tk.Label(center, text="Database Pulse", font=("Segoe UI", 30, "bold"), fg="#1e61ff")
        title.pack()

        # Version under title
        ver = tk.Label(center, text=version_text, font=("Segoe UI", 11), fg="#555")
        ver.pack(pady=(2, 10))

        subtitle = tk.Label(center, text="A Database Health Check Tool", font=("Segoe UI", 9))
        subtitle.pack(pady=(4, 0))


class RouterApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        try:
            if sys.platform.startswith("win"):
                from ctypes import windll
                windll.shcore.SetProcessDpiAwareness(1)  # type: ignore
        except Exception:
            pass

        self.geometry("1600x900")

        # Top banner
        self.banner = tk.Frame(self, bg="#ffcc80")
        self.banner.pack(side=tk.TOP, fill=tk.X)

        # Left buttons
        left = tk.Frame(self.banner, bg="#ffcc80")
        left.pack(side=tk.LEFT, padx=6, pady=6)
        self._nav_btns: Dict[str, tk.Button] = {}

        def mkbtn(txt, key, cmd):
            b = tk.Button(
                left, text=txt, command=lambda k=key, c=cmd: self._select_nav(k, c),
                bg="#ffffff", fg="#000000", activebackground="#d6e6ff",
                activeforeground="#000000", font=("Segoe UI", 10),
                padx=10, pady=4, relief=tk.RAISED, bd=1
            )
            b.pack(side=tk.LEFT, padx=4)
            self._nav_btns[key] = b

        mkbtn("Home", "home", self.show_home)
        mkbtn("Oracle Database", "oracle", self.show_oracle)
        mkbtn("SQL Server", "sql", self.show_sqlserver)

        # Right brand + version
        right = tk.Frame(self.banner, bg="#ffcc80")
        right.pack(side=tk.RIGHT, padx=10, pady=6)
        tk.Label(right, text="💓 Database Pulse", bg="#ffcc80", fg="#1e61ff", font=("Segoe UI", 14, "bold")).pack(side=tk.RIGHT)
        tk.Label(right, text=f" {APP_VERSION} ", bg="#ffcc80", fg="#222222", font=("Segoe UI", 9)).pack(side=tk.RIGHT, padx=(6,0))

        # Content stack (grid)
        self.stack = tk.Frame(self)
        self.stack.pack(fill=tk.BOTH, expand=True)
        self.stack.grid_rowconfigure(0, weight=1)
        self.stack.grid_columnconfigure(0, weight=1)

        # Views/containers
        self.view_landing = Landing(self.stack, APP_VERSION)
        self.oracle_container = tk.Frame(self.stack)   # container for Oracle module
        self.oracle_container.grid_rowconfigure(0, weight=1)
        self.oracle_container.grid_columnconfigure(0, weight=1)

        self.sql_container = tk.Frame(self.stack)      # container for SQL Server placeholder
        self.sql_container.grid_rowconfigure(0, weight=1)
        self.sql_container.grid_columnconfigure(0, weight=1)

        self.view_oracle: Optional[tk.Frame] = None
        self.view_sql: Optional[tk.Frame] = None

        # Default: Landing page
        self._select_nav("home", self.show_home)

    def _clear_nav_colors(self):
        for b in self._nav_btns.values():
            b.configure(bg="#ffffff", fg="#000000")

    def _select_nav(self, key: str, route_call):
        self._clear_nav_colors()
        if key in self._nav_btns:
            self._nav_btns[key].configure(bg="#6ea8fe")  # light blue highlight
        route_call()

    def _show(self, frame: tk.Frame):
        # Hide everything, then grid the target and raise
        for child in list(self.stack.winfo_children()):
            try:
                child.grid_forget()
            except Exception:
                pass
            try:
                child.pack_forget()
            except Exception:
                pass
            try:
                child.place_forget()
            except Exception:
                pass
        try:
            frame.grid(row=0, column=0, sticky="nsew")
        except Exception:
            pass
        try:
            frame.tkraise()
        except Exception:
            pass

    def show_home(self):
        try:
            self.view_landing.grid(row=0, column=0, sticky="nsew")
        except Exception:
            pass
        self._show(self.view_landing)

    def show_oracle(self):
        # Load on demand
        if self.view_oracle is None:
            mod = _lazy_import_oracle()
            self.view_oracle = self.oracle_container
            # v18 MonitorApp expects a .title on its master; provide a no-op
            if not hasattr(self.oracle_container, "title"):
                self.oracle_container.title = lambda *a, **k: None
            if not any(isinstance(w, ttk.Frame) and w.__class__.__name__ == "MonitorApp" 
                       for w in self.oracle_container.winfo_children()):
                mon = mod.MonitorApp(self.oracle_container, mod.load_config())
                try:
                    mon.grid(row=0, column=0, sticky="nsew")
                except Exception:
                    pass
        try:
            self.view_oracle.grid(row=0, column=0, sticky="nsew")
        except Exception:
            pass
        self._show(self.view_oracle)

    def show_sqlserver(self):
        if self.view_sql is None:
            mod = _lazy_import_sqlserver()
            self.view_sql = self.sql_container
            if not self.sql_container.winfo_children():
                inner = mod.SqlServerPlaceholder(self.sql_container)
                inner.grid(row=0, column=0, sticky="nsew")
        try:
            self.view_sql.grid(row=0, column=0, sticky="nsew")
        except Exception:
            pass
        self._show(self.view_sql)


def main():
    app = RouterApp()
    app.mainloop()


if __name__ == "__main__":
    main()
