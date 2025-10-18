#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Server module placeholder for Database Pulse.
"""

import tkinter as tk
from tkinter import ttk

class SqlServerPlaceholder(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        l = ttk.Label(self, text="SQL Server Monitoring — Coming Soon", font=("Segoe UI", 20, "bold"))
        l.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
