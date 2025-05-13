import subprocess
import os
import smtplib
import socket
import re
from datetime import timedelta
from email.mime.text import MIMEText
from email.utils import formataddr

# === CONFIGURATION ===
CONFIG_FILE = '/opt/oracle/scripts/ogg_mon/ogg.conf'
SMTP_SERVER = 'smtp.example.com'  # Replace with your SMTP server
SMTP_PORT = 25                    # Use 587 for TLS if needed
FROM_EMAIL = 'ogg-monitor@example.com'
FROM_NAME = 'OGG Monitor'
LAG_WARNING_MIN = 30
LAG_CRITICAL_MIN = 60

def read_config():
    entries = []
    with open(CONFIG_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('|')
            if len(parts) == 3:
                gg_home, db_name, email = parts
                entries.append({
                    'gg_home': gg_home.strip(),
                    'db_name': db_name.strip(),
                    'email': email.strip()
                })
    return entries

def run_ggsci_command(gg_home, command):
    ggsci = os.path.join(gg_home, 'ggsci')
    try:
        output = subprocess.check_output(
            f'echo "{command}" | {ggsci}',
            shell=True, stderr=subprocess.STDOUT, text=True
        )
        return output
    except subprocess.CalledProcessError as e:
        return f"Error executing GGSCI in {gg_home}: {e.output}"

def parse_lag_time(lag_str):
    try:
        h, m, s = map(int, lag_str.strip().split(":"))
        return timedelta(hours=h, minutes=m, seconds=s)
    except:
        return timedelta(0)

def parse_status(info_output):
    alerts = []
    for line in info_output.splitlines():
        if not line.strip().startswith(('EXTRACT', 'REPLICAT')):
            continue

        parts = re.split(r'\s+', line.strip())
        if len(parts) < 5:
            continue

        proc_type, name, status, lag_str = parts[:4]
        status = status.upper()
        lag = parse_lag_time(lag_str)

        if status in ['STOPPED', 'ABENDED']:
            alerts.append(f"<b>{proc_type} {name}</b>: <span style='color:red'>Status: {status}</span>")
        if lag >= timedelta(minutes=LAG_CRITICAL_MIN):
            alerts.append(f"<b>{proc_type} {name}</b>: <span style='color:red'>Lag: {lag_str} (Critical)</span>")
        elif lag >= timedelta(minutes=LAG_WARNING_MIN):
            alerts.append(f"<b>{proc_type} {name}</b>: <span style='color_
