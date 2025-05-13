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
SMTP_SERVER = 'smtp.example.com'   # Replace with your SMTP server
SMTP_PORT = 25                     # Use 587 for TLS if needed
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

def parse_info_all(info_output):
    alerts = []
    lines = info_output.strip().splitlines()
    headers_found = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if not headers_found and 'Lag at Chkpt' in line and 'Time Since Chkpt' in line:
            headers_found = True
            continue

        if headers_found and (line.startswith("REPLICAT") or line.startswith("EXTRACT")):
            parts = re.split(r'\s{2,}', line)
            if len(parts) < 5:
                continue

            program = parts[0]
            status = parts[1].upper()
            group = parts[2]
            lag_str = parts[3]
            since_chkpt_str = parts[4]

            lag = parse_lag_time(lag_str)
            since_chkpt = parse_lag_time(since_chkpt_str)
            severity = None

            # Determine severity
            if status in ["STOPPED", "ABENDED"]:
                severity = "Critical"
            elif lag >= timedelta(minutes=LAG_CRITICAL_MIN) or since_chkpt >= timedelta(minutes=LAG_CRITICAL_MIN):
                severity = "Critical"
            elif lag >= timedelta(minutes=LAG_WARNING_MIN) or since_chkpt >= timedelta(minutes=LAG_WARNING_MIN):
                severity = "Warning"

            if severity:
                alerts.append({
                    'program': program,
                    'group': group,
                    'status': status,
                    'lag': lag_str,
                    'since': since_chkpt_str,
                    'severity': severity
                })

    return alerts

def generate_html_report(db_name, manager_status, alerts):
    style = """
    <style>
    body { font-family: Arial; }
    table { border-collapse: collapse; width: 100%%; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
    th { background-color: #f2f2f2; }
    .Critical { background-color: #ffdddd; }
    .Warning { background-color: #fff4cc; }
    </style>
    """
    html = f"<html><head>{style}</head><body>"
    html += f"<h2>GoldenGate Alert Report - {db_name}</h2>"
    html += "<h3>Manager Status</h3><pre>{}</pre>".format(manager_status.strip())

    if alerts:
        html += "<h3>Detected Issues</h3>"
        html += """
        <table>
            <tr>
                <th>Program</th>
                <th>Group</th>
                <th>Status</th>
                <th>Lag at Chkpt</th>
                <th>Time Since Chkpt</th>
                <th>Severity</th>
            </tr>
        """
        for a in alerts:
            css_class = a['severity']
            html += f"""
                <tr class="{css_class}">
                    <td>{a['program']}</td>
                    <td>{a['group']}</td>
                    <td>{a['status']}</td>
                    <td>{a['lag']}</td>
                    <td>{a['since']}</td>
                    <td><strong>{a['severity']}</strong></td>
                </tr>
            """
        html += "</table>"
    else:
        html += "<p><strong>All GoldenGate processes are running normally.</strong></p>"

    html += "</body></html>"
    return html

def send_email(subject, html_body, to_email):
    msg = MIMEText(html_body, 'html')
    msg['Subject'] = subject
    msg['From'] = formataddr((FROM_NAME, FROM_EMAIL))
    msg['To'] = to_email

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10) as server:
            server.sendmail(FROM_EMAIL, [to_email], msg.as_string())
    except Exception as e:
        print(f"[ERROR] Failed to send email to {to_email}: {e}")

def monitor():
    configs = read_config()
    hostname = socket.gethostname()

    for entry in configs:
        gg_home = entry['gg_home']
        db_name = entry['db_name']
        email = entry['email']

        print(f"[INFO] Checking {db_name} at {gg_home}...")

        info_output = run_ggsci_command(gg_home, 'info all')
        mgr_output = run_ggsci_command(gg_home, 'info manager')

        alerts = parse_info_all(info_output)
        if alerts:
            html_report = generate_html_report(db_name, mgr_output, alerts)
            subject = f"[ALERT] GoldenGate issue on {db_name} @ {hostname}"
            send_email(subject, html_report, email)
        else:
            print(f"[OK] {db_name}: All processes healthy.")

if __name__ == '__main__':
    monitor()
