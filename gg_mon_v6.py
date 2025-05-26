import os
import socket
import subprocess
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict
import re

CONFIG_FILE = '/opt/oracle/scripts/ogg_mon/ogg.conf'
LAG_WARNING_THRESHOLD_MINUTES = 30
LAG_CRITICAL_THRESHOLD_MINUTES = 60

def read_config():
    configs = []
    with open(CONFIG_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('|')
                if len(parts) == 3:
                    gg_home, db_name, email = parts
                    configs.append({'gg_home': gg_home.strip(), 'db_name': db_name.strip(), 'email': email.strip()})
    return configs

def run_ggsci_command(gg_home, command):
    ggsci = os.path.join(gg_home, 'ggsci')
    try:
        output = subprocess.check_output(
            f'echo "{command}" | {ggsci}',
            shell=True, stderr=subprocess.STDOUT, text=True
        )
        return output.strip()
    except subprocess.CalledProcessError as e:
        return f"Error executing GGSCI in {gg_home}: {e.output.strip()}"

def extract_manager_status(info_output):
    lines = info_output.splitlines()
    manager_lines = [line.strip() for line in lines if line.strip().startswith("MANAGER")]
    if not manager_lines:
        return ("Manager is Down", True)
    status_line = manager_lines[0]
    if "STOPPED" in status_line or "ABENDED" in status_line:
        return (status_line, True)
    return (status_line, False)

def parse_info_all(output):
    alerts = []
    block = ''
    inside = False
    for line in output.splitlines():
        if re.match(r'^\s*(EXTRACT|REPLICAT)', line):
            if block:
                parsed = parse_process_block(block)
                if parsed:
                    alerts.append(parsed)
            block = line
            inside = True
        elif inside:
            block += '\n' + line
    if block:
        parsed = parse_process_block(block)
        if parsed:
            alerts.append(parsed)
    return alerts

def parse_process_block(block):
    lines = block.strip().splitlines()
    header = lines[0].split()
    if len(header) < 3:
        return None
    program, group, status = header[0], header[1], header[2]
    lag = since = '-'
    for line in lines[1:]:
        if 'Lag at Chkpt' in line:
            lag = line.split(':', 1)[1].strip()
        elif 'Time Since Chkpt' in line:
            since = line.split(':', 1)[1].strip()

    lag_mins = parse_lag_to_minutes(lag)
    since_mins = parse_lag_to_minutes(since)
    max_lag = max(lag_mins, since_mins)

    severity = None
    if status.upper() != "RUNNING":
        severity = "Critical"
    elif max_lag >= LAG_CRITICAL_THRESHOLD_MINUTES:
        severity = "Critical"
    elif max_lag >= LAG_WARNING_THRESHOLD_MINUTES:
        severity = "Warning"

    if severity:
        return {
            'program': program,
            'group': group,
            'status': status,
            'lag': lag,
            'since': since,
            'severity': severity
        }
    return None

def parse_lag_to_minutes(value):
    if not value or value.strip() == '-':
        return 0
    parts = value.strip().split(':')
    try:
        parts = list(map(int, parts))
        if len(parts) == 3:
            return parts[0]*60 + parts[1] + parts[2]/60
        elif len(parts) == 2:
            return parts[0]*60 + parts[1]
        elif len(parts) == 1:
            return int(parts[0])
    except ValueError:
        return 0
    return 0

def generate_consolidated_report(db_sections):
    style = """
    <style>
    body {
        font-family: Arial, sans-serif;
        font-size: 14px;
        color: #333;
        padding: 20px;
    }
    table {
        border-collapse: collapse;
        width: 100%%;
        margin-top: 10px;
    }
    th, td {
        border: 1px solid #ccc;
        padding: 10px 14px;
        text-align: center;
    }
    th {
        background-color: #f2f2f2;
        font-weight: bold;
    }
    tr:hover {
        background-color: #f9f9f9;
    }
    .Critical {
        background-color: #ffe5e5;
    }
    .Warning {
        background-color: #fff7cc;
    }
    h2, h3 {
        color: #2c3e50;
    }
    .mgr-down {
        color: red;
        font-weight: bold;
    }
    </style>
    """

    html = f"<html><head>{style}</head><body>"
    html += f"<h2>Consolidated GoldenGate Alert Report</h2>"

    for section in db_sections:
        db_name = section['db_name']
        mgr_status = section['manager_status']
        mgr_is_down = section['mgr_is_down']
        alerts = section['alerts']

        html += f"<h3>Database: {db_name}</h3>"

        html += "<p><strong>Manager Status:</strong><br>"
        if mgr_is_down:
            html += f"<span class='mgr-down'>{mgr_status}</span></p>"
        else:
            html += f"<pre>{mgr_status}</pre></p>"

        if alerts:
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
            html += "<p>No process issues detected.</p>"

        html += "<hr>"

    html += "</body></html>"
    return html

def send_email(subject, html_body, recipient):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'ggmon@{socket.gethostname()}'
    msg['To'] = recipient

    part = MIMEText(html_body, 'html')
    msg.attach(part)

    try:
        with smtplib.SMTP('localhost') as server:
            server.sendmail(msg['From'], [recipient], msg.as_string())
            print(f"[EMAIL] Alert sent to {recipient}")
    except Exception as e:
        print(f"[ERROR] Failed to send email to {recipient}: {e}")

def monitor():
    configs = read_config()
    hostname = socket.gethostname()
    email_alerts = defaultdict(list)

    for entry in configs:
        gg_home = entry['gg_home']
        db_name = entry['db_name']
        email = entry['email']

        print(f"[INFO] Checking {db_name} at {gg_home}...")

        info_output = run_ggsci_command(gg_home, 'info all')
        raw_mgr_output = run_ggsci_command(gg_home, 'info manager')
        mgr_output, mgr_is_down = extract_manager_status(raw_mgr_output)
        alerts = parse_info_all(info_output)

        if alerts or mgr_is_down:
            db_section = {
                'db_name': db_name,
                'manager_status': mgr_output,
                'mgr_is_down': mgr_is_down,
                'alerts': alerts
            }
            email_alerts[email].append(db_section)
        else:
            print(f"[OK] {db_name}: All processes healthy.")

    for email, db_sections in email_alerts.items():
        html_report = generate_consolidated_report(db_sections)
        subject = f"[ALERT] GoldenGate Issues Detected on Host {hostname}"
        send_email(subject, html_report, email)

if __name__ == "__main__":
    monitor()
