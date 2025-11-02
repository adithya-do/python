#!/usr/bin/env bash
#
# GoldenGate Classic multi-home monitor
# Reads config lines: GG_HOME|ORACLE_HOME|DB_NAME|ALERT_EMAIL|PAGE_EMAIL
# Sends HTML alert (mail -s) and concise text page (mailx -s) based on:
#   - Warning lag >= 10 minutes
#   - Critical lag >= 20 minutes
#   - STOPPED/ABENDED => Critical (no separate lag alert)
#   - MANAGER down => Critical and suppress other alerts
#
# Usage:
#   chmod +x gg_multi_home_monitor.sh
#   ./gg_multi_home_monitor.sh /path/to/ogg.conf
#
# Exit codes: 0 OK, nonzero on errors

set -euo pipefail

CONFIG_FILE="${1:-/opt/oracle/scripts/ogg_mon/ogg.conf}"

# Thresholds (seconds)
WARN_SECS=600
CRIT_SECS=1200

HOSTNAME_SHORT="$(hostname -s 2>/dev/null || hostname || echo "unknown-host")"
DATE_ISO="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [[ ! -r "$CONFIG_FILE" ]]; then
  echo "ERROR: Config file not readable: $CONFIG_FILE" >&2
  exit 2
fi

# --- Helpers -----------------------------------------------------------------

to_seconds() {
  # HH:MM:SS -> seconds; returns -1 for N/A
  local t="${1:-N/A}"
  if [[ "$t" =~ ^([0-9]{2}):([0-9]{2}):([0-9]{2})$ ]]; then
    echo $((10#${BASH_REMATCH[1]}*3600 + 10#${BASH_REMATCH[2]}*60 + 10#${BASH_REMATCH[3]}))
  else
    echo -1
  fi
}

html_escape() {
  # Minimal HTML escape for <pre> section
  sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
}

have_mail_a_header() {
  # Check whether `mail` supports -a for custom header (most do)
  (echo test | mail -s "x" -a "Content-Type: text/plain" "$USER" >/dev/null 2>&1) || return 1
  return 0
}

send_html() {
  local subject="$1" to_addr="$2" html_body="$3"
  if command -v mail >/dev/null 2>&1 && have_mail_a_header; then
    # Use `mail` for HTML (per requirement)
    printf "%s" "$html_body" | mail -s "$subject" -a "Content-Type: text/html" "$to_addr"
  else
    # Fallback to mailx with header
    printf "%s" "$html_body" | mailx -s "$subject" -a "Content-Type: text/html" "$to_addr"
  fi
}

send_text_page() {
  local subject="$1" to_addr="$2" text_body="$3"
  if command -v mailx >/dev/null 2>&1; then
    printf "%s" "$text_body" | mailx -s "$subject" "$to_addr"
  else
    # Fallback: plain mail
    printf "%s" "$text_body" | mail -s "$subject" "$to_addr"
  fi
}

make_html() {
  # Arguments:
  # 1 DB_NAME
  # 2 Hostname
  # 3 Severity (WARNING/CRITICAL)
  # 4 Summary (one-liner)
  # 5 Problem table rows (already <tr>...</tr>)
  # 6 Preformatted GGSCI output (escaped)
  local DB="$1" HOST="$2" SEV="$3" SUMMARY="$4" ROWS="$5" PRE="$6"

  cat <<HTML
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>GoldenGate Alert - $DB - $HOST - $SEV</title>
</head>
<body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.4;">
  <h2 style="margin:0 0 8px 0;">GoldenGate $SEV alert</h2>
  <div><strong>DB:</strong> $DB &nbsp; <strong>Host:</strong> $HOST &nbsp; <strong>When (UTC):</strong> $DATE_ISO</div>
  <p style="margin-top:8px;">$SUMMARY</p>

  <h3 style="margin:16px 0 6px 0;">Problem processes</h3>
  <table cellspacing="0" cellpadding="6" border="1" style="border-collapse:collapse;">
    <thead>
      <tr style="background:#f5f5f5;">
        <th align="left">Program</th>
        <th align="left">Group</th>
        <th align="left">Status</th>
        <th align="left">Lag at Chkpt</th>
        <th align="left">Time Since Chkpt</th>
        <th align="left">Reason</th>
      </tr>
    </thead>
    <tbody>
      $ROWS
    </tbody>
  </table>

  <h3 style="margin:16px 0 6px 0;">GGSCI&gt; info all</h3>
  <pre style="background:#111;color:#eee;padding:10px;border-radius:6px;overflow:auto;">GGSCI&gt; info all

$PRE</pre>

  <div style="color:#777;margin-top:10px;">This message was generated automatically on $HOST.</div>
</body>
</html>
HTML
}

# --- Main loop ---------------------------------------------------------------

while IFS= read -r rawline || [[ -n "$rawline" ]]; do
  # Trim spaces
  line="$(echo "$rawline" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^# ]] && continue

  IFS='|' read -r GG_HOME ORACLE_HOME DB_NAME ALERT_EMAIL PAGE_EMAIL <<<"$line" || true

  if [[ -z "${GG_HOME:-}" || -z "${ORACLE_HOME:-}" || -z "${DB_NAME:-}" || -z "${ALERT_EMAIL:-}" || -z "${PAGE_EMAIL:-}" ]]; then
    echo "WARN: Skipping malformed line: $line" >&2
    continue
  fi

  GGSCI_BIN="$GG_HOME/ggsci"
  if [[ ! -x "$GGSCI_BIN" ]]; then
    echo "ERROR: GGSCI not executable for DB '$DB_NAME': $GGSCI_BIN" >&2
    continue
  fi

  # Export env for this entry
  export ORACLE_HOME
  export PATH="$ORACLE_HOME/bin:$GG_HOME:$PATH"
  export LD_LIBRARY_PATH="${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"
  export TNS_ADMIN="${TNS_ADMIN:-$ORACLE_HOME/network/admin}"

  # Run 'info all'
  GGSCI_OUT="$("$GGSCI_BIN" <<'EOF'
info all
exit
EOF
  )" || true

  # Keep a sanitized version for embedding
  GGSCI_PRE="$(printf "%s\n" "$GGSCI_OUT" | html_escape)"

  # Parse lines of interest into pipe-delimited rows
  # Format: PROGRAM|STATUS|GROUP|LAG|SINCE
  parsed="$(printf "%s\n" "$GGSCI_OUT" | awk '
    function is_time(s) { return (s ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/) }
    $1=="MANAGER" {
      print $1 "|" $2 "|-|-|-"
    }
    ($1=="EXTRACT" || $1=="REPLICAT") {
      program=$1; status=$2; grp=$3; lag="N/A"; since="N/A"
      for(i=1;i<=NF;i++){
        if(is_time($i)){ lag=$i; if(i+1<=NF && is_time($(i+1))) since=$(i+1); break }
      }
      print program "|" status "|" grp "|" lag "|" since
    }
  ')"

  manager_status="UNKNOWN"
  problem_rows_html=""
  have_warning=0
  have_critical=0
  summary_bits=()

  # First pass: detect manager status
  while IFS='|' read -r prog status grp lag since || [[ -n "${prog:-}" ]]; do
    [[ -z "${prog:-}" ]] && continue
    if [[ "$prog" == "MANAGER" ]]; then
      manager_status="$status"
    fi
  done <<< "$parsed"

  # If manager is down, generate only manager critical alert
  if [[ "$manager_status" != "RUNNING" ]]; then
    have_critical=1
    summary_bits+=("MANAGER is $manager_status")
    problem_rows_html+="<tr><td>MANAGER</td><td>-</td><td>$manager_status</td><td>-</td><td>-</td><td>Manager down</td></tr>"
  else
    # Evaluate Extract/Replicat
    while IFS='|' read -r prog status grp lag since || [[ -n "${prog:-}" ]]; do
      [[ -z "${prog:-}" ]] && continue
      if [[ "$prog" == "MANAGER" ]]; then
        continue
      fi

      reason=""
      if [[ "$status" != "RUNNING" ]]; then
        have_critical=1
        reason="Process $status"
        summary_bits+=("$prog $grp $status")
        problem_rows_html+="<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>$reason</td></tr>"
        continue
      fi

      secs=$(to_seconds "$lag")
      if (( secs >= 0 )); then
        if (( secs >= CRIT_SECS )); then
          have_critical=1
          reason="Lag ≥ $(printf "%02d:%02d:%02d" $((CRIT_SECS/3600)) $(((CRIT_SECS%3600)/60)) $((CRIT_SECS%60)))"
          summary_bits+=("$prog $grp lag $lag")
          problem_rows_html+="<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>$reason</td></tr>"
        elif (( secs >= WARN_SECS )); then
          have_warning=1
          reason="Lag ≥ $(printf "%02d:%02d:%02d" $((WARN_SECS/3600)) $(((WARN_SECS%3600)/60)) $((WARN_SECS%60)))"
          summary_bits+=("$prog $grp lag $lag")
          problem_rows_html+="<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>$reason</td></tr>"
        fi
      fi
    done <<< "$parsed"
  fi

  # If no problems, continue
  if (( have_critical == 0 && have_warning == 0 )); then
    # Quiet if fully healthy
    continue
  fi

  # Compose subjects/bodies
  if (( have_critical == 1 )); then
    SUBJECT="GG CRITICAL [$HOSTNAME_SHORT] [$DB_NAME]"
    ONE_LINER="CRITICAL: $HOSTNAME_SHORT / $DB_NAME -> ${summary_bits[*]}"
    HTML_BODY="$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "CRITICAL" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")"
    # Send HTML alert to ALERT_EMAIL
    send_html "$SUBJECT" "$ALERT_EMAIL" "$HTML_BODY"
    # Send concise text page to PAGE_EMAIL
    send_text_page "$SUBJECT" "$PAGE_EMAIL" "$ONE_LINER"
  else
    SUBJECT="GG WARNING [$HOSTNAME_SHORT] [$DB_NAME]"
    ONE_LINER="WARNING: $HOSTNAME_SHORT / $DB_NAME -> ${summary_bits[*]}"
    HTML_BODY="$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "WARNING" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")"
    # Warning goes only to ALERT_EMAIL (no page)
    send_html "$SUBJECT" "$ALERT_EMAIL" "$HTML_BODY"
  fi

done < "$CONFIG_FILE"
