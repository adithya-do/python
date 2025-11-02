#!/usr/bin/env bash
# gg_multi_home_monitor.sh  (v2)
# Config line format: GG_HOME|ORACLE_HOME|DB_NAME|ALERT_EMAIL|PAGE_EMAIL
# Works with GoldenGate Classic Architecture; multiple GG homes on one server.

set -euo pipefail

CONFIG_FILE="${1:-/opt/oracle/scripts/ogg_mon/ogg.conf}"

# Thresholds (seconds)
WARN_SECS=600      # 10 min
CRIT_SECS=1200     # 20 min

HOSTNAME_SHORT="$(hostname -s 2>/dev/null || hostname || echo "unknown-host")"
DATE_ISO="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [[ ! -r "$CONFIG_FILE" ]]; then
  echo "ERROR: Config file not readable: $CONFIG_FILE" >&2
  exit 2
fi

# --- Helpers -----------------------------------------------------------------

to_seconds() {
  # HH:MM:SS -> seconds; -1 if not a time
  local t="${1:-N/A}"
  if [[ "$t" =~ ^([0-9]{2}):([0-9]{2}):([0-9]{2})$ ]]; then
    echo $((10#${BASH_REMATCH[1]}*3600 + 10#${BASH_REMATCH[2]}*60 + 10#${BASH_REMATCH[3]}))
  else
    echo -1
  fi
}

html_escape() {
  sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
}

send_html() {
  # Use `mail -s` for HTML per requirement; fall back to mailx if needed.
  local subject="$1" to_addr="$2" html_body="$3"
  if command -v mail >/dev/null 2>&1; then
    printf "%s" "$html_body" | mail -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr"
  else
    printf "%s" "$html_body" | mailx -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr"
  fi
}

send_text_page() {
  # Use `mailx -s` for the concise page
  local subject="$1" to_addr="$2" text_body="$3"
  if command -v mailx >/dev/null 2>&1; then
    printf "%s" "$text_body" | mailx -s "$subject" "$to_addr"
  else
    printf "%s" "$text_body" | mail -s "$subject" "$to_addr"
  fi
}

make_html() {
  # 1 DB_NAME, 2 Host, 3 Severity, 4 Summary, 5 <tr> rows, 6 preformatted info all
  cat <<HTML
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"/><title>GoldenGate $3 - $1@$2</title></head>
<body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.4;">
  <h2 style="margin:0 0 8px 0;">GoldenGate $3 alert</h2>
  <div><strong>DB:</strong> $1 &nbsp; <strong>Host:</strong> $2 &nbsp; <strong>When (UTC):</strong> $DATE_ISO</div>
  <p style="margin-top:8px;">$4</p>
  <h3 style="margin:16px 0 6px 0;">Problem processes</h3>
  <table cellspacing="0" cellpadding="6" border="1" style="border-collapse:collapse;">
    <thead><tr style="background:#f5f5f5;">
      <th align="left">Program</th><th align="left">Group</th><th align="left">Status</th>
      <th align="left">Lag at Chkpt</th><th align="left">Time Since Chkpt</th><th align="left">Reason</th>
    </tr></thead>
    <tbody>
      $5
    </tbody>
  </table>
  <h3 style="margin:16px 0 6px 0;">GGSCI&gt; info all</h3>
  <pre style="background:#111;color:#eee;padding:10px;border-radius:6px;overflow:auto;">GGSCI&gt; info all

$6</pre>
  <div style="color:#777;margin-top:10px;">This message was generated automatically on $2.</div>
</body>
</html>
HTML
}

# --- Main loop ---------------------------------------------------------------

# Read non-empty, non-comment lines safely
while IFS= read -r line; do
  # trim
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  [[ -z "$line" || "${line:0:1}" == "#" ]] && continue

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

  # Safely capture GGSCI output (avoid the fragile `|| true` after command substitution)
  GGSCI_OUT=""
  if ! GGSCI_OUT="$("$GGSCI_BIN" <<EOF
info all
exit
EOF
)"; then
    echo "ERROR: GGSCI failed for DB '$DB_NAME' in $GG_HOME" >&2
    continue
  fi

  GGSCI_PRE="$(printf "%s\n" "$GGSCI_OUT" | html_escape)"

  # Parse lines of interest into pipe-delimited rows: PROGRAM|STATUS|GROUP|LAG|SINCE
  parsed="$(printf "%s\n" "$GGSCI_OUT" | awk '
    function is_time(s) { return (s ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/) }
    {
      U1 = toupper($1); U2 = toupper($2)
      if (U1=="MANAGER") {
        print "MANAGER|" $2 "|-|-|-"
      } else if (U1=="EXTRACT" || U1=="REPLICAT") {
        program=$1; status=$2; grp=$3; lag="N/A"; since="N/A"
        for (i=1; i<=NF; i++) {
          if (is_time($i)) { lag=$i; if ((i+1)<=NF && is_time($(i+1))) since=$(i+1); break }
        }
        print program "|" status "|" grp "|" lag "|" since
      }
    }
  ')"

  manager_status="UNKNOWN"
  problem_rows_html=""
  have_warning=0
  have_critical=0
  summary_bits=()

  # Manager status
  while IFS='|' read -r prog status grp lag since; do
    [[ -z "${prog:-}" ]] && continue
    if [[ "$prog" == "MANAGER" ]]; then
      manager_status="$status"
    fi
  done <<< "$parsed"

  # If manager down => only manager critical
  if [[ "$manager_status" != "RUNNING" ]]; then
    have_critical=1
    summary_bits+=("MANAGER is $manager_status")
    problem_rows_html+="<tr><td>MANAGER</td><td>-</td><td>$manager_status</td><td>-</td><td>-</td><td>Manager down</td></tr>"
  else
    # Evaluate Extract/Replicat rows
    while IFS='|' read -r prog status grp lag since; do
      [[ -z "${prog:-}" ]] && continue
      [[ "$prog" == "MANAGER" ]] && continue

      if [[ "$status" != "RUNNING" ]]; then
        have_critical=1
        problem_rows_html+="<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>Process $status</td></tr>"
        summary_bits+=("$prog $grp $status")
        continue
      fi

      secs=$(to_seconds "$lag")
      if (( secs >= 0 )); then
        if (( secs >= CRIT_SECS )); then
          have_critical=1
          problem_rows_html+="<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>Lag ≥ 20m</td></tr>"
          summary_bits+=("$prog $grp lag $lag")
        elif (( secs >= WARN_SECS )); then
          have_warning=1
          problem_rows_html+="<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>Lag ≥ 10m</td></tr>"
          summary_bits+=("$prog $grp lag $lag")
        fi
      fi
    done <<< "$parsed"
  fi

  # No issues? quiet
  if (( have_critical == 0 && have_warning == 0 )); then
    continue
  fi

  if (( have_critical )); then
    SUBJECT="GG CRITICAL [$HOSTNAME_SHORT] [$DB_NAME]"
    ONE_LINER="CRITICAL: $HOSTNAME_SHORT / $DB_NAME -> ${summary_bits[*]}"
    HTML_BODY="$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "CRITICAL" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")"
    send_html "$SUBJECT" "$ALERT_EMAIL" "$HTML_BODY"
    send_text_page "$SUBJECT" "$PAGE_EMAIL" "$ONE_LINER"
  else
    SUBJECT="GG WARNING [$HOSTNAME_SHORT] [$DB_NAME]"
    ONE_LINER="WARNING: $HOSTNAME_SHORT / $DB_NAME -> ${summary_bits[*]}"
    HTML_BODY="$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "WARNING" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")"
    send_html "$SUBJECT" "$ALERT_EMAIL" "$HTML_BODY"
  fi

done < <(grep -v '^[[:space:]]*#' "$CONFIG_FILE" | sed '/^[[:space:]]*$/d')
