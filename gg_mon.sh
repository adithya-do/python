#!/bin/sh
# GoldenGate Classic multi-home monitor (POSIX sh)
# Config line: GG_HOME|ORACLE_HOME|DB_NAME|ALERT_EMAIL|PAGE_EMAIL

set -e

CONFIG_FILE="${1:-/opt/oracle/scripts/ogg_mon/ogg.conf}"

# Thresholds (seconds)
WARN_SECS=600     # 10 min
CRIT_SECS=1200    # 20 min

HOSTNAME_SHORT="$( (hostname -s 2>/dev/null || hostname) 2>/dev/null || echo unknown-host )"
DATE_ISO="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [ ! -r "$CONFIG_FILE" ]; then
  echo "ERROR: Config file not readable: $CONFIG_FILE" >&2
  exit 2
fi

to_seconds() {
  # HH:MM:SS -> seconds ; prints -1 if not a time
  t="$1"
  case "$t" in
    [0-9][0-9]:[0-9][0-9]:[0-9][0-9])
      h=${t%%:*}
      rest=${t#*:}
      m=${rest%%:*}
      s=${rest#*:}
      awk -v h="$h" -v m="$m" -v s="$s" 'BEGIN{printf("%d", h*3600 + m*60 + s)}'
      ;;
    *)
      printf -- "-1"
      ;;
  esac
}

html_escape() {
  # escape minimal for <pre>
  sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
}

send_html() {
  subject="$1"; to_addr="$2"; html_body="$3"
  if command -v mail >/dev/null 2>&1; then
    printf "%s" "$html_body" | mail -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr" 2>/dev/null || \
    printf "%s" "$html_body" | mailx -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr"
  else
    printf "%s" "$html_body" | mailx -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr"
  fi
}

send_text_page() {
  subject="$1"; to_addr="$2"; text_body="$3"
  if command -v mailx >/dev/null 2>&1; then
    printf "%s" "$text_body" | mailx -s "$subject" "$to_addr"
  else
    printf "%s" "$text_body" | mail -s "$subject" "$to_addr"
  fi
}

make_html() {
  # $1 DB  $2 HOST  $3 SEV  $4 SUMMARY  $5 ROWS  $6 PRE
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

# --- Main loop over config ---------------------------------------------------
# Read line-by-line; skip blanks/comments
while IFS= read -r rawline || [ -n "$rawline" ]; do
  line=$(printf "%s" "$rawline" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  [ -z "$line" ] && continue
  case "$line" in \#*) continue ;; esac

  IFS='|' read -r GG_HOME ORACLE_HOME DB_NAME ALERT_EMAIL PAGE_EMAIL <<EOF
$line
EOF

  if [ -z "$GG_HOME" ] || [ -z "$ORACLE_HOME" ] || [ -z "$DB_NAME" ] || [ -z "$ALERT_EMAIL" ] || [ -z "$PAGE_EMAIL" ]; then
    echo "WARN: Skipping malformed line: $line" >&2
    continue
  fi

  GGSCI_BIN="$GG_HOME/ggsci"
  if [ ! -x "$GGSCI_BIN" ]; then
    echo "ERROR: GGSCI not executable for DB '$DB_NAME': $GGSCI_BIN" >&2
    continue
  fi

  # Export per-entry env
  ORIG_PATH="$PATH"
  export ORACLE_HOME="$ORACLE_HOME"
  export PATH="$ORACLE_HOME/bin:$GG_HOME:$PATH"
  export LD_LIBRARY_PATH="${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"
  : "${TNS_ADMIN:=$ORACLE_HOME/network/admin}"
  export TNS_ADMIN

  # Capture GGSCI output (no bash-isms)
  if ! GGSCI_OUT="$("$GGSCI_BIN" <<EOF
info all
exit
EOF
)"; then
    echo "ERROR: GGSCI failed for DB '$DB_NAME' in $GG_HOME" >&2
    PATH="$ORIG_PATH"
    continue
  fi

  GGSCI_PRE=$(printf "%s\n" "$GGSCI_OUT" | html_escape)

  # Parse: PROGRAM|STATUS|GROUP|LAG|SINCE
  parsed=$(printf "%s\n" "$GGSCI_OUT" | awk '
    function timere(s){ return (s ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/) }
    $1=="MANAGER" {
      print "MANAGER|" $2 "|-|-|-"
    }
    ($1=="EXTRACT" || $1=="REPLICAT") {
      program=$1; status=$2; grp=$3; lag="N/A"; since="N/A"
      for (i=1;i<=NF;i++){
        if (timere($i)) { lag=$i; if (i+1<=NF && timere($(i+1))) since=$(i+1); break }
      }
      print program "|" status "|" grp "|" lag "|" since
    }
  ')

  manager_status="UNKNOWN"
  problem_rows_html=""
  have_warning=0
  have_critical=0
  summary_bits=""

  # Manager status
  printf "%s\n" "$parsed" | while IFS='|' read -r prog status grp lag since; do
    [ -n "$prog" ] || continue
    if [ "$prog" = "MANAGER" ]; then
      manager_status="$status"
    fi
  done

  # We need manager_status outside the subshell; recompute in POSIX-safe way
  manager_status=$(printf "%s\n" "$parsed" | awk -F'|' '$1=="MANAGER"{print $2; found=1} END{ if(!found) print "UNKNOWN" }')

  if [ "$manager_status" != "RUNNING" ]; then
    have_critical=1
    summary_bits="MANAGER is $manager_status"
    problem_rows_html="$problem_rows_html<tr><td>MANAGER</td><td>-</td><td>$manager_status</td><td>-</td><td>-</td><td>Manager down</td></tr>"
  else
    # Check Extract/Replicat
    printf "%s\n" "$parsed" | while IFS='|' read -r prog status grp lag since; do
      [ -n "$prog" ] || continue
      [ "$prog" = "MANAGER" ] && continue

      if [ "$status" != "RUNNING" ]; then
        have_critical=1
        summary_bits="${summary_bits:+$summary_bits; }$prog $grp $status"
        problem_rows_html="$problem_rows_html<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>Process $status</td></tr>"
        continue
      fi

      secs=$(to_seconds "$lag")
      if [ "$secs" -ge 0 ] 2>/dev/null; then
        if [ "$secs" -ge "$CRIT_SECS" ]; then
          have_critical=1
          summary_bits="${summary_bits:+$summary_bits; }$prog $grp lag $lag"
          problem_rows_html="$problem_rows_html<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>Lag ≥ 20m</td></tr>"
        elif [ "$secs" -ge "$WARN_SECS" ]; then
          have_warning=1
          summary_bits="${summary_bits:+$summary_bits; }$prog $grp lag $lag"
          problem_rows_html="$problem_rows_html<tr><td>$prog</td><td>$grp</td><td>$status</td><td>$lag</td><td>$since</td><td>Lag ≥ 10m</td></tr>"
        fi
      fi
    done

    # Pull flags back from the sub-shell using markers
    # (Simple approach: recompute from problem_rows_html)
    case "$problem_rows_html" in
      *"Lag ≥ 20m"*|*"Process "* ) have_critical=1 ;;
    esac
    case "$problem_rows_html" in
      *"Lag ≥ 10m"* ) have_warning=1 ;;
    esac
  fi

  # No issues? silence
  if [ "${have_critical:-0}" -eq 0 ] && [ "${have_warning:-0}" -eq 0 ]; then
    PATH="$ORIG_PATH"
    continue
  fi

  if [ "${have_critical:-0}" -ne 0 ]; then
    SUBJECT="GG CRITICAL [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    ONE_LINER="CRITICAL: $HOSTNAME_SHORT / $DB_NAME -> $summary_bits"
    HTML_BODY="$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "CRITICAL" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")"
    send_html "$SUBJECT" "$ALERT_EMAIL" "$HTML_BODY"
    send_text_page "$SUBJECT" "$PAGE_EMAIL" "$ONE_LINER"
  else
    SUBJECT="GG WARNING [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    ONE_LINER="WARNING: $HOSTNAME_SHORT / $DB_NAME -> $summary_bits"
    HTML_BODY="$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "WARNING" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")"
    send_html "$SUBJECT" "$ALERT_EMAIL" "$HTML_BODY"
  fi

  # restore PATH for next entry
  PATH="$ORIG_PATH"

done < "$CONFIG_FILE"
