#!/bin/sh
# GoldenGate Classic multi-home monitor (POSIX sh)  v4

# Config line format:
#   GG_HOME|ORACLE_HOME|DB_NAME|ALERT_EMAIL|PAGE_EMAIL
# Usage:
#   sh -n gg_multi_home_monitor_v4.sh     # syntax check
#   chmod +x gg_multi_home_monitor_v4.sh
#   ./gg_multi_home_monitor_v4.sh /opt/oracle/scripts/ogg_mon/ogg.conf

# ---- settings ---------------------------------------------------------------
CONFIG_FILE="${1:-/opt/oracle/scripts/ogg_mon/ogg.conf}"

WARN_SECS=600      # 10 minutes
CRIT_SECS=1200     # 20 minutes

# ---- utils ------------------------------------------------------------------
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
      h=${t%%:*}; rest=${t#*:}; m=${rest%%:*}; s=${rest#*:}
      awk -v h="$h" -v m="$m" -v s="$s" 'BEGIN{printf("%d", h*3600 + m*60 + s)}'
      ;;
    *) printf -- "-1" ;;
  esac
}

html_escape() {
  sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
}

send_html() {
  subject="$1"; to_addr="$2"
  # read HTML from stdin; prefer "mail -s -a Content-Type"
  if command -v mail >/dev/null 2>&1; then
    mail -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr"
  else
    # fallback to mailx
    mailx -s "$subject" -a "Content-Type: text/html; charset=UTF-8" "$to_addr"
  fi
}

send_text_page() {
  subject="$1"; to_addr="$2"
  # read TEXT from stdin; prefer mailx
  if command -v mailx >/dev/null 2>&1; then
    mailx -s "$subject" "$to_addr"
  else
    mail -s "$subject" "$to_addr"
  fi
}

make_html() {
  # $1 DB  $2 HOST  $3 SEV  $4 SUMMARY  $5 ROWS(HTML)  $6 PRE(escaped)
  DB="$1"; HOST="$2"; SEV="$3"; SUMMARY="$4"; ROWS="$5"; PRE="$6"
  printf '%s\n' \
'<!DOCTYPE html>' \
'<html>' \
'<head><meta charset="utf-8"/><title>GoldenGate '"$SEV"' - '"$DB"'@'"$HOST"'</title></head>' \
'<body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.4;">' \
'  <h2 style="margin:0 0 8px 0;">GoldenGate '"$SEV"' alert</h2>' \
'  <div><strong>DB:</strong> '"$DB"' &nbsp; <strong>Host:</strong> '"$HOST"' &nbsp; <strong>When (UTC):</strong> '"$DATE_ISO"'</div>' \
'  <p style="margin-top:8px;">'"$SUMMARY"'</p>' \
'  <h3 style="margin:16px 0 6px 0;">Problem processes</h3>' \
'  <table cellspacing="0" cellpadding="6" border="1" style="border-collapse:collapse;">' \
'    <thead><tr style="background:#f5f5f5;"><th align="left">Program</th><th align="left">Group</th><th align="left">Status</th><th align="left">Lag at Chkpt</th><th align="left">Time Since Chkpt</th><th align="left">Reason</th></tr></thead>' \
'    <tbody>' \
"$ROWS" \
'    </tbody>' \
'  </table>' \
'  <h3 style="margin:16px 0 6px 0;">GGSCI&gt; info all</h3>' \
'  <pre style="background:#111;color:#eee;padding:10px;border-radius:6px;overflow:auto;">GGSCI&gt; info all' \
'' \
"$PRE" \
'</pre>' \
'  <div style="color:#777;margin-top:10px;">This message was generated automatically on '"$HOST"'.</div>' \
'</body>' \
'</html>'
}

# ---- main loop over config ---------------------------------------------------
# Read non-empty, non-comment lines
while IFS= read -r rawline || [ -n "$rawline" ]; do
  # trim
  line=$(printf "%s" "$rawline" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  [ -z "$line" ] && continue
  case "$line" in \#*) continue ;; esac

  # Split by "|" without here-docs or pipes
  oldIFS=$IFS
  IFS='|'
  set -- $line
  IFS=$oldIFS
  GG_HOME=$1
  ORACLE_HOME=$2
  DB_NAME=$3
  ALERT_EMAIL=$4
  PAGE_EMAIL=$5

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
  ORIG_PATH=$PATH
  export ORACLE_HOME="$ORACLE_HOME"
  export PATH="$ORACLE_HOME/bin:$GG_HOME:$PATH"
  export LD_LIBRARY_PATH="${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"
  : "${TNS_ADMIN:=$ORACLE_HOME/network/admin}"
  export TNS_ADMIN

  # Run GGSCI without nested here-docs: pipe commands into GGSCI
  GGSCI_OUT=$(
    { printf 'info all\n'; printf 'exit\n'; } | "$GGSCI_BIN" 2>&1
  )

  GGSCI_PRE=$(printf "%s\n" "$GGSCI_OUT" | html_escape)

  # One-pass analysis in awk: derive manager status, severity, summary, and HTML rows
  analysis=$(
    printf "%s\n" "$GGSCI_OUT" | awk -v WARN="$WARN_SECS" -v CRIT="$CRIT_SECS" '
      function istime(s){ return (s ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/) }
      function sec(t,  a){ if(!istime(t)) return -1; n=split(t,a,":"); return a[1]*3600 + a[2]*60 + a[3] }
      {
        if ($1=="MANAGER") manager=$2
        if ($1=="EXTRACT" || $1=="REPLICAT") {
          program=$1; status=$2; grp=$3; lag="N/A"; since="N/A"
          for (i=1;i<=NF;i++) if (istime($i)) { lag=$i; if (i+1<=NF && istime($(i+1))) since=$(i+1); break }
          if (status!="RUNNING") {
            crit=1
            rows=rows "<tr><td>" program "</td><td>" grp "</td><td>" status "</td><td>" lag "</td><td>" since "</td><td>Process " status "</td></tr>\n"
            summary=summary program " " grp " " status "; "
          } else {
            s=sec(lag)
            if (s>=0) {
              if (s>=CRIT) {
                crit=1
                rows=rows "<tr><td>" program "</td><td>" grp "</td><td>" status "</td><td>" lag "</td><td>" since "</td><td>Lag ≥ 20m</td></tr>\n"
                summary=summary program " " grp " lag " lag "; "
              } else if (s>=WARN) {
                warn=1
                rows=rows "<tr><td>" program "</td><td>" grp "</td><td>" status "</td><td>" lag "</td><td>" since "</td><td>Lag ≥ 10m</td></tr>\n"
                summary=summary program " " grp " lag " lag "; "
              }
            }
          }
        }
      }
      END{
        if (manager=="") manager="UNKNOWN"
        if (manager!="RUNNING") {
          crit=1; warn=0;
          rows="<tr><td>MANAGER</td><td>-</td><td>" manager "</td><td>-</td><td>-</td><td>Manager down</td></tr>\n"
          summary="MANAGER is " manager
        }
        print "MANAGER=" manager
        print "CRIT=" (crit?1:0)
        print "WARN=" (warn?1:0)
        print "SUMMARY=" summary
        print "ROWS<<__EOR__"
        printf "%s", rows
        print "__EOR__"
      }'
  )

  manager_status=$(printf "%s\n" "$analysis" | awk -F= '/^MANAGER=/{sub(/^MANAGER=/,"");print}')
  have_critical=$(printf "%s\n" "$analysis" | awk -F= '/^CRIT=/{print $2}')
  have_warning=$(printf "%s\n" "$analysis" | awk -F= '/^WARN=/{print $2}')
  summary_bits=$(printf "%s\n" "$analysis" | awk -F= '/^SUMMARY=/{sub(/^SUMMARY=/,"");print}')
  problem_rows_html=$(printf "%s\n" "$analysis" | awk '/^ROWS<<__EOR__/{p=1;next} /^__EOR__/{p=0} p')

  # No issues? continue
  if [ "$have_critical" -eq 0 ] && [ "$have_warning" -eq 0 ]; then
    PATH="$ORIG_PATH"
    continue
  fi

  if [ "$have_critical" -ne 0 ]; then
    SUBJECT="GG CRITICAL [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    ONE_LINER="CRITICAL: $HOSTNAME_SHORT / $DB_NAME -> $summary_bits"
    HTML_BODY=$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "CRITICAL" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")
    printf "%s" "$HTML_BODY" | send_html "$SUBJECT" "$ALERT_EMAIL"
    printf "%s" "$ONE_LINER" | send_text_page "$SUBJECT" "$PAGE_EMAIL"
  else
    SUBJECT="GG WARNING [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    ONE_LINER="WARNING: $HOSTNAME_SHORT / $DB_NAME -> $summary_bits"
    HTML_BODY=$(make_html "$DB_NAME" "$HOSTNAME_SHORT" "WARNING" "$ONE_LINER" "$problem_rows_html" "$GGSCI_PRE")
    printf "%s" "$HTML_BODY" | send_html "$SUBJECT" "$ALERT_EMAIL"
  fi

  PATH="$ORIG_PATH"

done < "$CONFIG_FILE"
