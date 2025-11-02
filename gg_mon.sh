#!/bin/sh
# GoldenGate Classic multi-home monitor (POSIX sh)  v7
# Config line: GG_HOME|ORACLE_HOME|DB_NAME|ALERT_EMAIL|PAGE_EMAIL
# Writes "info all" to a file, analyzes that file, and emails plain text via mailx.

CONFIG_FILE="${1:-/opt/oracle/scripts/ogg_mon/ogg.conf}"
GG_OUT_DIR="${GG_OUT_DIR:-/var/tmp/ggmon}"

# Thresholds (seconds)
WARN_SECS=600     # 10 minutes
CRIT_SECS=1200    # 20 minutes

# Mail tool
MAILX_BIN="$(command -v mailx 2>/dev/null || command -v mail 2>/dev/null || echo mailx)"

HOSTNAME_SHORT="$( (hostname -s 2>/dev/null || hostname) 2>/dev/null || echo unknown-host )"
DATE_ISO="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

# Basic checks
if [ ! -r "$CONFIG_FILE" ]; then
  echo "ERROR: Config file not readable: $CONFIG_FILE" >&2
  exit 2
fi

mkdir -p "$GG_OUT_DIR" 2>/dev/null || GG_OUT_DIR="/tmp"

# Helpers ---------------------------------------------------------------------
to_seconds() {
  # HH:MM:SS -> seconds ; prints -1 if not a time
  t="$1"
  case "$t" in
    [0-9][0-9]:[0-9][0-9]:[0-9][0-9])
      h=${t%%:*}; rest=${t#*:}; m=${rest%%:*}; s=${rest#*:}
      awk -v h="$h" -v m="$m" -v s="$s" 'BEGIN{printf("%d", h*3600 + m*60 + s)}'
      ;;
    *)
      printf '%s' '-1'
      ;;
  esac
}

send_text() {
  # body from stdin
  subject="$1"; to_addr="$2"
  "$MAILX_BIN" -s "$subject" "$to_addr"
}

sanitize_infoall() {
  # $1 = OUTFILE ; emit only MANAGER/EXTRACT/REPLICAT lines (no banners/prompts)
  awk '/^(MANAGER|EXTRACT|REPLICAT)[[:space:]]/{print}' "$1"
}

# Main loop -------------------------------------------------------------------
while IFS= read -r rawline || [ -n "$rawline" ]; do
  # trim, skip comments/blank
  line=$(printf "%s" "$rawline" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  [ -z "$line" ] && continue
  case "$line" in \#*) continue ;; esac

  # Parse fields
  oldIFS=$IFS; IFS='|'; set -- $line; IFS=$oldIFS
  GG_HOME=$1; ORACLE_HOME=$2; DB_NAME=$3; ALERT_EMAIL=$4; PAGE_EMAIL=$5

  if [ -z "$GG_HOME" ] || [ -z "$ORACLE_HOME" ] || [ -z "$DB_NAME" ] || [ -z "$ALERT_EMAIL" ] || [ -z "$PAGE_EMAIL" ]; then
    echo "WARN: Skipping malformed line: $line" >&2
    continue
  fi

  GGSCI_BIN="$GG_HOME/ggsci"
  if [ ! -x "$GGSCI_BIN" ]; then
    echo "ERROR: GGSCI not executable for DB '$DB_NAME': $GGSCI_BIN" >&2
    continue
  fi

  # Per-entry env
  ORIG_PATH=$PATH
  export ORACLE_HOME="$ORACLE_HOME"
  export PATH="$ORACLE_HOME/bin:$GG_HOME:$PATH"
  export LD_LIBRARY_PATH="${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"
  : "${TNS_ADMIN:=$ORACLE_HOME/network/admin}"
  export TNS_ADMIN

  # Run GGSCI -> OUTFILE (no nested here-docs)
  TS="$(date +%Y%m%d%H%M%S)"
  OUTFILE="$GG_OUT_DIR/info_all_${DB_NAME}_${TS}.txt"
  ( echo "info all"; echo "exit" ) | "$GGSCI_BIN" > "$OUTFILE" 2>&1

  # Analyze OUTFILE
  manager_status="$(awk '/^MANAGER[[:space:]]/ {print $2; exit}' "$OUTFILE")"
  [ -z "$manager_status" ] && manager_status="UNKNOWN"

  have_critical=0
  have_warning=0
  summary_bits=""
  problem_rows_txt=""

  if [ "$manager_status" != "RUNNING" ]; then
    have_critical=1
    summary_bits="MANAGER is $manager_status"
    problem_rows_txt=$(
      printf "%-9s %-15s %-10s %-10s %-10s %s\n" "Program" "Group" "Status" "Lag" "Since" "Reason"
      printf "%-9s %-15s %-10s %-10s %-10s %s\n" "---------" "---------------" "----------" "----------" "----------" "------"
      printf "%-9s %-15s %-10s %-10s %-10s %s\n" "MANAGER" "-" "$manager_status" "-" "-" "Manager down"
    )
  else
    # Build problem table from EXTRACT/REPLICAT rows
    analysis=$(
      awk -v WARN="$WARN_SECS" -v CRIT="$CRIT_SECS" '
        function istime(s){ return (s ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/) }
        function sec(t,  a){ if(!istime(t)) return -1; n=split(t,a,":"); return a[1]*3600 + a[2]*60 + a[3] }
        BEGIN{
          h1="Program"; h2="Group"; h3="Status"; h4="Lag"; h5="Since"; h6="Reason";
          header=sprintf("%-9s %-15s %-10s %-10s %-10s %s\n", h1,h2,h3,h4,h5,h6);
          underline=sprintf("%-9s %-15s %-10s %-10s %-10s %s\n","---------","---------------","----------","----------","----------","------");
        }
        $1=="EXTRACT" || $1=="REPLICAT" {
          program=$1; status=$2; grp=$3; lag="N/A"; since="N/A"
          c=0
          for(i=1;i<=NF;i++){
            if(istime($i)){ c++; if(c==1) lag=$i; else if(c==2){ since=$i; break } }
          }
          if (status!="RUNNING") {
            crit=1
            rows=rows sprintf("%-9s %-15s %-10s %-10s %-10s %s\n", program, grp, status, lag, since, "Process " status)
            sum=sum sprintf("%s %s %s; ", program, grp, status)
            next
          }
          s=sec(lag)
          if (s>=0) {
            if (s>=CRIT) {
              crit=1
              rows=rows sprintf("%-9s %-15s %-10s %-10s %-10s %s\n", program, grp, status, lag, since, "Lag >= 20m")
              sum=sum sprintf("%s %s lag %s; ", program, grp, lag)
            } else if (s>=WARN) {
              warn=1
              rows=rows sprintf("%-9s %-15s %-10s %-10s %-10s %s\n", program, grp, status, lag, since, "Lag >= 10m")
              sum=sum sprintf("%s %s lag %s; ", program, grp, lag)
            }
          }
        }
        END{
          print "CRIT=" (crit?1:0)
          print "WARN=" (warn?1:0)
          print "SUM=" sum
          print "ROWS_BEGIN"
          if (rows!="") {
            printf "%s%s%s", header, underline, rows
          } else {
            printf "(no problematic processes)\n"
          }
          print "ROWS_END"
        }' "$OUTFILE"
    )

    have_critical=$(printf "%s\n" "$analysis" | awk -F= '/^CRIT=/{print $2}')
    have_warning=$(printf "%s\n" "$analysis" | awk -F= '/^WARN=/{print $2}')
    summary_bits=$(printf "%s\n" "$analysis" | awk -F= '/^SUM=/{sub(/^SUM=/,"");print}')
    problem_rows_txt=$(printf "%s\n" "$analysis" | awk '/^ROWS_BEGIN/{p=1;next} /^ROWS_END/{p=0} p')
  fi

  # Skip if healthy
  if [ "${have_critical:-0}" -eq 0 ] && [ "${have_warning:-0}" -eq 0 ]; then
    PATH="$ORIG_PATH"
    continue
  fi

  # Build a sanitized one-block “GGSCI info all (filtered)” to avoid duplicates/garbage
  INFOALL_CLEAN="$(sanitize_infoall "$OUTFILE")"

  if [ "${have_critical:-0}" -ne 0 ]; then
    SUBJECT="GG CRITICAL [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    ONE_LINER="CRITICAL: $HOSTNAME_SHORT / $DB_NAME -> $summary_bits"
    {
      printf "GoldenGate ALERT: CRITICAL\n"
      printf "DB: %s | Host: %s | When (UTC): %s\n" "$DB_NAME" "$HOSTNAME_SHORT" "$DATE_ISO"
      printf "Summary: %s\n\n" "$summary_bits"
      printf "Problem processes:\n%s\n\n" "$problem_rows_txt"
      printf "----- GGSCI info all (filtered) -----\n%s\n" "$INFOALL_CLEAN"
    } | send_text "$SUBJECT" "$ALERT_EMAIL"
    printf "%s\n" "$ONE_LINER" | send_text "$SUBJECT" "$PAGE_EMAIL"
  else
    SUBJECT="GG WARNING [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    {
      printf "GoldenGate ALERT: WARNING\n"
      printf "DB: %s | Host: %s | When (UTC): %s\n" "$DB_NAME" "$HOSTNAME_SHORT" "$DATE_ISO"
      printf "Summary: %s\n\n" "$summary_bits"
      printf "Problem processes:\n%s\n\n" "$problem_rows_txt"
      printf "----- GGSCI info all (filtered) -----\n%s\n" "$INFOALL_CLEAN"
    } | send_text "$SUBJECT" "$ALERT_EMAIL"
  fi

  PATH="$ORIG_PATH"

done < "$CONFIG_FILE"
