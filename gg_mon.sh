#!/bin/sh
# GoldenGate Classic multi-home monitor (POSIX sh)  v5 (text-only emails via mailx)
# Config line: GG_HOME|ORACLE_HOME|DB_NAME|ALERT_EMAIL|PAGE_EMAIL

# Usage:
#   sh -n gg_multi_home_monitor_v5.sh
#   chmod +x gg_multi_home_monitor_v5.sh
#   ./gg_multi_home_monitor_v5.sh /opt/oracle/scripts/ogg_mon/ogg.conf

CONFIG_FILE="${1:-/opt/oracle/scripts/ogg_mon/ogg.conf}"

# Thresholds (seconds)
WARN_SECS=600      # 10 minutes
CRIT_SECS=1200     # 20 minutes

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

send_text() {
  # read body from stdin; prefer mailx as requested
  subject="$1"; to_addr="$2"
  if command -v mailx >/dev/null 2>&1; then
    mailx -s "$subject" "$to_addr"
  else
    mail -s "$subject" "$to_addr"
  fi
}

# --- main loop over config ---------------------------------------------------
while IFS= read -r rawline || [ -n "$rawline" ]; do
  # trim, skip comments/blank
  line=$(printf "%s" "$rawline" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  [ -z "$line" ] && continue
  case "$line" in \#*) continue ;; esac

  # parse fields split by |
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

  # per-entry env
  ORIG_PATH=$PATH
  export ORACLE_HOME="$ORACLE_HOME"
  export PATH="$ORACLE_HOME/bin:$GG_HOME:$PATH"
  export LD_LIBRARY_PATH="${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"
  : "${TNS_ADMIN:=$ORACLE_HOME/network/admin}"
  export TNS_ADMIN

  # Run GGSCI (POSIX-safe; no nested here-docs)
  GGSCI_OUT=$(
    { printf 'info all\n'; printf 'exit\n'; } | "$GGSCI_BIN" 2>&1
  )

  # Analyze in awk; emit flags, summary, and a preformatted text table for problems
  analysis=$(
    printf "%s\n" "$GGSCI_OUT" | awk -v WARN="$WARN_SECS" -v CRIT="$CRIT_SECS" '
      function istime(s){ return (s ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/) }
      function sec(t,  a){ if(!istime(t)) return -1; n=split(t,a,":"); return a[1]*3600 + a[2]*60 + a[3] }
      {
        if ($1=="MANAGER") manager=$2
        if ($1=="EXTRACT" || $1=="REPLICAT") {
          program=$1; status=$2; grp=$3; lag="N/A"; since="N/A"
          for (i=1;i<=NF;i++) if (istime($i)) { lag=$i; if (i+1<=NF && istime($(i+1))) since=$(i+1); break }
          n++; P[n]=program; S[n]=status; G[n]=grp; L[n]=lag; T[n]=since
        }
      }
      END{
        # If manager down => only manager row matters
        if (manager=="" ) manager="UNKNOWN"
        if (manager!="RUNNING") {
          crit=1; warn=0;
          n=1; P[1]="MANAGER"; S[1]=manager; G[1]="-"; L[1]="-"; T[1]="-"; R[1]="Manager down"
          rows_only_manager=1
        } else {
          for (i=1;i<=n;i++){
            if (P[i]=="MANAGER") continue
            if (S[i]!="RUNNING") { R[i]="Process " S[i]; crit=1; sum=sum P[i]" "G[i]" "S[i]"; sep="; "; continue }
            s=sec(L[i])
            if (s>=0) {
              if (s>=CRIT) { R[i]="Lag >= 20m"; crit=1; sum=sum sep P[i]" "G[i]" lag "L[i]"; sep="; " }
              else if (s>=WARN) { R[i]="Lag >= 10m"; warn=1; sum=sum sep P[i]" "G[i]" lag "L[i]"; sep="; " }
            }
          }
        }
        if (rows_only_manager!=1) {
          # remove non-problem rows
          m=0
          for (i=1;i<=n;i++){
            if (R[i]!="") { m++; P2[m]=P[i]; S2[m]=S[i]; G2[m]=G[i]; L2[m]=L[i]; T2[m]=T[i]; R2[m]=R[i]; }
          }
          n=m; delete P; delete S; delete G; delete L; delete T; delete R
          for (i=1;i<=n;i++){ P[i]=P2[i]; S[i]=S2[i]; G[i]=G2[i]; L[i]=L2[i]; T[i]=T2[i]; R[i]=R2[i] }
        }

        if (sum=="") sum=(manager!="RUNNING" ? "MANAGER is " manager : "")

        print "MANAGER=" manager
        print "CRIT=" (crit?1:0)
        print "WARN=" (warn?1:0)
        print "SUMMARY=" sum

        # Output a preformatted text table for problems
        print "ROWS_TXT<<__EOR__"
        if (n>0) {
          h1="Program"; h2="Group"; h3="Status"; h4="Lag"; h5="Since"; h6="Reason"
          printf "%-9s %-15s %-10s %-10s %-10s %s\n", h1,h2,h3,h4,h5,h6
          printf "%-9s %-15s %-10s %-10s %-10s %s\n", "---------","---------------","----------","----------","----------","------"
          for (i=1;i<=n;i++){
            printf "%-9s %-15s %-10s %-10s %-10s %s\n", P[i],G[i],S[i],L[i],T[i],R[i]
          }
        } else {
          print "(no problematic processes)"
        }
        print "__EOR__"
      }'
  )

  manager_status=$(printf "%s\n" "$analysis" | awk -F= '/^MANAGER=/{sub(/^MANAGER=/,"");print}')
  have_critical=$(printf "%s\n" "$analysis" | awk -F= '/^CRIT=/{print $2}')
  have_warning=$(printf "%s\n" "$analysis" | awk -F= '/^WARN=/{print $2}')
  summary_bits=$(printf "%s\n" "$analysis" | awk -F= '/^SUMMARY=/{sub(/^SUMMARY=/,"");print}')
  problem_rows_txt=$(printf "%s\n" "$analysis" | awk '/^ROWS_TXT<<__EOR__/{p=1;next} /^__EOR__/{p=0} p')

  # If no problems, be quiet
  if [ "$have_critical" -eq 0 ] && [ "$have_warning" -eq 0 ]; then
    PATH="$ORIG_PATH"
    continue
  fi

  if [ "$have_critical" -ne 0 ]; then
    SUBJECT="GG CRITICAL [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    ONE_LINER="CRITICAL: $HOSTNAME_SHORT / $DB_NAME -> $summary_bits"
    # Full alert (plain text) to ALERT_EMAIL
    {
      printf "GoldenGate ALERT: CRITICAL\n"
      printf "DB: %s | Host: %s | When (UTC): %s\n" "$DB_NAME" "$HOSTNAME_SHORT" "$DATE_ISO"
      printf "Summary: %s\n\n" "$summary_bits"
      printf "Problem processes:\n%s\n\n" "$problem_rows_txt"
      printf "----- GGSCI> info all -----\n%s\n" "$GGSCI_OUT"
    } | send_text "$SUBJECT" "$ALERT_EMAIL"
    # Precise page (one-liner) to PAGE_EMAIL
    printf "%s\n" "$ONE_LINER" | send_text "$SUBJECT" "$PAGE_EMAIL"
  else
    SUBJECT="GG WARNING [$HOSTNAME_SHORT] [$DB_NAME]"
    [ -n "$summary_bits" ] || summary_bits="See details"
    # Warning to ALERT_EMAIL (no page)
    {
      printf "GoldenGate ALERT: WARNING\n"
      printf "DB: %s | Host: %s | When (UTC): %s\n" "$DB_NAME" "$HOSTNAME_SHORT" "$DATE_ISO"
      printf "Summary: %s\n\n" "$summary_bits"
      printf "Problem processes:\n%s\n\n" "$problem_rows_txt"
      printf "----- GGSCI> info all -----\n%s\n" "$GGSCI_OUT"
    } | send_text "$SUBJECT" "$ALERT_EMAIL"
  fi

  PATH="$ORIG_PATH"

done < "$CONFIG_FILE"
