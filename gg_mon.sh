#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Oracle GoldenGate Alert Script (Classic)
# Config format:
#   GGHOME|DB_NAME|WARN_LAG|CRIT_LAG|ALERT_EMAIL|PAGE_EMAIL
# ---------------------------------------------------------------------------

CONFIG_FILE="/opt/oracle/scripts/ogg_mon/ogg_alert.conf"
HOSTNAME=$(hostname)
DATE_STR=$(date '+%Y-%m-%d %H:%M:%S')

# ====== COMMON ENV (SET HERE) ==============================================
COMMON_ORACLE_HOME="/u01/app/oracle/product/19c/dbhome_1"
COMMON_TNS_ADMIN="/u01/app/oracle/network/admin"
COMMON_PATH_EXTRA=""   # e.g. /usr/local/bin
# ===========================================================================

# export common env once
[[ -n "$COMMON_ORACLE_HOME" ]] && export ORACLE_HOME="$COMMON_ORACLE_HOME"
[[ -n "$COMMON_TNS_ADMIN"  ]] && export TNS_ADMIN="$COMMON_TNS_ADMIN"
# [[ -n "$COMMON_ORACLE_HOME" ]] && export LD_LIBRARY_PATH="$COMMON_ORACLE_HOME/lib:$LD_LIBRARY_PATH"

# ------------------ helpers -------------------------------------------------

time_to_sec() {
    local t="$1"
    IFS=: read -r h m s <<< "$t"
    h=${h:-0}; m=${m:-0}; s=${s:-0}
    echo $((10#$h*3600 + 10#$m*60 + 10#$s))
}

send_html_email() {
    # mailx-only HTML
    # $1=to $2=subject $3=html body
    local _to="$1" _sub="$2" _body="$3"
    if command -v mailx >/dev/null 2>&1; then
        # most Linux mailx support -a for header
        printf "%s\n" "$_body" | mailx -a "Content-Type: text/html" -s "$_sub" "$_to"
    else
        echo "[$(date)] ERROR: mailx not found, cannot send HTML to $_to" >&2
    fi
}

send_text_email() {
    # plain text pager
    # $1=to $2=subject $3=body
    local _to="$1" _sub="$2" _body="$3"
    if command -v mailx >/dev/null 2>&1; then
        printf "%s\n" "$_body" | mailx -s "$_sub" "$_to"
    else
        echo "[$(date)] ERROR: mailx not found, cannot send TEXT to $_to" >&2
    fi
}

html_header() {
cat <<EOF
<html>
<head>
  <style>
    body { font-family: Arial, Helvetica, sans-serif; font-size: 13px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #666; padding: 4px; text-align: left; }
    th { background: #f2f2f2; }
    .warn { background: #fff3cd; }
    .crit { background: #f8d7da; }
    pre { background: #f5f5f5; padding: 6px; border: 1px solid #ddd; }
  </style>
</head>
<body>
EOF
}

html_footer() {
cat <<EOF
</body>
</html>
EOF
}

# ------------------ main ----------------------------------------------------

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Config file $CONFIG_FILE not found. Exiting."
    exit 1
fi

while IFS='|' read -r GGHOME DB_NAME WARN_LAG CRIT_LAG ALERT_EMAIL PAGE_EMAIL; do
    # skip comments / blanks
    [[ -z "$GGHOME" ]] && continue
    [[ "$GGHOME" =~ ^# ]] && continue

    # per-line env
    export GG_HOME="$GGHOME"

    # build PATH for this line
    LINE_PATH="$PATH"
    [[ -n "$COMMON_ORACLE_HOME" ]] && LINE_PATH="${COMMON_ORACLE_HOME}/bin:${LINE_PATH}"
    LINE_PATH="${GG_HOME}:${LINE_PATH}"
    [[ -n "$COMMON_PATH_EXTRA" ]] && LINE_PATH="${COMMON_PATH_EXTRA}:${LINE_PATH}"
    export PATH="$LINE_PATH"

    GGSCI_BIN="${GG_HOME}/ggsci"
    if [[ ! -x "$GGSCI_BIN" ]]; then
        echo "GGSCI not found at $GGSCI_BIN – skipping"
        continue
    fi

    WARN_SEC=$(time_to_sec "$WARN_LAG")
    CRIT_SEC=$(time_to_sec "$CRIT_LAG")

    # run info all
    INFO_ALL_OUT=$("$GGSCI_BIN" <<EOF 2>/dev/null
info all
EOF
)
    if [[ -z "$INFO_ALL_OUT" ]]; then
        SUBJECT="[CRITICAL][OGG][$HOSTNAME][$DB_NAME] GGSCI failed for $GG_HOME"
        HTML=$(html_header)
        HTML+="<h3 style=\"color:#b30000;\">GoldenGate ALERT (CRITICAL)</h3>"
        HTML+="<p><b>Host:</b> $HOSTNAME<br><b>DB:</b> $DB_NAME<br><b>GG Home:</b> $GG_HOME<br><b>Time:</b> $DATE_STR</p>"
        HTML+="<p>GGSCI did not return output. Check common ORACLE_HOME/TNS/PATH or OGG install.</p>"
        HTML+="$(html_footer)"
        [[ -n "$ALERT_EMAIL" ]] && send_html_email "$ALERT_EMAIL" "$SUBJECT" "$HTML"

        if [[ -n "$PAGE_EMAIL" ]]; then
            TXT="CRITICAL OGG ALERT
Host   : $HOSTNAME
DB     : $DB_NAME
GGHome : $GG_HOME
Time   : $DATE_STR
Issue  : GGSCI command failed (info all)
Action : Check env and GoldenGate now."
            send_text_email "$PAGE_EMAIL" "$SUBJECT" "$TXT"
        fi
        continue
    fi

    # manager check
    MANAGER_LINE=$(echo "$INFO_ALL_OUT" | awk '/^MANAGER/ {print $0}')
    MANAGER_CRIT=0
    if echo "$MANAGER_LINE" | grep -Eiq "STOP|ABEND|STOPPED|ABENDED|NOT RUNNING"; then
        MANAGER_CRIT=1
    fi

    PROBLEM_ROWS=""
    OVERALL_SEVERITY="NONE"
    PAGE_TEXT_LINES=()

    if [[ $MANAGER_CRIT -eq 1 ]]; then
        OVERALL_SEVERITY="CRIT"
        PROBLEM_ROWS+="<tr class=\"crit\"><td>MANAGER</td><td>MANAGER</td><td>CRITICAL</td><td>Manager is not running</td><td>--</td></tr>"
        PAGE_TEXT_LINES+=("Manager DOWN")
    else
        # loop processes
        while IFS='|' read -r PROG STATUS GRP; do
            if echo "$STATUS" | grep -Eiq "STOP|ABEND|ABENDED|STOPPED|NOT.RUNNING|NOTRUNNING"; then
                OVERALL_SEVERITY="CRIT"
                PROBLEM_ROWS+="<tr class=\"crit\"><td>${PROG}</td><td>${GRP}</td><td>CRITICAL</td><td>${PROG} ${GRP} is ${STATUS}</td><td>--</td></tr>"
                PAGE_TEXT_LINES+=("${PROG} ${GRP} is ${STATUS}")
            else
                # running -> check lag
                LAG_OUT=$("$GGSCI_BIN" <<EOF 2>/dev/null
info $GRP, showch
EOF
)
                LAG_LINE=$(echo "$LAG_OUT" | grep -i "Lag at Chkpt" | head -1)
                [[ -z "$LAG_LINE" ]] && LAG_LINE=$(echo "$LAG_OUT" | grep -i "Lag at" | head -1)
                LAG_STR="00:00:00"
                if [[ -n "$LAG_LINE" ]]; then
                    LAG_STR=$(echo "$LAG_LINE" | sed -n 's/.*Lag at [Cc]hkpt[[:space:]]*\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\).*/\1/p')
                    [[ -z "$LAG_STR" ]] && LAG_STR=$(echo "$LAG_LINE" | grep -oE '[0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1)
                fi
                LAG_SEC=$(time_to_sec "$LAG_STR")

                if (( LAG_SEC >= CRIT_SEC )); then
                    OVERALL_SEVERITY="CRIT"
                    PROBLEM_ROWS+="<tr class=\"crit\"><td>${PROG}</td><td>${GRP}</td><td>CRITICAL</td><td>Lag at Chkpt ${LAG_STR} ≥ critical ${CRIT_LAG}</td><td>${LAG_STR}</td></tr>"
                    PAGE_TEXT_LINES+=("${PROG} ${GRP} lag ${LAG_STR} >= crit ${CRIT_LAG}")
                elif (( LAG_SEC >= WARN_SEC )); then
                    [[ "$OVERALL_SEVERITY" != "CRIT" ]] && OVERALL_SEVERITY="WARN"
                    PROBLEM_ROWS+="<tr class=\"warn\"><td>${PROG}</td><td>${GRP}</td><td>WARNING</td><td>Lag at Chkpt ${LAG_STR} ≥ warning ${WARN_LAG}</td><td>${LAG_STR}</td></tr>"
                fi
            fi
        done < <(
            echo "$INFO_ALL_OUT" | awk '
                /^(EXTRACT|REPLICAT)[[:space:]]+/ {
                    prog=$1; status=$2; grp=$3;
                    print prog "|" status "|" grp;
                }
            '
        )
    fi

    [[ "$OVERALL_SEVERITY" == "NONE" ]] && continue

    # build HTML
    HTML=$(html_header)
    if [[ "$OVERALL_SEVERITY" == "CRIT" ]]; then
        HTML+="<h3 style=\"color:#b30000;\">GoldenGate ALERT (CRITICAL)</h3>"
    else
        HTML+="<h3 style=\"color:#8a6d3b;\">GoldenGate ALERT (WARNING)</h3>"
    fi
    HTML+="<p><b>Host:</b> $HOSTNAME<br><b>DB:</b> $DB_NAME<br><b>GG Home:</b> $GG_HOME<br><b>Checked at:</b> $DATE_STR</p>"
    HTML+="<table><tr><th>Type</th><th>Group</th><th>Severity</th><th>Description</th><th>Lag</th></tr>"
    HTML+="$PROBLEM_ROWS"
    HTML+="</table>"
    HTML+="<h4>GGSCI Output (info all)</h4>"
    HTML+="<pre>GGSCI> info all
$(printf '%s\n' "$INFO_ALL_OUT")
</pre>"
    HTML+="$(html_footer)"

    if [[ "$OVERALL_SEVERITY" == "CRIT" ]]; then
        SUBJECT="[CRITICAL][OGG][$HOSTNAME][$DB_NAME] Issues in $GG_HOME"
    else
        SUBJECT="[WARNING][OGG][$HOSTNAME][$DB_NAME] Issues in $GG_HOME"
    fi

    [[ -n "$ALERT_EMAIL" ]] && send_html_email "$ALERT_EMAIL" "$SUBJECT" "$HTML"

    # pager
    if [[ "$OVERALL_SEVERITY" == "CRIT" && -n "$PAGE_EMAIL" ]]; then
        TXT="CRITICAL OGG ALERT
Host   : $HOSTNAME
DB     : $DB_NAME
GGHome : $GG_HOME
Time   : $DATE_STR
"
        if ((${#PAGE_TEXT_LINES[@]} > 0)); then
            TXT+="Issues :\n"
            for ln in "${PAGE_TEXT_LINES[@]}"; do
                TXT+=" - $ln\n"
            done
        else
            TXT+="Issues : See HTML alert\n"
        fi
        TXT+="Action : Check GoldenGate now."
        send_text_email "$PAGE_EMAIL" "$SUBJECT" "$TXT"
    fi

done < "$CONFIG_FILE"
