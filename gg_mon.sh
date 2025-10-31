#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Oracle GoldenGate Alert Script (Classic)
# - Multiple GG homes
# - Config format:
#   GGHOME|DB_NAME|WARN_LAG|CRIT_LAG|ALERT_EMAIL|PAGE_EMAIL
# - Alerts -> HTML (detailed)
# - Pages  -> TEXT (short, precise)
# - All alerts/pages include HOST + DB_NAME
# ---------------------------------------------------------------------------

CONFIG_FILE="/opt/oracle/scripts/ogg_mon/ogg_alert.conf"
HOSTNAME=$(hostname)
DATE_STR=$(date '+%Y-%m-%d %H:%M:%S')

# ------------------ helpers -------------------------------------------------

time_to_sec() {
    local t="$1"
    IFS=: read -r h m s <<< "$t"
    h=${h:-0}; m=${m:-0}; s=${s:-0}
    echo $((10#$h*3600 + 10#$m*60 + 10#$s))
}

send_html_email() {
    # $1=to, $2=subj, $3=html
    local _to="$1" _sub="$2" _body="$3"
    if command -v sendmail >/dev/null 2>&1; then
        {
            echo "To: ${_to}"
            echo "Subject: ${_sub}"
            echo "MIME-Version: 1.0"
            echo "Content-Type: text/html"
            echo
            echo "${_body}"
        } | sendmail -t
    elif command -v mailx >/dev/null 2>&1; then
        echo "${_body}" | mailx -a "Content-Type: text/html" -s "${_sub}" "${_to}"
    else
        echo "[$(date)] ERROR: cannot send HTML email to $_to" >&2
    fi
}

send_text_email() {
    # $1=to, $2=subj, $3=body (plain text)
    local _to="$1" _sub="$2" _body="$3"
    if command -v sendmail >/dev/null 2>&1; then
        {
            echo "To: ${_to}"
            echo "Subject: ${_sub}"
            echo
            echo "${_body}"
        } | sendmail -t
    elif command -v mailx >/dev/null 2>&1; then
        echo "${_body}" | mailx -s "${_sub}" "${_to}"
    else
        echo "[$(date)] ERROR: cannot send TEXT email to $_to" >&2
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
    .ok { background: #d4edda; }
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
    # skip blanks / comments
    [[ -z "$GGHOME" ]] && continue
    [[ "$GGHOME" =~ ^# ]] && continue

    GGSCI_BIN="${GGHOME}/ggsci"
    if [[ ! -x "$GGSCI_BIN" ]]; then
        echo "GGSCI not found at $GGSCI_BIN – skipping"
        continue
    fi

    # Convert thresholds
    WARN_SEC=$(time_to_sec "$WARN_LAG")
    CRIT_SEC=$(time_to_sec "$CRIT_LAG")

    # Run info all
    INFO_ALL_OUT=$("$GGSCI_BIN" <<EOF 2>/dev/null
info all
EOF
)
    if [[ -z "$INFO_ALL_OUT" ]]; then
        SUBJECT="[CRITICAL][OGG][$HOSTNAME][$DB_NAME] GGSCI failed for $GGHOME"
        HTML=$(html_header)
        HTML+="<h3 style=\"color:#b30000;\">GoldenGate ALERT (CRITICAL)</h3>"
        HTML+="<p><b>Host:</b> $HOSTNAME<br><b>DB:</b> $DB_NAME<br><b>GG Home:</b> $GGHOME<br><b>Time:</b> $DATE_STR</p>"
        HTML+="<p>GGSCI did not return output. Please check environment / permissions.</p>"
        HTML+="$(html_footer)"
        [[ -n "$ALERT_EMAIL" ]] && send_html_email "$ALERT_EMAIL" "$SUBJECT" "$HTML"

        # pager (TEXT)
        if [[ -n "$PAGE_EMAIL" ]]; then
            TXT="CRITICAL OGG ALERT
Host   : $HOSTNAME
DB     : $DB_NAME
GGHome : $GGHOME
Time   : $DATE_STR
Issue  : GGSCI command failed (info all)
Action : Check OGG processes/env now."
            send_text_email "$PAGE_EMAIL" "$SUBJECT" "$TXT"
        fi
        continue
    fi

    # Check manager first
    MANAGER_LINE=$(echo "$INFO_ALL_OUT" | awk '/^MANAGER/ {print $0}')
    MANAGER_CRIT=0
    if echo "$MANAGER_LINE" | grep -Eiq "STOP|ABEND|STOPPED|ABENDED|NOT RUNNING"; then
        MANAGER_CRIT=1
    fi

    PROBLEM_ROWS=""
    OVERALL_SEVERITY="NONE"
    PAGE_TEXT_LINES=()  # for pager body

    if [[ $MANAGER_CRIT -eq 1 ]]; then
        OVERALL_SEVERITY="CRIT"
        PROBLEM_ROWS+="<tr class=\"crit\"><td>MANAGER</td><td>MANAGER</td><td>CRITICAL</td><td>Manager is not running</td><td>--</td></tr>"
        PAGE_TEXT_LINES+=("Manager DOWN")
    else
        # Manager ok -> check EXTRACT/REPLICAT
        echo "$INFO_ALL_OUT" | awk '
            /^(EXTRACT|REPLICAT)[[:space:]]+/ {
                prog=$1; status=$2; grp=$3;
                print prog "|" status "|" grp;
            }
        ' | while IFS='|' read -r PROG STATUS GRP; do
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
                    # warnings -> no pager
                fi
            fi
        done
    fi

    # nothing to alert
    if [[ "$OVERALL_SEVERITY" == "NONE" ]]; then
        continue
    fi

    # -------- HTML ALERT (alert_email) ----------
    HTML=$(html_header)
    if [[ "$OVERALL_SEVERITY" == "CRIT" ]]; then
        HTML+="<h3 style=\"color:#b30000;\">GoldenGate ALERT (CRITICAL)</h3>"
    else
        HTML+="<h3 style=\"color:#8a6d3b;\">GoldenGate ALERT (WARNING)</h3>"
    fi
    HTML+="<p><b>Host:</b> $HOSTNAME<br><b>DB:</b> $DB_NAME<br><b>GG Home:</b> $GGHOME<br><b>Checked at:</b> $DATE_STR</p>"
    HTML+="<table><tr><th>Type</th><th>Group</th><th>Severity</th><th>Description</th><th>Lag</th></tr>"
    HTML+="$PROBLEM_ROWS"
    HTML+="</table>"

    HTML+="<h4>GGSCI Output (info all)</h4>"
    HTML+="<pre>GGSCI> info all
$(printf '%s\n' "$INFO_ALL_OUT")
</pre>"
    HTML+="$(html_footer)"

    if [[ "$OVERALL_SEVERITY" == "CRIT" ]]; then
        SUBJECT="[CRITICAL][OGG][$HOSTNAME][$DB_NAME] Issues in $GGHOME"
    else
        SUBJECT="[WARNING][OGG][$HOSTNAME][$DB_NAME] Issues in $GGHOME"
    fi

    [[ -n "$ALERT_EMAIL" ]] && send_html_email "$ALERT_EMAIL" "$SUBJECT" "$HTML"

    # -------- TEXT PAGE (page_email, only critical) ----------
    if [[ "$OVERALL_SEVERITY" == "CRIT" && -n "$PAGE_EMAIL" ]]; then
        # make precise short text
        TXT="CRITICAL OGG ALERT
Host   : $HOSTNAME
DB     : $DB_NAME
GGHome : $GGHOME
Time   : $DATE_STR
"
        if ((${#PAGE_TEXT_LINES[@]} > 0)); then
            TXT+="Issues :\n"
            for ln in "${PAGE_TEXT_LINES[@]}"; do
                TXT+=" - $ln\n"
            done
        else
            TXT+="Issues : Unknown (see HTML alert)\n"
        fi
        TXT+="Action : Check GoldenGate now."
        send_text_email "$PAGE_EMAIL" "$SUBJECT" "$TXT"
    fi

done < "$CONFIG_FILE"
