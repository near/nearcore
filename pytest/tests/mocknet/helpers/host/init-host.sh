#!/usr/bin/env bash
# Installs the neard and log rotation user units, then downloads the neard
# binaries. mirror.py runs it from `init` after it uploads this directory,
# host.env and binaries/urls.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

UNIT_DIR="$HOME/.config/systemd/user"
LOG_FILE="$LOGS_DIR/logs.txt"
LOGROTATE_POLICY="$LOGS_DIR/.neard_logrotate_policy"
LOGROTATE_CMD="/usr/sbin/logrotate -s $LOGS_DIR/.logrotate_status $LOGROTATE_POLICY"
mkdir -p "$UNIT_DIR" "$LOGS_DIR"

# Rotate the log when it gets larger than 200M. Remove the oldest rotated logs
# when all logs together use more than the root disk size minus 50G.
# cspell:ignore prerotate copytruncate missingok notifempty dateext endscript delaycompress dateformat
logs_max_size=$(($(df -B1 --output=size / | tail -n 1) - 50 * 1024 * 1024 * 1024))
cat >"$LOGROTATE_POLICY" <<EOF
$LOG_FILE {
    su ubuntu ubuntu
    size 200M
    rotate 100000
    copytruncate
    missingok
    notifempty
    dateext
    compress
    delaycompress
    dateformat -%Y-%m-%d-%H-%M-%S
    create 0644 ubuntu ubuntu
    prerotate
        total_size=\$(du -sb $LOG_FILE* | awk '{total+=\$1}END{print total}')
        while [ \$total_size -gt $logs_max_size ]; do
            oldest_file=\$(ls -1 $LOG_FILE-* | head -n1)
            rm -f "\$oldest_file"
            total_size=\$(du -sb $LOG_FILE* | awk '{total+=\$1}END{print total}')
        done
    endscript
}
EOF

if [ "$IS_TRAFFIC" = 1 ]; then
    # `neard mirror run` exits when it has sent all the traffic.
    restart=no
else
    restart=on-failure
fi

cat >"$UNIT_DIR/neard.service" <<EOF
[Unit]
Description=neard
# Stop restarting after 5 failures in 10 minutes.
StartLimitIntervalSec=600
StartLimitBurst=5

[Service]
EnvironmentFile=-$HOME/.secrets
EnvironmentFile=-$FORKNET_HOME/neard.env
ExecStartPre=-$LOGROTATE_CMD -f
ExecStart=/bin/bash $FORKNET_HOME/run-neard.sh
StandardOutput=append:$LOG_FILE
StandardError=inherit
KillSignal=SIGINT
TimeoutStopSec=300
Restart=$restart
RestartSec=5
EOF

cat >"$UNIT_DIR/neard-logrotate.service" <<EOF
[Unit]
Description=rotate the neard log

[Service]
Type=oneshot
ExecStart=$LOGROTATE_CMD
EOF

cat >"$UNIT_DIR/neard-logrotate.timer" <<EOF
[Unit]
Description=rotate the neard log every minute

[Timer]
OnActiveSec=1min
OnUnitActiveSec=1min

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now neard-logrotate.timer

bash "$FORKNET_HOME/download-binaries.sh"
