# Sourced by the other scripts in this directory. mirror.py uploads the
# directory to the forknet home on each host and writes host.env there.
#
# host.env sets:
#   NEAR_HOME           neard home of the test chain
#   SOURCE_HOME         mainnet neard home read by `neard mirror run`
#   IS_TRAFFIC          1 on the traffic generator, 0 on other nodes
#   RUN_ID              id of the `mirror.py init` run
#   MOCKNET_STORE_PATH  GCS path where the first validator uploads genesis

FORKNET_HOME=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$FORKNET_HOME/host.env"

BINARIES_DIR="$FORKNET_HOME/binaries"
# neard0 runs the setup steps. `current` links to the binary that the neard
# unit runs.
NEARD0="$BINARIES_DIR/neard0"
# One of: new-test, awaiting-network-init, setting-up, resetting, ready,
# or "error: ...". mirror.py starts neard only in the ready state.
STATE_FILE="$FORKNET_HOME/state"
# The state of the test chain right after setup. `reset` restores it.
START_BACKUP="$FORKNET_HOME/backups/start"

# Holds a lock on the forknet home until the script exits, so two commands
# that change the host do not overlap. The setup and reset units skip it: no
# script accepts their states. A child script inherits fd 9 and flock returns
# at once; opening the file again would wait for the parent forever.
lock_host() {
    if [ ! -e /dev/fd/9 ]; then
        exec 9>"$FORKNET_HOME/lock"
    fi
    flock 9
}

# Fails unless the state file holds $1.
require_state() {
    local state
    state=$(cat "$STATE_FILE" 2>/dev/null || echo "no test")
    if [ "$state" != "$1" ]; then
        echo "state is $state, expected $1" >&2
        exit 1
    fi
}

# EnvironmentFile= properties of the neard unit. Transient units get them
# too, so systemd parses the files the same way everywhere.
UNIT_ENV_FILES=(
    -p "EnvironmentFile=-$HOME/.secrets"
    -p "EnvironmentFile=-$FORKNET_HOME/neard.env"
)

# Runs neard0 in a transient unit, so it gets the env files of the neard unit.
run_neard0() {
    systemd-run --user --wait --pipe --quiet --collect --same-dir \
        "${UNIT_ENV_FILES[@]}" "$NEARD0" "$@"
}

# Runs a script from this directory in the background as the transient user
# unit $1.
run_in_unit() {
    systemd-run --user --collect --unit "$1" "${UNIT_ENV_FILES[@]}" \
        /bin/bash "$FORKNET_HOME/$2"
}
