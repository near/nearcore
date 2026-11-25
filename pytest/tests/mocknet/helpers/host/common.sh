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

# Holds a lock on the forknet home until the script exits. The scripts that
# change the state, the config, the env file or the binaries take it, so two
# commands on the same host do not overlap. The background setup and reset
# units do not need it: the state is setting-up or resetting while they run,
# and no script accepts it. The lock is re-entrant: a script that another
# script runs inherits fd 9, and flock on the same open file returns at once.
# Opening the file again would wait for the parent forever.
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

# Runs a script from this directory in the background as the transient user
# unit $1, with the same environment files as the neard unit.
run_in_unit() {
    systemd-run --user --collect --unit "$1" \
        -p "EnvironmentFile=-$HOME/.secrets" \
        -p "EnvironmentFile=-$FORKNET_HOME/neard.env" \
        /bin/bash "$FORKNET_HOME/$2"
}
