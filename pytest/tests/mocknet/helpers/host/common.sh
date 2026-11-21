# Sourced by the other scripts in this directory. mirror.py uploads the
# directory to the forknet home on each host and writes host.env there.
#
# host.env sets:
#   NEAR_HOME           neard home of the test chain
#   SOURCE_HOME         mainnet neard home read by `neard mirror run`
#   IS_TRAFFIC          1 on the traffic generator, 0 on other nodes
#   RUN_ID              id of the `mirror.py init` run
#   MOCKNET_STORE_PATH  GCS path where the first validator uploads genesis
#   LOGS_DIR            directory of the neard log file

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
