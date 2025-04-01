#!/usr/bin/env bash
# cspell:ignore tlsv
set -euo pipefail

###############################################################################
# Usage: replay_blocks_from_snapshots.sh <start_idx> <end_idx> <base_dir> [--dry-run]
#
# - <start_idx>: first block to process
# - <end_idx>:   last block to process
# - <base_dir>:  directory containing:
#       template-home/   (the base home with config, genesis, etc we init once)
#
# Environment variable:
#   NEARD_PATH
###############################################################################

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <start_idx> <end_idx> <base_dir> [--dry-run]"
  exit 1
fi

START_IDX="$1"
END_IDX="$2"
BASE_DIR="$3"

DRY_RUN=0
if [[ "${4:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

if [[ ! -x "$NEARD_PATH" ]]; then
  echo "ERROR: NEARD_PATH='$NEARD_PATH' is not executable/found."
  exit 1
fi

# Key paths
TEMPLATE_HOME="$BASE_DIR/template-home"
CURRENT_SNAPSHOT="$BASE_DIR/current_snapshot"
CSV_OUTPUT_DIR="$BASE_DIR/csv-output"
mkdir -p "$CSV_OUTPUT_DIR"

SNAPSHOT_LIST_URL="https://snapshot.neardata.xyz/mainnet/rpc/snapshots.txt"

echo "======================================================="
echo "START: $(date)"
echo "Range: $START_IDX..$END_IDX"
echo "Base dir: $BASE_DIR"
echo "NEARD_PATH: $NEARD_PATH"
[[ $DRY_RUN -eq 1 ]] && echo "[DRY RUN MODE: big commands only printed]"
echo "======================================================="

# 1) Download snapshot list
TMP_SNAPSHOT_LIST="$(mktemp /tmp/snapshots.XXXXXX)"
curl --proto '=https' --tlsv1.2 -sSf "$SNAPSHOT_LIST_URL" -o "$TMP_SNAPSHOT_LIST"

mapfile -t ALL_SNAPSHOTS < "$TMP_SNAPSHOT_LIST"
rm -f "$TMP_SNAPSHOT_LIST"

# 2) Filter snapshots >= START_IDX
CANDIDATES=()
for HEAD in "${ALL_SNAPSHOTS[@]}"; do
  if (( HEAD >= START_IDX )); then
    CANDIDATES+=("$HEAD")
  fi
done
if [[ ${#CANDIDATES[@]} -eq 0 ]]; then
  echo "No snapshot has HEAD >= $START_IDX."
  exit 1
fi

CURRENT_START="$START_IDX"

################################################################################
# MAIN LOOP
################################################################################
for SNAP_HEAD in "${CANDIDATES[@]}"; do
  # We'll process [CURRENT_START..LOCAL_END], where LOCAL_END = min(SNAP_HEAD, END_IDX).
  if (( SNAP_HEAD > END_IDX )); then
    LOCAL_END="$END_IDX"
  else
    LOCAL_END="$SNAP_HEAD"
  fi

  if (( CURRENT_START > LOCAL_END )); then
    echo "Done: CURRENT_START=$CURRENT_START > LOCAL_END=$LOCAL_END"
    break
  fi

  echo "============================================"
  echo "SNAPSHOT HEAD=$SNAP_HEAD => blocks $CURRENT_START..$LOCAL_END"
  echo "============================================"

  # 4)
  if [[ ! -d "$TEMPLATE_HOME" ]]; then
    echo "ERROR: $TEMPLATE_HOME doesn't exist."
    exit 1
  fi

  # 5) Download snapshot if needed
  DOWNLOAD_MARKER="$CURRENT_SNAPSHOT/.downloaded_block_$SNAP_HEAD"
  if [[ -f "$DOWNLOAD_MARKER" ]]; then
    echo "Snapshot HEAD=$SNAP_HEAD is already downloaded (marker found)."
  else
    cp -r "$TEMPLATE_HOME" "$CURRENT_SNAPSHOT"
    # heavy command => skip if DRY_RUN
    DOWNLOAD_CMD="curl --proto '=https' --tlsv1.2 -sSf \
      'https://raw.githubusercontent.com/fastnear/static/refs/heads/main/down_rclone.sh' \
      | DATA_PATH='$CURRENT_SNAPSHOT/data' CHAIN_ID='mainnet' BLOCK='$SNAP_HEAD' bash"

    if (( DRY_RUN )); then
      echo "[DRY RUN] Download: $DOWNLOAD_CMD"
    else
      echo "Downloading HEAD=$SNAP_HEAD (~1.5TiB)."
      set +e
      eval "$DOWNLOAD_CMD"
      DL_EXIT=$?
      set -e
      if [[ $DL_EXIT -ne 0 ]]; then
        echo "Warning: snapshot download had exit code=$DL_EXIT, ignoring."
      else
        echo "Snapshot HEAD=$SNAP_HEAD download completed."
      fi
      touch "$DOWNLOAD_MARKER"
    fi
  fi

  # 5b) Run the node for a while => ensures DB migrations
  RUN_CMD="\"$NEARD_PATH\" --home \"$CURRENT_SNAPSHOT\" run"
  if (( DRY_RUN )); then
    echo "[DRY RUN] Migrations => we'd run node: $RUN_CMD"
  else
    echo "Starting node to do migrations"
    LOG_FILE="$CURRENT_SNAPSHOT/node_run.log"
    "$NEARD_PATH" --home "$CURRENT_SNAPSHOT" run >"$LOG_FILE" 2>&1 &
    NODE_PID=$!

    # poll for "Waiting for peers"
    while true; do
      if grep -q "Waiting for peers" "$LOG_FILE" 2>/dev/null; then
        echo "Found 'Waiting for peers' => sending SIGINT to node pid=$NODE_PID"
        kill -SIGINT "$NODE_PID" || true
        break
      fi
      # If node died early, break
      if ! kill -0 "$NODE_PID" 2>/dev/null; then
        echo "Node process died prematurely, aborting migration step."
        break
      fi
      sleep 2
    done

    # Wait for node to exit
    wait "$NODE_PID" || true
    rm -f "$LOG_FILE"
    echo "Node stopped; migrations presumably done."
  fi

  # 6) Determine shard count from view_chain
  echo "[INFO] Checking shard layout at block $CURRENT_START..."
  VIEW_CHAIN_OUT="$("$NEARD_PATH" --home "$CURRENT_SNAPSHOT" \
      view_state view_chain --height "$CURRENT_START" 2>&1 || true)"

  SHARD_IDS="$(echo "$VIEW_CHAIN_OUT" \
    | grep -oE '^shard [0-9]+,' \
    | sed 's/^shard //; s/,//')"

  mapfile -t SHARD_ARRAY <<< "$SHARD_IDS"

  if [[ ${#SHARD_ARRAY[@]} -eq 0 ]]; then
    echo "[WARN] No shard lines found in view_chain output!"
    echo "       Possibly 1 default shard: 0"
    SHARD_ARRAY=(0)
  fi

  echo "Shards found: ${SHARD_ARRAY[*]}"

  # 7) apply_range for each shard with retry
  for SHARD_ID in "${SHARD_ARRAY[@]}"; do
    CSV_FILE="$CSV_OUTPUT_DIR/apply_range_${SNAP_HEAD}_shard${SHARD_ID}.csv"

    # First attempt: "parallel"
    APPLY_CMD_PARALLEL="\"$NEARD_PATH\" --home \"$CURRENT_SNAPSHOT\" view_state apply_range \
      --shard-id=\"$SHARD_ID\" \
      --start-index=\"$CURRENT_START\" \
      --end-index=\"$LOCAL_END\" \
      --csv-file=\"$CSV_FILE\" \
      parallel"

    # Second attempt (only if first fails): "sequential"
    APPLY_CMD_SEQUENTIAL="\"$NEARD_PATH\" --home \"$CURRENT_SNAPSHOT\" view_state apply_range \
      --shard-id=\"$SHARD_ID\" \
      --start-index=\"$CURRENT_START\" \
      --end-index=\"$LOCAL_END\" \
      --csv-file=\"$CSV_FILE\" \
      sequential"

    if (( DRY_RUN )); then
      echo "[DRY RUN] apply_range parallel => $APPLY_CMD_PARALLEL"
      echo "[DRY RUN] apply_range sequential => $APPLY_CMD_SEQUENTIAL"
      continue
    fi

    echo "apply_range shard=$SHARD_ID => $CSV_FILE (parallel attempt)"
    set +e
    eval "$APPLY_CMD_PARALLEL"
    EXIT_CODE_PARALLEL=$?
    set -e

    if [[ $EXIT_CODE_PARALLEL -eq 0 ]]; then
      # parallel succeeded => record success
      echo "$SNAP_HEAD,$SHARD_ID,$CURRENT_START,$LOCAL_END" >> ok.csv
    else
      echo "[WARN] apply_range parallel failed (exit=$EXIT_CODE_PARALLEL), retry with sequential..."
      set +e
      eval "$APPLY_CMD_SEQUENTIAL"
      EXIT_CODE_SEQUENTIAL=$?
      set -e

      if [[ $EXIT_CODE_SEQUENTIAL -eq 0 ]]; then
        # sequential succeeded => record success
        echo "$SNAP_HEAD,$SHARD_ID,$CURRENT_START,$LOCAL_END" >> ok.csv
      else
        echo "[WARN] apply_range sequential also failed (exit=$EXIT_CODE_SEQUENTIAL)."
        echo "$SNAP_HEAD,$SHARD_ID,$CURRENT_START,$LOCAL_END" >> failed.csv
      fi
    fi
  done

  # 8) Remove the snapshot folder
  echo "Removing $CURRENT_SNAPSHOT..."
  rm -rf "$CURRENT_SNAPSHOT"

  # Move CURRENT_START
  CURRENT_START=$((LOCAL_END + 1))
  if (( CURRENT_START > END_IDX )); then
    echo "Reached end_idx=$END_IDX => done."
    break
  fi
done

echo "Finished range [$START_IDX..$END_IDX]."
