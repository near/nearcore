#!/usr/bin/env bash

# Takes one argument -- chain_id (mainnet or testnet)
# Prerequisites:
# - systemd neard service
# - neard home is in /home/ubuntu/.near
# - neard binary is in /home/ubuntu/neard
# - /home/ubuntu/.near/config.json is initialised
# - metrics are exported to 3030 port
# - aws s3 client
# First, prepares config files using jq.
# Restarts neard and waits for cold head to sync with final head
# Downloads latest rpc db in hot-data
# Stops neard, prepares rpc db to be used as hot db, changes config to look at hot db instead of archival db
# Restarts neard
# After that node is running in split storage mode
# Legacy archival db can be removed

NEAR_HOME=/home/ubuntu/.near
chain=$1
service_name=neard

if [ "$chain" != "testnet" ] && [ "$chain" != "mainnet" ]
then
  echo "Chain should be 'mainnet' or 'testnet', got '$chain'"
  exit 1
fi

function intro() {
  echo "This script is going to migrate your archival node to cold storage"
  echo
  echo "The script makes the following assumptions:"
  echo " * You have about 10TB of disk space available. Recommended setup is to mount a cheap HDD to '~/.near/cold-data'."
  echo " * The node can be controlled as a systemd service. The script will run 'sudo systemctl restart $service_name' when it needs to restart the node."
  echo " * The machine has commands 'jq' and 'aws' available, or if those commands are not available, they can be installed using 'apt'."
  echo " * It's ok to stop the node for several minutes."
  echo " * You are not going to do any other maintenance on this node in the next 24 hours."
  echo
  read -p "Do you want to proceed? (Y/N): " choice
  case "$choice" in
    y|Y )
      echo "Proceeding with migration..."
      ;;
    n|N )
      echo "Aborting migration..."
      exit 1 # Exit with status code 1
      ;;
    * )
      echo "Invalid choice, please enter Y or N"
      intro # Re-run the intro function until a valid choice is made
      ;;
  esac
}

function check_jq() {
    if ! command -v jq &> /dev/null; then
        echo "'jq' command not found. Installing 'jq' package using 'apt'..."
        sudo apt update && sudo apt install jq
    else
        echo "'jq' command is already installed"
    fi
}

function check_aws() {
    if ! command -v aws &> /dev/null; then
        echo "'aws' command not found. Installing 'awscli' package using 'apt'..."
        sudo apt update && sudo apt install awscli
    else
        echo "'aws' command is already installed"
    fi
}

# Prepare all configs
function prepare_configs {
  check_jq
  # make archival config
  jq '.archive = true | .save_trie_changes=true' < $NEAR_HOME/config.json > $NEAR_HOME/config.json.archival

  # create migration config that has cold_store part
  jq '.cold_store = .store' < $NEAR_HOME/config.json.archival > $NEAR_HOME/config.json.migration

  # create split storage config that points to the right hot storage
  jq '.store.path = "hot-data"' < $NEAR_HOME/config.json.migration > $NEAR_HOME/config.json.split
}

# Run for 10 minutes with TrieChanges
function run_with_trie_changes {
  echo 'Starting an archival run with TrieChanges'
  cp $NEAR_HOME/config.json.archival $NEAR_HOME/config.json
  echo "Restarting the systemd service '$service_name'"
  sudo systemctl restart $service_name

  echo 'Waiting 10 minutes'
  sleep 600
}

# Initialize cold storage and make it up to date
function init_cold_storage {
  # Switch to migration mode
  echo 'Starting initial migration run'
  echo 'Expect the migration to take a long time (>1 hour).'
  echo 'Do not interrupt. Any interruption will undo the migration process.'
  cp $NEAR_HOME/config.json.migration $NEAR_HOME/config.json

  echo "Restarting the systemd service '$service_name'"
  sudo systemctl restart $service_name

  # Wait for the migration to complete
  while [[ -z "$head" || -z "$cold_head" || $(($head - $cold_head)) -ge 1000 ]]; do
    # Get data from the metrics page of a localhost node that is assumed to listen on port 3030.
  metrics_url="http://0.0.0.0:3030/metrics"
    head=$(curl "$metrics_url" --silent | grep 'block_height_head' | tail -n1 | awk '{print $2}')
    cold_head=$(curl "$metrics_url" --silent | grep 'cold_head_height' | tail -n1 | awk '{print $2}')

    echo "Head of hot storage: '$head'"
    echo "Head of cold storage: '$cold_head'"

    # Wait for 2 minutes before trying again
    sleep 120
  done
}

# Download latest rpc backup
function download_latest_rpc_backup {
  check_aws
  echo 'Starting rpc download'
  aws s3 --no-sign-request cp "s3://near-protocol-public/backups/$chain/rpc/latest" .
  latest=$(cat latest)
  aws s3 --no-sign-request cp --recursive "s3://near-protocol-public/backups/$chain/rpc/$latest" $NEAR_HOME/hot-data
}

# Finish migration to split storage
function finish_split_storage_migration {
  echo "Stopping the systemd service '$service_name'"
  sudo systemctl stop $service_name

  # Change hot store type
  echo 'Changing RPC DB kind to Hot'
  /home/ubuntu/neard cold-store prepare-hot --store-relative-path='hot-data'

  # Switch to split storage mode
  echo 'Starting split storage run'
  cp $NEAR_HOME/config.json.split $NEAR_HOME/config.json
  echo "Restarting the systemd service '$service_name'"
  sudo systemctl restart $service_name

  sleep 300

  hot_db_kind=$(curl \
    --silent
    -H "Content-Type: application/json" \
    -X POST \
    --data '{"jsonrpc":"2.0","method":"EXPERIMENTAL_split_storage_info","params":[],"id":"dontcare"}' \
    0.0.0.0:3030 \
    | jq '.result.hot_db_kind')

  echo "Hot DbKind is '$hot_db_kind'"
  if [ "$hot_db_kind" != '"Hot"' ]
  then
    "Hot DbKind is not 'Hot'"
    exit 1
  else
    "Hot DbKind is correct"
  fi
}

# Backup old config.json
intro
cp $NEAR_HOME/config.json $NEAR_HOME/config.json.init
prepare_configs
run_with_trie_changes
init_cold_storage
download_latest_rpc_backup
finish_split_storage_migration
