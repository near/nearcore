#!/usr/bin/env bash
set -euo pipefail

# Should be started in detached mode
# to ensure an ssh disconnect will not interrupt the process:
# /split_storage_migration.sh (testnet|mainnnet)  >> ./split_storage_migration.log &
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

if [ "$chain" != "testnet" ] && [ "$chain" != "mainnet" ]
then
  echo "Chain should be 'mainnet' or 'testnet', got '$chain'"
  exit 1
fi

# Prepare all configs
function prepare_configs {
  sudo apt update
  sudo apt install jq awscli -y

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
  sudo systemctl restart neard

  echo 'Waiting 10 minutes'
  sleep 600
}

# Initialize cold storage and make it up to date
function init_cold_storage {
  # Switch to migration mode
  echo 'Starting initial migration run'
  cp $NEAR_HOME/config.json.migration $NEAR_HOME/config.json
  sudo systemctl restart neard

  # Wait for migration success status
  while true
  do
    cold_head=$(curl 0.0.0.0:3030/metrics | grep 'cold_head_height' | tail -n1 | awk '{print $2}')

    echo "Checking if cold storage cold head is ready. Cold head: $cold_head"

    if [[ ! -z "$cold_head" ]]
      then
        break
    fi

    sleep 120
  done
}

# Download latest rpc backup
function download_latest_rpc_backup {
  echo 'Starting rpc download'
  aws s3 --no-sign-request cp "s3://near-protocol-public/backups/$chain/rpc/latest" .
  latest=$(cat latest)
  aws s3 --no-sign-request cp --recursive "s3://near-protocol-public/backups/$chain/rpc/$latest" $NEAR_HOME/hot-data
}

# Finish migration to split storage
function finish_split_storage_migration {
  # Change hot store type
  echo 'Changing rpc db kind to hot'
  sudo systemctl stop neard
  /home/ubuntu/neard cold-store prepare-hot --store-relative-path='hot-data'

  # Switch to split storage mode
  echo 'Starting split storage run'
  cp $NEAR_HOME/config.json.split $NEAR_HOME/config.json
  sudo systemctl restart neard

  sleep 300

  hot_db_kind=$(curl \
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
cp $NEAR_HOME/config.json $NEAR_HOME/config.json.init
prepare_configs
run_with_trie_changes
init_cold_storage
download_latest_rpc_backup
finish_split_storage_migration
