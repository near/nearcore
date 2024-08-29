#!/bin/bash
set -eox pipefail

# This script can be used on archival nodes to recover missing data from early 2024.
#
# Requirements:
# - The cold store must be mounted on an SSD to make the operation faster.
# - The config 'resharding_config.batch_delay' must be set to 0.
# - neard service must be stopped for the entire duration.
# 
# The script is idempotent. If interrupted, it can be restarted from the beginning, 
# although in the interest of time it's advised to continue from the last successful step.
#
# On top of every command, where applicable, we include the number of expected database writes.
# The estimated number of batches assumes a batch size of 500.0 KB. 
#
# The entire procedure is expected to take several days to complete.

##########
# neard must be stopped
##########
systemctl is-active --quiet neard && echo "neard must be stopped" && exit 1


##########
# Recover missing trie nodes before resharding by re-applying blocks
##########
export RUST_LOG=info

echo "Regenerating trie nodes between heights 109913254 - 110050000 in shard 2"
# DB writes = 377624
/home/ubuntu/neard view-state -t cold --readwrite apply-range --start-index 109913254 --end-index 110050000 --shard-id 2 --storage trie-free --save-state cold sequential


##########
# Perform first resharding
##########
export RUST_LOG=debug

echo "Resharding database at height 114580307 and shard 0"
# DB writes >= 133646770, batch_count = 16569
/home/ubuntu/neard database resharding --height 114580307 --shard-id 0 --restore

echo "Resharding database at height 114580307 and shard 1"
# DB writes = 79328587, batch_count = 13437
/home/ubuntu/neard database resharding --height 114580307 --shard-id 1 --restore

echo "Resharding database at height 114580307 and shard 2"
# DB writes >= 92619516, batch_count = 16467 
/home/ubuntu/neard database resharding --height 114580307 --shard-id 2 --restore

echo "Resharding database at height 114580307 and shard 3"
# DB writes = 258676854, batch_count = 28279
/home/ubuntu/neard database resharding --height 114580307 --shard-id 3 --restore


##########
# Perform second resharding
##########
export RUST_LOG=debug

echo "Resharding database at height 115185107 and shard 0"
# DB writes = 135273932, batch_count = 16856
/home/ubuntu/neard database resharding --height 115185107 --shard-id 0 --restore

echo "Resharding database at height 115185107 and shard 1"
# DB writes = 79959560, batch_count = 13549
/home/ubuntu/neard database resharding --height 115185107 --shard-id 1 --restore

echo "Resharding database at height 115185107 and shard 2"
# DB writes = 188940809, batch_count = 19209
/home/ubuntu/neard database resharding --height 115185107 --shard-id 2 --restore

echo "Resharding database at height 115185107 and shard 3"
# DB writes = 66932493, batch_count = 9886
/home/ubuntu/neard database resharding --height 115185107 --shard-id 3 --restore

echo "Resharding database at height 115185107 and shard 4"
# DB writes = 194985099, batch_count = 18833
/home/ubuntu/neard database resharding --height 115185107 --shard-id 4 --restore