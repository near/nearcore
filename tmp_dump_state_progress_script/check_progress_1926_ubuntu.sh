#!/bin/bash
  
echo $(date +'%Y-%m-%d %H:%M:%S')

bucket_name="state-parts-xiangyi"
chain_id="testnet"
epoch_height="1923"
epoch_id="FGjVC2AzDGxomZv3wEW6z95WfkjCURaSxX5SzQ6ot8pk"

for shard_id in {0..3}; do
  objects_shard=$(aws s3 ls s3://state-parts-xiangyi/multi_node_tmp_1/chain_id=testnet/epoch_height=1926/epoch_id=HnKfEGZ1got7bkzakech1aFSt1TNibVVmE3AoSdW6sFL/shard_id=$shard_id/ | sort -k1,2)
  num_entries_str=$(echo "$objects_shard" | wc -l)
  num_entries=$(( $(echo "$num_entries_str" | tr -d ' ') ))
  first_file=$(echo "$objects_shard" | awk '{print $NF}')

  IFS="_" read -ra parts <<< "$first_file"

  last_substr=${parts[4]}
  total_parts=$((10#$last_substr))
  percentage_done=$(echo "scale=3; $num_entries/$total_parts" | bc)

  # Get the earliest and latest modified time
  earliest=$(echo "$objects_shard" | head -n 1  | awk '{print $1, $2}')
  latest=$(echo "$objects_shard" | tail -n 1 | awk '{print $1, $2}')

  # Convert the timestamps to Unix epoch time
  earliest_epoch=$(date -d "$earliest" +%s)
  latest_epoch=$(date -d "$latest" +%s)

  # Calculate the time difference in seconds
  time_diff=$((latest_epoch - earliest_epoch))

  # at this point you have the diff in seconds and you can parse it however you want :)
  diffTime=$(date -u -d @"$time_diff" +"%H:%M:%S") # you'll need `brew install coreutils`

  echo "shard_id=$shard_id: percentage_done=$percentage_done, dumped_entries=$num_entries, total_parts=$total_parts, time_spent=$diffTime"
done