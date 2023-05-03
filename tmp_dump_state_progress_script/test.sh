#!/bin/bas
timestamp1="2023-05-03 12:45:45"
timestamp2="2023-05-03 11:45:49"

# Convert the timestamps to Unix epoch time
epoch1=$(date -jf "%Y-%m-%d %H:%M:%S" "$timestamp1" +%s)
epoch2=$(date -jf "%Y-%m-%d %H:%M:%S" "$timestamp2" +%s)

# Calculate the time difference in seconds
diffSeconds="$(($epoch2-$epoch1))"

echo "Diff in seconds: $diffSeconds"
# at this point you have the diff in seconds and you can parse it however you want :)

diffTime=$(gdate -d@${diffSeconds} -u +%H:%M:%S) # you'll need `brew install coreutils`
echo "Diff time(H:M:S): $diffTime"


*/5 * * * * bash ~/workspace/near/nearcore/tmp_dump_state_progress_script/check_progress.sh >> ~/workspace/near/logs/dump_state_output.log 2>&1
*/5 * * * * bash ~/workspace/near/nearcore/tmp_dump_state_progress_script/check_progress_1925.sh >> ~/workspace/near/logs/dump_state_1925_output.log 2>&1

  "state_sync": {
    "dump": {
      "location": {
        "S3": {
          "bucket":"state-parts-xiangyi",
          "region":"us-west-1"
        }
      }
    }
  },


~/.near/log_config.json
{"rust_log": "warn,stats=info,state_sync_dump=debug"}