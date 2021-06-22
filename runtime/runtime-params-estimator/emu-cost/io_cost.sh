#!/usr/bin/env bash

log=./io_log.txt
base=.
mark=`date +"%Y-%m-%d-%H-%M-%S"`

echo $mark | tee -a $log

# cargo build --release --package neard
# cargo build --release --package genesis-populate


acc=1000000
echo "Using $acc accounts..." | tee -a $log
dir=/tmp/data$acc
rm -rf $dir
$base/../../target/release/neard  --home $dir init \
    --test-seed=alice.near --account-id=test.near --fast
$base/emu-cost/counter_plugin/qemu-x86_64  -d plugin -cpu Westmere-v1 -R 8G \
      -plugin file=$base/emu-cost/counter_plugin/libcounter.so,arg="started",arg="on_every_close"  \
      $base/../../target/release/genesis-populate --home $dir --additional-accounts-num $acc 2>&1 | tee -a $log
rm -rf $dir

# brew install feedgnuplot
# seq 20 | awk '{print $1, A, $1*$1}' | feedgnuplot --domain --dataid --lines --points  --title "Instructions to IO"  --unset grid --terminal 'dumb 80,40' --exit
# cat $log | python ./emu-cost/data_builder.py | feedgnuplot --domain --dataid --lines --points  --title "Instructions to IO"  --unset grid  --autolegend
