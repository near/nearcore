#!/bin/bash

log=./io_log.txt
base=.
mark=`date +"%Y-%m-%d-%H-%M-%S"`

echo $mark | tee -a $log

# cargo build --release --package neard

#for acc in 10
for acc in 10 100 200 300 1000 2000 3000 10000 15000 20000 30000 40000 50000 60000 70000 80000 90000 100000 500000
do
  echo "Using $acc accounts..." | tee -a $log
  dir=/tmp/data$acc
  rm -rf $dir
  $base/../../target/release/neard  --home $dir init --chain-id= \
    --test-seed=alice.near --account-id=test.near --fast
  $base/emu-cost/counter_plugin/qemu-x86_64  -d plugin -cpu Westmere-v1 \
      -plugin file=$base/emu-cost/counter_plugin/libcounter.so,arg="started",arg="on_every_close"  \
      $base/../../target/release/genesis-populate --home $dir --additional-accounts-num $acc 2>&1 | tee -a $log
  rm -rf $dir
  # tail -1
done

# brew install feedgnuplot
# seq 20 | awk '{print $1, A, $1*$1}' | feedgnuplot --domain --dataid --lines --points  --title "Instructions to IO"  --unset grid --terminal 'dumb 80,40' --exit
# cat $log | python ./emu-cost/data_builder.py | feedgnuplot --domain --dataid --lines --points  --title "Instructions to IO"  --unset grid  --autolegend