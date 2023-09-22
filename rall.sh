
clear
tmux clear-history

rm stdout-*

TEST=sharding_upgrade

for i in {1..1}
do
    echo "test $i"
    OUT=stdout-$i

    # --run-ignored all \
    # RUST_LOG=info \
    #RUST_LOG=info,catchup=trace,store=trace,client=debug,store=debug,test=debug,resharding=trace \
    # RUST_LOG=debug,resharding=trace,waclaw=trace,catchup=trace \
    # RUST_LOG=debug,resharding=trace \
    RUST_BACKTRACE=all \
    cargo nextest run -p integration-tests \
        --features nightly \
        --no-fail-fast \
        --no-capture \
        $TEST \
        | egrep -v prev_prev_stake_change \
        | egrep -v NetworkRequests.ForwardTx \
        > $OUT
        # | tee $OUT
        # | egrep -v -i "FlatStorage is not ready|Add delta for flat storage creation|epoch_manager: all proposals" \

     sed -E -i 's/ed25519:(.{4})(.{40})/ed25519:\1/g' $OUT

     sed -E -i 's/([0-9]*)([0-9]{30})/\1e30/g' $OUT
     sed -E -i 's/([0-9]*)([0-9]{25})/\1e25/g' $OUT
     sed -E -i 's/([0-9]*)([0-9]{20})/\1e20/g' $OUT
     sed -E -i 's/AccountId/AId/g' $OUT

     cat $OUT | egrep -a -i error

 done
