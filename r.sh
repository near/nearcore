
# RUST_LOG=trace,client=trace \
#RUST_LOG=debug,test=trace \

rm stdout-*

for i in {1..9}
do
    echo "test $i"
    OUT=stdout-$i

    # RUST_LOG=info \
    # RUST_BACKTRACE=all \
    cargo nextest run -p integration-tests \
        --no-capture \
        --features nightly \
        test_shard_layout_upgrade_simple \
        | egrep -v -i "FlatStorage is not ready|Add delta for flat storage creation|epoch_manager: all proposals" \
        > $OUT
        # | tee $OUT

        sed -E -i .bak 's/ed25519:(.{4})(.{40})/ed25519:\1/g' $OUT

        sed -E -i .bak 's/([0-9]*)([0-9]{30})/\1e30/g' $OUT
        sed -E -i .bak 's/([0-9]*)([0-9]{25})/\1e25/g' $OUT
        sed -E -i .bak 's/([0-9]*)([0-9]{20})/\1e20/g' $OUT
        sed -E -i .bak 's/AccountId/AId/g' $OUT

done
