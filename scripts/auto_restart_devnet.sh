#!/usr/bin/env bash
set -e

pushd $(dirname "$0")/..

trap ctrl_c INT

function ctrl_c() {
    echo "** Interrupted by CTRL-C"
    DEVNET_PID=$(pgrep devnet)
    echo "Devnet PID is $DEVNET_PID. Killing it"
    kill -9 $DEVNET_PID
}

while :
do
    echo "Starting the devnet and sleeping for 10 sec"
    cargo run --release --package=devnet -- --log-level Debug --test-block-period=500 >> devnet.log 2>&1 &
    sleep 10
    ACCOUNT=rebooter_$(openssl rand -hex 10)
    echo "Creating account $ACCOUNT"
    scripts/rpc.py create_account $ACCOUNT 0
    sleep 3
    CONTRACT=rebooter_$(openssl rand -hex 10)
    echo "Creating contract $CONTRACT"
    scripts/rpc.py create_account -s $ACCOUNT $CONTRACT 0
    sleep 3
    echo "Deploying code at tests/hello.wasm"
    scripts/rpc.py deploy -s $ACCOUNT $CONTRACT tests/hello.wasm
    sleep 3
    while :
    do
        VALUE=$(openssl rand -hex 10)
        echo "Setting value $VALUE"
        scripts/rpc.py schedule_function_call -s $ACCOUNT $CONTRACT setValue --args=\{\"value\":\"$VALUE\"\}
        sleep 5
        echo "Reading value"
        RESULT=$(scripts/rpc.py call_view_function -s $ACCOUNT $CONTRACT getValue)
        echo "Got result $RESULT"
        if [ "$RESULT" != "{\"result\": \"$VALUE\"}" ]; then
            echo "Doesn't match the origin value. Restarting the devnet."
            break
        fi
    done
    DEVNET_PID=$(pgrep devnet)
    echo "Devnet PID is $DEVNET_PID. Killing it"
    kill -9 $DEVNET_PID
    sleep 1
done
