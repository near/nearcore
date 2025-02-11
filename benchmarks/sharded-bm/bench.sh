#!/usr/bin/env bash

set -o errexit

NEARD="${NEARD:-/home/ubuntu/neard}"
NEAR_HOME="${NEAR_HOME:-/home/ubuntu/.near}"

GENESIS=${NEAR_HOME}/genesis.json
CONFIG=${NEAR_HOME}/config.json

CASE="cases/$2"
BM_PARAMS=${CASE}/params.json
GENESIS_PATCH=${CASE}/genesis_patch.json
CONFIG_PATCH=${CASE}/config_patch.json

USERS_DATA_DIR="${USERS_DATA_DIR:-user-data}"
LOG_DIR="${LOG_DIR:-logs}"

RPC_URL="${RPC_URL:-http://127.0.0.1:4040}"
SYNTH_BM_PATH="../synth-bm/Cargo.toml"
NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${GENESIS})

reset() {
    echo "=> Resetting chain history, user accounts and clearing the database"
    rm -rf ${NEAR_HOME}/data/*
    rm -rf ${USERS_DATA_DIR}
    echo "=> Done"
}

init() {
    echo "=> Initializing network"
    reset
    rm ${CONFIG} ${GENESIS}
    /${NEARD} --home ${NEAR_HOME} init --chain-id localnet
    tweak_config
    echo "=> Done"
}

tweak_config() {
    echo "===> Applying configuration changes"

    echo "editing ${GENESIS}"
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${GENESIS} ${GENESIS_PATCH} > tmp.$$.json && mv tmp.$$.json ${GENESIS} || rm tmp.$$.json
    # remove quotes around "gas_limit" (workaround for jq 1.6 bigint bug)
    sed -i 's/"gas_limit": "\(.*\)"/"gas_limit": \1/' ${GENESIS}

    echo "editing ${CONFIG}"
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${CONFIG} ${CONFIG_PATCH} > tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json

    echo "===> Done"
}

create_accounts() {
    echo "=> Creating accounts"
    echo "Number of shards: ${NUM_SHARDS}"

    mkdir -p ${USERS_DATA_DIR}

    num_accounts=$(jq '.num_accounts' ${BM_PARAMS})

    for i in $(seq 0 $((NUM_SHARDS-1))); do
        prefix=$(printf "a%02d" ${i})
        data_dir="${USERS_DATA_DIR}/shard${i}"
        nonce=$((1+i*num_accounts))
        echo "Creating ${num_accounts} accounts for shard: ${i}, account prefix: ${prefix}, use data dir: ${data_dir}, nonce: ${nonce}"
        RUST_LOG=info \
        cargo run --manifest-path ${SYNTH_BM_PATH} --release -- create-sub-accounts \
            --rpc-url ${RPC_URL} \
            --signer-key-path ${NEAR_HOME}/validator_key.json \
            --nonce ${nonce} \
            --sub-account-prefix ${prefix} \
            --num-sub-accounts ${num_accounts} \
            --deposit 953060601875000000010000 \
            --channel-buffer-size 1200 \
            --interval-duration-micros 800 \
            --user-data-dir ${data_dir}
    done

    echo "=> Done"
}

native_transfers() {
    echo "=> Running native token transfer benchmark"
    echo "Number of shards: ${NUM_SHARDS}"

    mkdir -p ${LOG_DIR}

    num_transfers=$(jq '.num_transfers' ${BM_PARAMS})
    buffer_size=$(jq '.channel_buffer_size' ${BM_PARAMS})
    interval=$(jq '.interval_duration_micros' ${BM_PARAMS})

    echo "Config: num_transfers: ${num_transfers}, buffer_size: ${buffer_size}, interval: ${interval}"

    trap 'kill $(jobs -p) 2>/dev/null' EXIT
    for i in $(seq 0 $((NUM_SHARDS-1))); do
        log="${LOG_DIR}/shard${i}"
        data_dir="${USERS_DATA_DIR}/shard${i}"
        echo "Running benchmark for shard: ${i}, log file: ${log}, data dir: ${data_dir}"
        RUST_LOG=info \
        cargo run --manifest-path ${SYNTH_BM_PATH} --release -- benchmark-native-transfers \
            --rpc-url ${RPC_URL} \
            --user-data-dir ${data_dir}/ \
            --num-transfers ${num_transfers} \
            --channel-buffer-size ${buffer_size} \
            --interval-duration-micros ${interval} \
            --amount 1 &> ${log} &
    done
    wait
    
    echo "=> Done"
}

monitor() {
    old_now=0
    old_processed=0
    while true 
    do 
        date
        now=$(date +%s%3N)
        processed=$(curl -s localhost:3030/metrics | grep transaction_processed_total | grep -v "#" | awk '{ print $2 }')
        
        if [ $old_now -ne 0 ]; then
            elapsed=$((now-old_now))
            delta=$((processed-old_processed))
            tps=$(bc <<< "scale=2;${delta}/${elapsed}*1000")
            echo "elapsed ${elapsed}ms, total tx: ${processed}, delta tx: ${delta}, TPS: ${tps}"
        fi

        old_now=${now}
        old_processed=${processed}

        mpstat 10 1 | grep -v Linux
    done
}

if ! [[ -d $CASE ]]; then
    echo "'$CASE' is not a valid test case directory"
    exit 1
fi

case "$1" in
    reset)
        reset
        ;;

    init)
        init
        ;;

    tweak-config)
        tweak_config
        ;;

    create-accounts)
        create_accounts
        ;;

    native-transfers)
        native_transfers
        ;;

    monitor)
        monitor
        ;;

    *)
        echo "Usage: $0 {reset|init|tweak-config|create-accounts|native-transfers|monitor} <BENCH CASE>"
        ;;
esac