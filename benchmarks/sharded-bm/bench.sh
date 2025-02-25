#!/usr/bin/env bash

# cspell:word benchnet mpstat

set -o errexit

CASE="${2:-$CASE}"
BM_PARAMS=${CASE}/params.json

if ! [[ -d $CASE ]]; then
    echo "'$CASE' is not a valid test case directory"
    exit 1
fi

NUM_CHUNK_PRODUCERS=$(jq '.chunk_producers' ${BM_PARAMS})
NUM_RPCS=$(jq '.rpcs' ${BM_PARAMS})
NUM_NODES=$((NUM_CHUNK_PRODUCERS+NUM_RPCS))

if [ "${NUM_RPCS}" -gt "1" ]; then
    echo "no more than 1 rpc node is currently supported"
    exit 1
fi

NEAR_HOME="${NEAR_HOME:-/home/ubuntu/.near}"
GENESIS=${NEAR_HOME}/genesis.json
CONFIG=${NEAR_HOME}/config.json
LOG_CONFIG=${NEAR_HOME}/log_config.json

BASE_GENESIS_PATCH=${CASE}/$(jq -r '.base_genesis_patch' ${BM_PARAMS})
BASE_CONFIG_PATCH=${CASE}/$(jq -r '.base_config_patch' ${BM_PARAMS})
BASE_LOG_CONFIG_PATCH=cases/log_patch.json
GENESIS_PATCH=${CASE}/genesis_patch.json
CONFIG_PATCH=${CASE}/config_patch.json

USERS_DATA_DIR="${USERS_DATA_DIR:-user-data}"
LOG_DIR="${LOG_DIR:-logs}"
BENCHNET_DIR="${BENCHNET_DIR:-$HOME/bench}"

RPC_ADDR="127.0.0.1:4040"
SYNTH_BM_PATH="../synth-bm/Cargo.toml"
SYNTH_BM_BIN="${SYNTH_BM_BIN:-../synth-bm/target/release/near-synth-bm}"
RUN_ON_FORKNET=$(jq 'has("forknet")' ${BM_PARAMS})

echo "Test case: ${CASE}"
echo "Num nodes: ${NUM_NODES}"
if [ "${RUN_ON_FORKNET}" = true ]; then
    if [[ -z "${VIRTUAL_ENV}" ]]; then
        echo "Must provide VIRTUAL_ENV in environment" 1>&2
        exit 1
    fi
    FORKNET_NAME=$(jq -r '.forknet.name' ${BM_PARAMS})
    FORKNET_RPC_ADDR=$(jq -r '.forknet.rpc_addr' ${BM_PARAMS})
    RPC_ADDR=${FORKNET_RPC_ADDR}
    NODE_BINARY_URL="https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/master/neard"
    alias mirror="${VIRTUAL_ENV}/python3 tests/mocknet/mirror.py --chain-id mainnet --start-height 1 --unique-id ${FORKNET_NAME}"
    echo "Forknet name: ${FORKNET_NAME}"
    echo "Forknet RPC address: ${FORKNET_RPC_ADDR}"
else
    NEARD="${NEARD:-/home/ubuntu/neard}"
    echo "neard path: ${NEARD}"
fi

if [ "${NUM_NODES}" -eq "1" ]; then
    NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${GENESIS} 2>/dev/null) || true 
    VALIDATOR_KEY=${NEAR_HOME}/validator_key.json
else
    for i in $(seq 0 $((NUM_NODES-1))); do
        NEAR_HOMES=("${NEAR_HOMES[@]}" ${BENCHNET_DIR}/node${i})
    done
    NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${NEAR_HOMES[0]}/genesis.json 2>/dev/null) || true
    VALIDATOR_KEY=${NEAR_HOMES[0]}/validator_key.json
fi

RPC_URL="http://${RPC_ADDR}"


start_nodes_forknet() {
    # todo mirror command
}

start_nodes_local() {
    if [ "${NUM_NODES}" -eq "1" ]; then
        sudo systemctl start neard
    else 
        mkdir -p ${LOG_DIR}
        for node in "${NEAR_HOMES[@]}"; do
            log="${LOG_DIR}/$(basename ${node})"
            echo "Starting node: ${node}, log: ${log}"
            nohup ${NEARD} --home ${node} run 2> ${log} &
        done
    fi
}

start_nodes() {
    echo "=> Starting all nodes"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        stop_nodes_forknet
    else
        stop_nodes_local
    fi
    echo "=> Done"
}

stop_nodes_forknet() {
    # todo mirror command
}

stop_nodes_local() {
    if [ "${NUM_NODES}" -eq "1" ]; then
        sudo systemctl stop neard
    else 
        killall --wait neard || true
    fi
}

stop_nodes() {
    echo "=> Stopping all nodes"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        stop_nodes_forknet
    else
        stop_nodes_local
    fi
    echo "=> Done"
}

reset() {
    if [ "${RUN_ON_FORKNET}" = true ]; then
        echo "Not supported on forknet"
        exit 1
    fi

    stop_nodes
    echo "=> Resetting chain history, user accounts and clearing the database"
    if [ "${NUM_NODES}" -eq "1" ]; then
        find ${NEAR_HOME}/data -mindepth 1 -delete
    else 
        rm -rf ${BENCHNET_DIR}
    fi
    rm -rf ${USERS_DATA_DIR}
    echo "=> Done"
}

init_forknet() {
    mirror init-neard-runner --neard-binary-url ${NODE_BINARY_URL}
    mirror update-binaries
    mirror new-test \
    --epoch-length 15000 \
    --genesis-protocol-version 73 \
    --num-validators 7 \
    --num-seats 7 \
    --stateless-setup \
    --new-chain-id ${FORKNET_NAME} \
    --yes

    # Todo copy synth bm & accounts
}

init_local() {
    reset
    if [ "${NUM_NODES}" -eq "1" ]; then
        rm -f ${CONFIG} ${GENESIS} 
        /${NEARD} --home ${NEAR_HOME} init --chain-id localnet
    else
        /${NEARD} --home ${BENCHNET_DIR} localnet -v ${NUM_CHUNK_PRODUCERS} --non-validators-rpc ${NUM_RPCS} --tracked-shards=none
    fi
    tweak_config
}

init() {
    echo "=> Initializing ${NUM_NODES} node network"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        init_forknet
    else
        init_local
    fi
    echo "=> Done"
}

edit_genesis() {
    echo "editing ${1}"
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${1} ${BASE_GENESIS_PATCH} > tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${1} ${GENESIS_PATCH} > tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    # remove quotes around "gas_limit" (workaround for jq 1.6 bigint bug)
    sed -i 's/"gas_limit": "\(.*\)"/"gas_limit": \1/' ${1}
}

edit_config() {
    echo "editing ${1}"
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${1} ${BASE_CONFIG_PATCH} > tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${1} ${CONFIG_PATCH} > tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
}

edit_log_config() {
    echo "editing ${1}"
    touch ${1}
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${1} ${BASE_LOG_CONFIG_PATCH} > tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
}

tweak_config() {
    if [ "${RUN_ON_FORKNET}" = true ]; then
        echo "Not supported on forknet"
        exit 1
    fi

    echo "===> Applying configuration changes"
    if [ "${NUM_NODES}" -eq "1" ]; then
        edit_genesis ${GENESIS}
        edit_config ${CONFIG}
        edit_log_config ${LOG_CONFIG}
        # set single node RPC port to known value
        jq --arg val "${RPC_ADDR}" \
            '.rpc.addr |= $val' ${CONFIG} > tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json
    else
        for node in "${NEAR_HOMES[@]}"; do
            edit_genesis ${node}/genesis.json
            edit_config ${node}/config.json
            edit_log_config ${node}/log_config.json
        done
        # set single node RPC port to known value
        jq --arg val "${RPC_ADDR}" \
            '.rpc.addr |= $val' ${NEAR_HOMES[-1]}/config.json > tmp.$$.json && mv tmp.$$.json ${NEAR_HOMES[-1]}/config.json || rm tmp.$$.json
    fi
    echo "===> Done"
}

create_accounts() {
    if [ "${RUN_ON_FORKNET}" = true ]; then
        echo "Not supported on forknet"
        exit 1
    fi

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
            --signer-key-path ${VALIDATOR_KEY} \
            --nonce ${nonce} \
            --sub-account-prefix ${prefix} \
            --num-sub-accounts ${num_accounts} \
            --deposit 953060601875000000010000 \
            --channel-buffer-size 1200 \
            --requests-per-second 500 \
            --user-data-dir ${data_dir}
    done

    echo "=> Done"
}

native_transfers_forknet() {
    # todo
}

native_transfers_local() {
    mkdir -p ${LOG_DIR}
    trap 'kill $(jobs -p) 2>/dev/null' EXIT
    for i in $(seq 0 $((NUM_SHARDS-1))); do
        log="${LOG_DIR}/gen_shard${i}"
        data_dir="${USERS_DATA_DIR}/shard${i}"
        echo "Running benchmark for shard: ${i}, log file: ${log}, data dir: ${data_dir}"
        RUST_LOG=info \
        cargo run --manifest-path ${SYNTH_BM_PATH} --release -- benchmark-native-transfers \
            --rpc-url ${RPC_URL} \
            --user-data-dir ${data_dir}/ \
            --num-transfers ${num_transfers} \
            --channel-buffer-size ${buffer_size} \
            --requests-per-second ${rps} \
            --amount 1 &> ${log} &
    done
    wait
}

native_transfers() {
    echo "=> Running native token transfer benchmark"
    echo "Number of shards: ${NUM_SHARDS}"

    num_transfers=$(jq '.num_transfers' ${BM_PARAMS})
    buffer_size=$(jq '.channel_buffer_size' ${BM_PARAMS})
    rps=$(jq '.requests_per_second' ${BM_PARAMS})
    rps=$(bc <<< "scale=0;${rps}/${NUM_SHARDS}")
    echo "Config: num_transfers: ${num_transfers}, buffer_size: ${buffer_size}, RPS: ${rps}"

    if [ "${RUN_ON_FORKNET}" = true ]; then
        native_transfers_forknet
    else
        native_transfers_local
    fi

    echo "=> Done"
}

monitor() {
    old_now=0
    old_processed=0
    while true 
    do 
        date
        now=$(date +%s%3N)
        processed=$(curl -s localhost:3030/metrics | grep near_transaction_processed_successfully_total | grep -v "#" | awk '{ print $2 }')
        
        if [ $old_now -ne 0 ]; then
            elapsed=$((now-old_now))
            delta=$((processed-old_processed))
            tps=$(bc <<< "scale=2;${delta}/${elapsed}*1000")
            all_tps=($tps "${all_tps[@]}")
            all_tps=("${all_tps[@]:0:3}")
            sum=0
            count=0
            for x in "${all_tps[@]}"; do
                count=$((count+1))
                sum=$(bc <<< "${sum}+${x}")
            done
            avg_tps=$(bc <<< "scale=2;${sum}/${count}")
            echo "elapsed ${elapsed}ms, total tx: ${processed}, delta tx: ${delta}, TPS: ${tps}", sustained TPS: ${avg_tps}
        fi

        old_now=${now}
        old_processed=${processed}

        mpstat 10 1 | grep -v Linux
    done
}

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

    start-nodes)
        start_nodes
        ;;

    stop-nodes)
        stop_nodes
        ;;

    *)
        echo "Usage: $0 {reset|init|tweak-config|create-accounts|native-transfers|monitor|start-nodes|stop-nodes} <BENCH CASE>"
        ;;
esac