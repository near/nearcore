#!/usr/bin/env bash

# cspell:word benchnet mpstat tonumber

set -o errexit

CASE="${2:-$CASE}"
BM_PARAMS=${CASE}/params.json

if ! [[ -d $CASE ]]; then
    echo "'$CASE' is not a valid test case directory"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &>/dev/null; then
    echo "jq could not be found, please install it."
    exit 1
fi

NUM_CHUNK_PRODUCERS=$(jq '.chunk_producers' ${BM_PARAMS})
NUM_RPCS=$(jq '.rpcs' ${BM_PARAMS})
NUM_NODES=$((NUM_CHUNK_PRODUCERS + NUM_RPCS))

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
BENCHNET_DIR="${BENCHNET_DIR:-/home/ubuntu/bench}"

RPC_ADDR="127.0.0.1:4040"
SYNTH_BM_PATH="../synth-bm/Cargo.toml"
SYNTH_BM_BIN="${SYNTH_BM_BIN:-/home/ubuntu/nearcore/benchmarks/synth-bm/target/release/near-synth-bm}"
RUN_ON_FORKNET=$(jq 'has("forknet")' ${BM_PARAMS})
PYTEST_PATH="../../pytest/"
TX_GENERATOR=$(jq -r '.tx_generator.enabled // false' ${BM_PARAMS})

echo "Test case: ${CASE}"
echo "Num nodes: ${NUM_NODES}"

if [ "${NUM_NODES}" -eq "1" ]; then
    NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${GENESIS} 2>/dev/null) || true
    VALIDATOR_KEY=${NEAR_HOME}/validator_key.json
else
    for i in $(seq 0 $((NUM_NODES - 1))); do
        NEAR_HOMES+=("${BENCHNET_DIR}/node${i}")
    done
    NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${NEAR_HOMES[0]}/genesis.json 2>/dev/null) || true
    VALIDATOR_KEY=${NEAR_HOMES[0]}/validator_key.json
fi

if [ "${RUN_ON_FORKNET}" = true ]; then
    GEN_NODES_DIR="${GEN_NODES_DIR:-/home/ubuntu/bench}"
    if [ -z "${FORKNET_NAME}" ] || [ -z "${FORKNET_START_HEIGHT}" ]; then
        echo "Error: Required environment variables not set"
        echo "Please set: FORKNET_NAME, FORKNET_START_HEIGHT"
        exit 1
    fi
    FORKNET_ENV="FORKNET_NAME=${FORKNET_NAME} FORKNET_START_HEIGHT=${FORKNET_START_HEIGHT}"
    FORKNET_NEARD_LOG="/home/ubuntu/neard-logs/logs.txt"
    FORKNET_NEARD_PATH="${NEAR_HOME}/neard-runner/binaries/neard0"
    UPDATE_BINARIES="${UPDATE_BINARIES:-false}"
    NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${GENESIS} 2>/dev/null) || true
    NODE_BINARY_URL=$(jq -r '.forknet.binary_url' ${BM_PARAMS})
    VALIDATOR_KEY=${NEAR_HOME}/validator_key.json
    MIRROR="${VIRTUAL_ENV}/python3 tests/mocknet/MIRROR.py --chain-id mainnet --start-height ${FORKNET_START_HEIGHT} \
        --unique-id ${FORKNET_NAME}"
    echo "Forknet name: ${FORKNET_NAME}"
else
    NEARD="${NEARD:-/home/ubuntu/neard}"
    echo "neard path: ${NEARD}"
fi

RPC_URL="http://${RPC_ADDR}"

start_nodes_forknet() {
    cd ${PYTEST_PATH}
    $MIRROR --host-type nodes run-cmd --cmd "cd ${BENCHNET_DIR}; ${FORKNET_ENV} ./bench.sh start-neard0 ${CASE}"
    cd -
}

start_neard0() {
    nohup ${FORKNET_NEARD_PATH} --home ${NEAR_HOME} run &> ${FORKNET_NEARD_LOG} &
}

start_nodes_local() {
    if [ "${NUM_NODES}" -eq "1" ]; then
        sudo systemctl start neard
    else
        mkdir -p ${LOG_DIR}
        for node in "${NEAR_HOMES[@]}"; do
            log="${LOG_DIR}/$(basename ${node})"
            echo "Starting node: ${node}, log: ${log}"
            nohup ${NEARD} --home ${node} run &>${log} &
        done
    fi
}

start_nodes() {
    echo "=> Starting all nodes"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        start_nodes_forknet
    else
        start_nodes_local
    fi
    echo "=> Done"
}

stop_nodes_forknet() {
    cd ${PYTEST_PATH}
    $MIRROR --host-type nodes run-cmd --cmd "killall --wait neard0 || true"
    cd -
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

reset_forknet() {
    cd ${PYTEST_PATH}
    $MIRROR --host-type nodes run-cmd --cmd \
        "find ${NEAR_HOME}/data -mindepth 1 -delete ; rm -rf ${BENCHNET_DIR}/${USERS_DATA_DIR}"
    if [ "${TX_GENERATOR}" = true ]; then
        $MIRROR --host-type nodes run-cmd --cmd \
            "jq 'del(.tx_generator)' ${CONFIG} > tmp.$$.json && \
            mv tmp.$$.json ${CONFIG} || rm tmp.$$.json"
    fi
    cd -
}

reset_local() {
    if [ "${NUM_NODES}" -eq "1" ]; then
        find ${NEAR_HOME}/data -mindepth 1 -delete
    else
        rm -rf ${BENCHNET_DIR}
    fi
    rm -rf ${USERS_DATA_DIR}
}

reset() {
    stop_nodes
    echo "=> Resetting chain history, user accounts and clearing the database"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        reset_forknet
    else
        reset_local
    fi
    echo "=> Done"
}

fetch_forknet_details() {
    # Get all instances for this forknet
    local instances=$(gcloud compute instances list \
        --project=nearone-mocknet \
        --filter="name~'${FORKNET_NAME}' AND -name~'traffic'" \
        --format="get(name,networkInterfaces[0].networkIP)")    
    local total_lines=$(echo "$instances" | wc -l | tr -d ' ')
    local num_cp_instances=$((total_lines - 1))
    # Get the last instance (RPC node)
    FORKNET_RPC_NODE_ID=$(echo "$instances" | tail -n1 | awk '{print $1}')
    FORKNET_RPC_INTERNAL_IP=$(echo "$instances" | tail -n1 | awk '{print $2}')
    if [ -z "$FORKNET_RPC_INTERNAL_IP" ]; then
        echo "FORKNET_RPC_INTERNAL_IP is empty! something went wrong while listing GCP instances"
        exit 1
    fi
    # Extract the public key from the node_key.json file
    NODE_PUBLIC_KEY=$(jq -r '.public_key' ${GEN_NODES_DIR}/node${NUM_CHUNK_PRODUCERS}/node_key.json)
    FORKNET_BOOT_NODES="${NODE_PUBLIC_KEY}@${FORKNET_RPC_INTERNAL_IP}:24567"
    # Verify we have the correct number of chunk producers
    if [ "$num_cp_instances" -ne "$NUM_CHUNK_PRODUCERS" ]; then
        echo "Error: Expected ${NUM_CHUNK_PRODUCERS} chunk producers but found ${num_cp_instances}"
        exit 1
    fi
    # Get chunk producer nodes using head instead of sed
    FORKNET_CP_NODES=$(echo "$instances" | head -n "$num_cp_instances" | awk '{print $1}')
    FORKNET_RPC_ADDR="${FORKNET_RPC_INTERNAL_IP}:3030"
    RPC_ADDR=${FORKNET_RPC_ADDR}
    RPC_URL="http://${RPC_ADDR}"
    echo "Forknet RPC address: ${FORKNET_RPC_ADDR}"
    echo "Forknet RPC node: ${FORKNET_RPC_NODE_ID}"
    echo "Forknet CP nodes: ${FORKNET_CP_NODES}"
}

init_forknet() {
    cd ${PYTEST_PATH}
    $MIRROR init-neard-runner --neard-binary-url ${NODE_BINARY_URL} --neard-upgrade-binary-url ""
    if [ "${UPDATE_BINARIES}" = true ]; then
        $MIRROR --host-type nodes update-binaries || true
    fi
    $MIRROR --host-type nodes run-cmd --cmd "mkdir -p ${BENCHNET_DIR}"
    $MIRROR --host-type nodes upload-file --src ${SYNTH_BM_BIN} --dst ${BENCHNET_DIR}
    $MIRROR --host-type nodes run-cmd --cmd "chmod +x ${BENCHNET_DIR}/near-synth-bm"
    cd -
}

init_local() {
    reset
    if [ "${NUM_NODES}" -eq "1" ]; then
        rm -f ${CONFIG} ${GENESIS}
        /${NEARD} --home ${NEAR_HOME} init --chain-id localnet
    else
        /${NEARD} --home ${BENCHNET_DIR} localnet -v ${NUM_CHUNK_PRODUCERS} --non-validators-rpc ${NUM_RPCS} \
            --tracked-shards=none
    fi
}

init() {
    echo "=> Initializing ${NUM_NODES} node network"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        init_forknet
    else
        init_local
    fi
    tweak_config
    echo "=> Done"
}

edit_genesis() {
    echo "editing ${1}"
    jq 'del(.shard_layout.V1)' ${1} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    
    if [ -f "${BASE_GENESIS_PATCH}" ]; then
        jq -s 'reduce .[] as $item ({}; . * $item)' \
            ${1} ${BASE_GENESIS_PATCH} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    fi
    
    if [ -f "${GENESIS_PATCH}" ]; then
        jq -s 'reduce .[] as $item ({}; . * $item)' \
            ${1} ${GENESIS_PATCH} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    fi
    
    # remove quotes around "gas_limit" (workaround for jq 1.6 bigint bug)
    sed -i'.bak' -e 's/"gas_limit": "\(.*\)"/"gas_limit": \1/' ${1} && rm "${1}.bak"
}

edit_config() {
    echo "editing ${1}"
    if [ -f "${BASE_CONFIG_PATCH}" ]; then
        jq -s 'reduce .[] as $item ({}; . * $item)' \
            ${1} ${BASE_CONFIG_PATCH} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    fi
    
    if [ -f "${CONFIG_PATCH}" ]; then
        jq -s 'reduce .[] as $item ({}; . * $item)' \
            ${1} ${CONFIG_PATCH} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    fi
}

edit_log_config() {
    echo "editing ${1}"
    touch ${1}
    jq -s 'reduce .[] as $item ({}; . * $item)' \
        ${1} ${BASE_LOG_CONFIG_PATCH} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
}

tweak_config_forknet() {
    fetch_forknet_details
    local cwd=$(pwd)
    cd ${PYTEST_PATH}
    $MIRROR --host-type nodes upload-file --src ${cwd}/bench.sh --dst ${BENCHNET_DIR}
    $MIRROR --host-type nodes upload-file --src ${cwd}/cases --dst ${BENCHNET_DIR}
    $MIRROR --host-type nodes upload-file --src ${GEN_NODES_DIR} --dst ${BENCHNET_DIR}/nodes
    cd -
    local node_index=0
    for node in ${FORKNET_CP_NODES}; do
        local cmd="cp -r ${BENCHNET_DIR}/nodes/node${node_index}/* ${NEAR_HOME}/ && cd ${BENCHNET_DIR};"
        cmd="${cmd} ${FORKNET_ENV} ./bench.sh tweak-config-forknet-node ${CASE} ${FORKNET_BOOT_NODES}"
        cd ${PYTEST_PATH}
        $MIRROR --host-filter ".*${node}" run-cmd --cmd "${cmd}"
        cd -
        node_index=$((node_index + 1))
    done

    cd ${PYTEST_PATH}
    local cmd="cp -r ${BENCHNET_DIR}/nodes/node${NUM_CHUNK_PRODUCERS}/* ${NEAR_HOME}/ && cd ${BENCHNET_DIR};"
    cmd="${cmd} ${FORKNET_ENV} ./bench.sh tweak-config-forknet-node ${CASE}"
    $MIRROR --host-filter ".*${FORKNET_RPC_NODE_ID}" run-cmd --cmd "${cmd}"
    cd -
}

tweak_config_forknet_node() {
    local node_type=${1}
    local boot_nodes=${2}
    jq --arg val "0.0.0.0:24567" \
        '.network.addr |= $val' ${CONFIG} >tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json
    jq --arg val "0.0.0.0:3030" \
        '.rpc.addr |= $val' ${CONFIG} >tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json
    if [ -n "$boot_nodes" ]; then
        jq --arg val "${boot_nodes}" \
            '.network.boot_nodes |= $val' ${CONFIG} >tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json
    fi
}

tweak_config_local() {
    if [ "${NUM_NODES}" -eq "1" ]; then
        edit_genesis ${GENESIS}
        edit_config ${CONFIG}
        edit_log_config ${LOG_CONFIG}
        # Set single node RPC port to known value
        jq --arg val "${RPC_ADDR}" \
            '.rpc.addr |= $val' ${CONFIG} >tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json
    else
        for node in "${NEAR_HOMES[@]}"; do
            edit_genesis ${node}/genesis.json
            edit_config ${node}/config.json
            edit_log_config ${node}/log_config.json
        done
        # Set single node RPC port to known value
        jq --arg val "${RPC_ADDR}" \
            '.rpc.addr |= $val' ${NEAR_HOMES[NUM_NODES - 1]}/config.json >tmp.$$.json && \
            mv tmp.$$.json ${NEAR_HOMES[NUM_NODES - 1]}/config.json || rm tmp.$$.json
    fi

    if [ -d "${CASE}/epoch_configs" ]; then
        # Copy epoch_configs directory if it exists
        if [ "${NUM_NODES}" -eq "1" ]; then
            local protocol_version=$(jq -r '.protocol_version' ${GENESIS})
            cp -r "${CASE}/epoch_configs" "${NEAR_HOME}/"
            if [ -f "${NEAR_HOME}/epoch_configs/template.json" ]; then
                mv "${NEAR_HOME}/epoch_configs/template.json" "${NEAR_HOME}/epoch_configs/${protocol_version}.json"
            fi
        else
            local protocol_version=$(jq -r '.protocol_version' ${NEAR_HOMES[0]}/genesis.json)
            for node in "${NEAR_HOMES[@]}"; do
                cp -r "${CASE}/epoch_configs" "${node}/"
                if [ -f "${node}/epoch_configs/template.json" ]; then
                    mv "${node}/epoch_configs/template.json" "${node}/epoch_configs/${protocol_version}.json"
                fi
            done
        fi
    fi
}

tweak_config() {
    echo "===> Applying configuration changes"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        tweak_config_forknet
    else
        tweak_config_local
    fi
    echo "===> Done"
}

create_accounts_forknet() {
    fetch_forknet_details
    cd ${PYTEST_PATH}
    if [ "${TX_GENERATOR}" = false ]; then
        $MIRROR --host-filter ".*${FORKNET_RPC_NODE_ID}" run-cmd --cmd \
            "cd ${BENCHNET_DIR}; \
            ${FORKNET_ENV} ./bench.sh create-accounts-local ${CASE} ${RPC_URL}"
    else
        for node in ${FORKNET_CP_NODES}; do
            $MIRROR --host-filter ".*${node}" run-cmd --cmd \
                "cd ${BENCHNET_DIR}; ${FORKNET_ENV} ./bench.sh create-accounts-on-tracked-shard ${CASE} ${RPC_URL}"
        done
    fi
    cd -
}

set_create_accounts_vars() {
    if [ "${RUN_ON_FORKNET}" = true ]; then
        cmd="./near-synth-bm"
    else
        cmd="cargo run --manifest-path ${SYNTH_BM_PATH} --release --"
    fi
    url=${1}

    mkdir -p ${USERS_DATA_DIR}
    num_accounts=$(jq '.num_accounts' ${BM_PARAMS})
    echo "Number of shards: ${NUM_SHARDS}"
    echo "Accounts per shard: ${num_accounts}"
    echo "RPC: ${url}"
}

create_sub_accounts() {
    local shard_index=${1}
    local prefix=${2}
    local data_dir=${3}
    local nonce=$((1 + shard_index * num_accounts))
    echo "Creating ${num_accounts} accounts for shard: ${shard_index}, account prefix: ${prefix}, use data dir: ${data_dir}, nonce: ${nonce}"
    RUST_LOG=info \
        ${cmd} create-sub-accounts \
        --rpc-url ${url} \
        --signer-key-path ${VALIDATOR_KEY} \
        --nonce ${nonce} \
        --sub-account-prefixes ${prefix} \
        --num-sub-accounts ${num_accounts} \
        --deposit 9530606018750000000100000000 \
        --channel-buffer-size 1200 \
        --requests-per-second 100 \
        --user-data-dir ${data_dir}
}

create_accounts_local() {
    set_create_accounts_vars ${1}
    for i in $(seq 0 $((NUM_SHARDS - 1))); do
        local prefix=$(printf "a%02d" ${i})
        local data_dir="${USERS_DATA_DIR}/shard${i}"
        create_sub_accounts ${i} ${prefix} ${data_dir}
    done
}

create_accounts_on_tracked_shard() {
    set_create_accounts_vars ${1}
    local shard=$(curl -s localhost:3030/metrics | grep near_client_tracked_shards | awk -F'[="}]' '$5 == 1 {print $3; exit}')
    # Check if i exists and is an integer
    if [ -z "${shard}" ] || ! [[ "${shard}" =~ ^[0-9]+$ ]]; then
        echo "Error: Failed to get valid shard index from metrics"
        echo "shard=${shard}"
        exit 1
    fi
    local prefix=$(printf "a%02d" ${shard})
    local data_dir="${USERS_DATA_DIR}/shard"
    create_sub_accounts ${shard} ${prefix} ${data_dir}
}

create_accounts() {
    echo "=> Creating accounts"
    if [ "${RUN_ON_FORKNET}" = true ]; then
        create_accounts_forknet
    else
        create_accounts_local ${RPC_URL}
    fi
    echo "=> Done"
}

native_transfers_forknet() {
    fetch_forknet_details
    cd ${PYTEST_PATH}
    $MIRROR --host-filter ".*${FORKNET_RPC_NODE_ID}" run-cmd --cmd \
        "cd ${BENCHNET_DIR}; \
        ${FORKNET_ENV} ./bench.sh native-transfers-local ${CASE} ${RPC_URL}"
    cd -
}

native_transfers_local() {
    local cmd
    if [ "${RUN_ON_FORKNET}" = true ]; then
        cmd="./near-synth-bm"
    else
        cmd="cargo run --manifest-path ${SYNTH_BM_PATH} --release --"
    fi
    local url=${1}

    echo "Number of shards: ${NUM_SHARDS}"
    echo "RPC: ${url}"
    local num_transfers=$(jq '.num_transfers' ${BM_PARAMS})
    local buffer_size=$(jq '.channel_buffer_size' ${BM_PARAMS})
    local rps=$(jq '.requests_per_second' ${BM_PARAMS})
    local rps=$(bc <<<"scale=0;${rps}/${NUM_SHARDS}")
    echo "Config: num_transfers: ${num_transfers}, buffer_size: ${buffer_size}, RPS: ${rps}"
    mkdir -p ${LOG_DIR}
    trap 'kill $(jobs -p) 2>/dev/null' EXIT
    for i in $(seq 0 $((NUM_SHARDS - 1))); do
        local log="${LOG_DIR}/gen_shard${i}"
        local data_dir="${USERS_DATA_DIR}/shard${i}"
        echo "Running benchmark for shard: ${i}, log file: ${log}, data dir: ${data_dir}"
        RUST_LOG=info ${cmd} benchmark-native-transfers \
            --rpc-url ${url} \
            --user-data-dir ${data_dir}/ \
            --num-transfers ${num_transfers} \
            --channel-buffer-size ${buffer_size} \
            --requests-per-second ${rps} \
            --amount 1 &> ${log} &
    done
    wait
}

native_transfers_injection() {
    fetch_forknet_details
    local tps=$(jq -r '.tx_generator.tps' ${BM_PARAMS})
    local volume=$(jq -r '.tx_generator.volume' ${BM_PARAMS})
    local accounts_path="${BENCHNET_DIR}/${USERS_DATA_DIR}/shard"
    cd ${PYTEST_PATH}
    # Create a glob pattern for the host filter
    host_filter=$(echo ${FORKNET_CP_NODES} | sed 's/ /|/g')
    # Stop neard0 on all chunk producer nodes
    $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd "killall --wait neard0 || true"
    # Update the CONFIG file on all chunk producer nodes
    $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd "jq --arg tps ${tps} \
        --arg volume ${volume} --arg accounts_path ${accounts_path} \
        '.tx_generator = {\"tps\": ${tps}, \"volume\": ${volume}, \
        \"accounts_path\": \"${accounts_path}\", \"thread_count\": 2}' ${CONFIG} > tmp.$$.json && \
        mv tmp.$$.json ${CONFIG} || rm tmp.$$.json"
    # Restart neard on all chunk producer nodes
    $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd \
        "cd ${BENCHNET_DIR}; \
        ${FORKNET_ENV} ./bench.sh start-neard0 ${CASE}"
    cd -
}

native_transfers() {
    echo "=> Running native token transfer benchmark"
    if [ "${TX_GENERATOR}" = true ]; then
        native_transfers_injection
    elif [ "${RUN_ON_FORKNET}" = true ]; then
        native_transfers_forknet
    else
        native_transfers_local ${RPC_URL}
    fi
    echo "=> Done"
}

stop_injection() {
    echo "=> Disabling transactions injection"
    fetch_forknet_details
    cd ${PYTEST_PATH}
    # Create a glob pattern for the host filter
    host_filter=$(echo ${FORKNET_CP_NODES} | sed 's/ /|/g')
    # Stop neard0 on all chunk producer nodes
    $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd "killall --wait neard0 || true"
    # Remove tx generator from config on all chunk producer nodes
    $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd "jq 'del(.tx_generator)' ${CONFIG} > tmp.$$.json && mv tmp.$$.json ${CONFIG} || rm tmp.$$.json"
    cd -
    echo "=> Done"
}

monitor() {
    local old_now=0
    local old_processed=0
    local all_tps=()

    while true; do
        date
        local now=$(date +%s%3N)
        local processed=$(curl -s localhost:3030/metrics | \
            grep near_transaction_processed_successfully_total | \
            grep -v "#" | \
            awk '{ print $2 }')

        if [ $old_now -ne 0 ]; then
            local elapsed=$((now - old_now))
            local delta=$((processed - old_processed))
            local tps=$(bc <<<"scale=2;${delta}/${elapsed}*1000")
            all_tps=($tps "${all_tps[@]}")
            all_tps=("${all_tps[@]:0:3}")
            local sum=0
            local count=0

            for x in "${all_tps[@]}"; do
                count=$((count + 1))
                sum=$(bc <<<"${sum}+${x}")
            done

            local avg_tps=$(bc <<<"scale=2;${sum}/${count}")
            echo "elapsed ${elapsed}ms, total tx: ${processed}, delta tx: ${delta}, TPS: ${tps}, sustained TPS: ${avg_tps}"
        fi

        old_now=$now
        old_processed=$processed
        mpstat 10 1 | grep -v Linux
    done
}

case "${1}" in
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

stop-injection)
    stop_injection
    ;;

# Forknet specific methods, not part of user API.
tweak-config-forknet-node)
    tweak_config_forknet_node ${2} ${3}
    ;;

start-neard0)
    start_neard0
    ;;

create-accounts-local)
    create_accounts_local ${3}
    ;;

native-transfers-local)
    native_transfers_local ${3}
    ;;

create-accounts-on-tracked-shard)
    create_accounts_on_tracked_shard ${3}
    ;;

*)
    echo "Usage: ${0} {reset|init|tweak-config|create-accounts|native-transfers|monitor|start-nodes|stop-nodes|stop-injection} <CASE>"
    ;;
esac
