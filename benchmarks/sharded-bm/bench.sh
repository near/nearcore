#!/usr/bin/env bash

# cspell:word benchnet mpstat tonumber

set -o errexit

CASE="${CASE:-$2}"
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

GEN_LOCALNET_DONE=false

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
GENESIS_TIME="${GENESIS_TIME:-2025-04-04T14:24:06.156907Z}"

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
if [ -n "${SYNTH_BM_BIN}" ]; then
    SYNTH_BM_BASENAME="${SYNTH_BM_BASENAME:-$(basename ${SYNTH_BM_BIN})}"
else
    echo "SYNTH_BM_BIN is not set, accounts will be created in other ways, e.g. on forknet init"
fi
RUN_ON_FORKNET=$(jq 'has("forknet")' ${BM_PARAMS})
PYTEST_PATH="../../pytest/"
TX_GENERATOR=$(jq -r '.tx_generator.enabled // false' ${BM_PARAMS})
CREATE_ACCOUNTS_RPS=$(jq -r '.account_rps // 100' ${BM_PARAMS})
EPOCH_LENGTH=$(jq -r '.epoch_length // 1000' ${BASE_GENESIS_PATCH})

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
    if [ -z "${FORKNET_NAME}" ] || [ -z "${FORKNET_START_HEIGHT}" ]; then
        echo "Error: Required environment variables not set"
        echo "Please set: FORKNET_NAME, FORKNET_START_HEIGHT"
        exit 1
    fi
    FORKNET_ENV="FORKNET_NAME=${FORKNET_NAME} FORKNET_START_HEIGHT=${FORKNET_START_HEIGHT}"
    if [ -n "${SYNTH_BM_BASENAME}" ]; then
        FORKNET_ENV="${FORKNET_ENV} SYNTH_BM_BASENAME=${SYNTH_BM_BASENAME}"
    fi
    FORKNET_NEARD_LOG="/home/ubuntu/neard-logs/logs.txt"
    FORKNET_NEARD_PATH="${NEAR_HOME}/neard-runner/binaries/neard0"
    NUM_SHARDS=$(jq '.shard_layout.V2.shard_ids | length' ${GENESIS} 2>/dev/null) || true
    NODE_BINARY_URL=$(jq -r '.forknet.binary_url' ${BM_PARAMS})
    VALIDATOR_KEY=${NEAR_HOME}/validator_key.json
    MIRROR="python3 tests/mocknet/mirror.py --chain-id mainnet --start-height ${FORKNET_START_HEIGHT} \
        --unique-id ${FORKNET_NAME}"
    echo "Forknet name: ${FORKNET_NAME}"
else
    NEARD="${NEARD:-/home/ubuntu/neard}"
    echo "neard path: ${NEARD}"
fi

RPC_URL="http://${RPC_ADDR}"

mirror_cmd() {
    shift
    cd ${PYTEST_PATH}
    $MIRROR --host-type nodes "$@"
    cd -
}

start_nodes_forknet() {
    cd ${PYTEST_PATH}
    $MIRROR start-nodes
    cd -
}

start_neard0() {
    local cmd_suffix=""
    local tracing_ip=${2:-$TRACING_SERVER_INTERNAL_IP}
    local neard_cmd="${FORKNET_NEARD_PATH} --home ${NEAR_HOME} run"

    if [ ! -z "${tracing_ip}" ]; then
        echo "Tracing server internal IP: ${tracing_ip}"
        export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://${tracing_ip}:4317/"
    else
        echo "Tracing server internal IP is not set."
    fi

    nohup ${neard_cmd} > ${FORKNET_NEARD_LOG} 2>&1 &
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
    $MIRROR stop-nodes
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
        "find ${NEAR_HOME}/data -mindepth 1 -delete ; rm -rf ${BENCHNET_DIR} "
    $MIRROR --host-type nodes env --clear-all
    $MIRROR reset --backup-id start --yes
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
        --filter="name~'-${FORKNET_NAME}-' AND -name~'traffic' AND -name~'tracing'" \
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
    # Verify we have the correct number of chunk producers
    if [ "$num_cp_instances" -ne "$NUM_CHUNK_PRODUCERS" ]; then
        echo "Error: Expected ${NUM_CHUNK_PRODUCERS} chunk producers but found ${num_cp_instances}"
        exit 1
    fi
    # Get chunk producer nodes
    FORKNET_CP_NODES=$(echo "$instances" | head -n "$num_cp_instances" | awk '{print $1}')
    FORKNET_RPC_ADDR="${FORKNET_RPC_INTERNAL_IP}:3030"
    RPC_ADDR=${FORKNET_RPC_ADDR}
    RPC_URL="http://${RPC_ADDR}"
    echo "Forknet RPC address: ${FORKNET_RPC_ADDR}"
    echo "Forknet RPC node: ${FORKNET_RPC_NODE_ID}"
    echo "Forknet CP nodes: ${FORKNET_CP_NODES}"
    # Try to get tracing server IP if it exists
    local tracing_instances=$(gcloud compute instances list \
        --project=nearone-mocknet \
        --filter="name~'-${FORKNET_NAME}-' AND name~'tracing'" \
        --format="get(networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)")
    if [ ! -z "$tracing_instances" ]; then
        TRACING_SERVER_INTERNAL_IP=$(echo "$tracing_instances" | awk '{print $1}')
        TRACING_SERVER_EXTERNAL_IP=$(echo "$tracing_instances" | awk '{print $2}')
        echo "Tracing server internal IP: ${TRACING_SERVER_INTERNAL_IP}"
        echo "Tracing server external IP: ${TRACING_SERVER_EXTERNAL_IP}"
    fi
}

gen_forknet() {
    if [ "${GEN_LOCALNET_DONE}" = true ]; then
        echo "Will use existing nodes homes for forknet"
        return 0
    fi
    
    echo "===> Initializing nodes homes for forknet using new-test"
    local cwd=$(pwd)
    cd ${PYTEST_PATH}
    
    # Upload bench.sh and test case files to all nodes
    $MIRROR --host-type nodes upload-file --src ${cwd}/bench.sh --dst ${BENCHNET_DIR}
    $MIRROR --host-type nodes upload-file --src ${cwd}/cases --dst ${BENCHNET_DIR}

    echo "Running new-test to initialize nodes and collect validator keys"
    $MIRROR --host-type nodes new-test --state-source empty --patches-path "${BENCHNET_DIR}/${CASE}" \
        --epoch-length ${EPOCH_LENGTH} --num-validators ${NUM_CHUNK_PRODUCERS} \
        --new-chain-id ${FORKNET_NAME} --stateless-setup --yes
    
    echo "Waiting for node initialization to complete..."
    $MIRROR --host-type nodes status
    while ! $MIRROR --host-type nodes status | grep -q "all.*nodes ready"; do
        echo "Waiting for nodes to be ready..."
        sleep 10
        $MIRROR --host-type nodes status
    done
    
    cd -
    
    GEN_LOCALNET_DONE=true
    echo "===> Done initializing nodes with new-test"
}

init_forknet() {
    cd ${PYTEST_PATH}
    # Initialize neard runner with the specified binary
    $MIRROR init-neard-runner --neard-binary-url ${NODE_BINARY_URL} --neard-upgrade-binary-url ""
    
    if [ "${UPDATE_BINARIES}" = "true" ] || [ "${UPDATE_BINARIES}" = "1" ]; then
        echo "===> Updating binaries"
        $MIRROR --host-type nodes update-binaries || true
    fi
    
    # Create benchmark dir on nodes
    $MIRROR --host-type nodes run-cmd --cmd "mkdir -p ${BENCHNET_DIR}"
    
    if [ -n "${SYNTH_BM_BIN}" ]; then
        # Check if SYNTH_BM_BIN is a URL or a filepath and handle accordingly
        if [[ "${SYNTH_BM_BIN}" =~ ^https?:// ]]; then
            # It's a URL, download it on remote machines
            $MIRROR --host-type nodes run-cmd --cmd "cd ${BENCHNET_DIR} && curl -L -o ${SYNTH_BM_BASENAME} ${SYNTH_BM_BIN} && chmod +x ${SYNTH_BM_BASENAME}"
        else
            # It's a filepath, upload it from local machine
            $MIRROR --host-type nodes upload-file --src ${SYNTH_BM_BIN} --dst ${BENCHNET_DIR}
            $MIRROR --host-type nodes run-cmd --cmd "chmod +x ${BENCHNET_DIR}/${SYNTH_BM_BASENAME}"
        fi
    fi
    
    cd -
    
    # Initialize the network using new-test command
    gen_forknet
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
    jq --arg time "${GENESIS_TIME}" \
        'del(.shard_layout.V1) | .genesis_time = $time' ${1} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    
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
    if [ -z "${TRACING_SERVER_INTERNAL_IP}" ]; then
        jq '.opentelemetry = null' ${1} >tmp.$$.json && mv tmp.$$.json ${1} || rm tmp.$$.json
    fi
}

tweak_config_forknet() {
    gen_forknet
    fetch_forknet_details

    cd ${PYTEST_PATH}
    
    if [ ! -z "${TRACING_SERVER_INTERNAL_IP}" ]; then
        echo "Will start nodes with tracing enabled to: ${TRACING_SERVER_INTERNAL_IP}"
        FORKNET_ENV="${FORKNET_ENV} TRACING_SERVER_INTERNAL_IP=${TRACING_SERVER_INTERNAL_IP}"
        $MIRROR --host-type nodes env --key-value "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://${TRACING_SERVER_INTERNAL_IP}:4317"
    fi

    # Apply custom configs
    local cmd="cd ${BENCHNET_DIR}; ${FORKNET_ENV} ./bench.sh tweak-config-forknet-node ${CASE}"
    $MIRROR --host-type nodes run-cmd --cmd "${cmd}"

    # TODO: Apply custom configs to RPC node to set up all shards tracking
    # local cmd="
    #     jq '.tracked_shards_config = \"AllShards\" | .store.load_mem_tries_for_tracked_shards = false' ${CONFIG} > tmp.$$.json && \
    #     mv tmp.$$.json ${CONFIG} || rm tmp.$$.json
    # "
    # $MIRROR --host-filter ".*${FORKNET_RPC_NODE_ID}" run-cmd --cmd "${cmd}"
    
    cd -
}

tweak_config_forknet_node() {
    edit_genesis ${GENESIS}
    edit_config ${CONFIG}
    edit_log_config ${LOG_CONFIG}
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
        local host_filter=$(echo ${FORKNET_CP_NODES} | sed 's/ /|/g')
        $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd \
            "cd ${BENCHNET_DIR}; ${FORKNET_ENV} ./bench.sh create-accounts-on-tracked-shard ${CASE} ${RPC_URL}"
    fi
    cd -
}

set_create_accounts_vars() {
    if [ "${RUN_ON_FORKNET}" = true ]; then
        cmd="./${SYNTH_BM_BASENAME}"
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
        --requests-per-second ${CREATE_ACCOUNTS_RPS} \
        --user-data-dir ${data_dir} \
        --ignore-failures
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
    local source_file="${NEAR_HOME}/user-data/shard_${shard}.json"
    local data_dir="${USERS_DATA_DIR}/shard.json"
    echo "Copying user data from ${source_file} to ${data_dir}"
    rm -rf ${data_dir}
    mkdir -p ${USERS_DATA_DIR}
    cp ${source_file} ${data_dir}
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
        cmd="./${SYNTH_BM_BASENAME}"
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
    local accounts_path="${BENCHNET_DIR}/${USERS_DATA_DIR}/shard.json"
    cd ${PYTEST_PATH}
    # Create a glob pattern for the host filter
    host_filter=$(echo ${FORKNET_CP_NODES} | sed 's/ /|/g')
    
    # Update the CONFIG file on all chunk producer nodes
    $MIRROR --host-filter ".*(${host_filter})" stop-nodes
    $MIRROR --host-filter ".*(${host_filter})" run-cmd --cmd "jq --arg tps ${tps} \
        --arg volume ${volume} --arg accounts_path ${accounts_path} \
        '.tx_generator = {\"tps\": ${tps}, \"volume\": ${volume}, \
        \"accounts_path\": \"${accounts_path}\", \"thread_count\": 2}' ${CONFIG} > tmp.$$.json && \
        mv tmp.$$.json ${CONFIG} || rm tmp.$$.json"
    $MIRROR --host-filter ".*(${host_filter})" start-nodes

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
    
    $MIRROR stop-nodes
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

get_traces() {
    fetch_forknet_details
    echo "=> Fetching latest traces"
    if [ -z "${TRACING_SERVER_EXTERNAL_IP}" ]; then
        echo "Error: TRACING_SERVER_EXTERNAL_IP is not set."
        return 1
    fi
    
    local cur_time="${2:-$(date +%s)}"
    local lag_secs=10
    local len_secs=10
    local output_dir=${1:-.}
    
    local start_time=$(bc <<< "$cur_time - $lag_secs - $len_secs")
    start_time="$start_time""000"
    local end_time=$(bc <<< "$cur_time - $lag_secs")
    end_time="$end_time""000"
    
    echo "Current time: $cur_time"
    echo "Start time: $start_time"
    echo "End time: $end_time"
    
    mkdir -p "${output_dir}"
    
    local trace_file="${output_dir}/trace_${start_time}.json"
    local profile_file="${output_dir}/profile_${start_time}.json"
    
    curl -X POST http://${TRACING_SERVER_EXTERNAL_IP}:8080/raw_trace --compressed \
        -H 'Content-Type: application/json' \
        -d "{\"start_timestamp_unix_ms\": $start_time, \"end_timestamp_unix_ms\": $end_time, \"filter\": {\"nodes\": [],\"threads\": []}}" \
        -o "${trace_file}"
    
    curl -X POST http://${TRACING_SERVER_EXTERNAL_IP}:8080/profile --compressed \
        -H 'Content-Type: application/json' \
        -d "{\"start_timestamp_unix_ms\": $start_time, \"end_timestamp_unix_ms\": $end_time, \"filter\": {\"nodes\": [],\"threads\": []}}" \
        -o "${profile_file}"
        
    echo "=> Traces saved to ${trace_file} and ${profile_file}"
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

get-traces)
    get_traces ${2} ${3}
    ;;

mirror)
    mirror_cmd "$@"
    ;;

# Forknet specific methods, not part of user API.
tweak-config-forknet-node)
    tweak_config_forknet_node
    ;;

start-neard0)
    start_neard0 ${2} ${3}
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
    echo "Usage: ${0} {reset|init|tweak-config|create-accounts|native-transfers|monitor|start-nodes|stop-nodes|stop-injection|get-traces|mirror}"
    ;;
esac
