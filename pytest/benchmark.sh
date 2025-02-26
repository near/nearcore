#!/bin/bash

# Configuration
LOG_FILE="/tmp/err"
SCRIPT_MODE=""
OPERATION=""
NODE_RPC="http://127.0.0.1:3030"

# Function to show usage
usage() {
    echo "Usage: $0 [--mode 1|2] [--operation prepare|run] [--rpc-url URL]"
    echo "  --mode: Select benchmark mode (1 for native transfers, 2 for sweat benchmark)"
    echo "  --operation: Select operation type (prepare or run)"
    echo "  --rpc-url: RPC URL (optional, defaults to http://127.0.0.1:3030)"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --operation)
            OPERATION="$2"
            shift 2
            ;;
        --rpc-url)
            NODE_RPC="$2"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

# Validate inputs
if [[ -n "$MODE" && ! "$MODE" =~ ^(1|2)$ ]]; then
    echo "Error: Mode must be 1 or 2"
    usage
fi

if [[ -n "$OPERATION" && ! "$OPERATION" =~ ^(prepare|run)$ ]]; then
    echo "Error: Operation must be prepare or run"
    usage
fi

# Function to run command in background with logging
run_background() {
    local cmd="$1"
    local log_file="$2"
    echo "Running command in background: $cmd"
    eval "$cmd" > "$log_file" 2>&1 &
    local pid=$!
    echo "Process ID: $pid"
    echo "Logs are being written to $log_file"
    echo $pid > "/tmp/benchmark_pid_$3"
}

# Function to check status
check_status() {
    local pid=$1
    local log_file=$2
    echo "=== Checking status of background process $pid ==="
    if ! ps -p $pid > /dev/null; then
        echo "Process $pid has finished"
        echo "Last 10 lines of log:"
        tail -n 10 "$log_file"
        return 1
    else
        echo "Process $pid is still running"
        echo "Last 5 lines of log:"
        tail -n 5 "$log_file"
        return 0
    fi
}

# Mode 1: Native Transfers
run_mode_1() {
    local operation=$1
    local rpc_url=$2
    local log_file="/tmp/err"
    
    if [ "$operation" == "prepare" ]; then
        run_background "RUST_LOG=debug /home/ubuntu/near-synth-bm create-sub-accounts \
            --rpc-url $rpc_url \
            --signer-key-path /home/ubuntu/.near/validator_key.json \
            --nonce 1000 \
            --sub-account-prefix '2,c,h,m,x' \
            --num-sub-accounts 500 \
            --deposit 953060601875000000010000000 \
            --channel-buffer-size 1200 \
            --requests-per-second 1250 \
            --user-data-dir user-data && \
            echo 'Mode 1 preparation complete'" "$log_file" "mode1"
    else
        run_background "RUST_LOG=info /home/ubuntu/near-synth-bm benchmark-native-transfers \
            --rpc-url $rpc_url \
            --user-data-dir /home/ubuntu/user-data/ \
            --read-nonces-from-network \
            --num-transfers 90000000 \
            --channel-buffer-size 30000 \
            --requests-per-second 3000 \
            --amount 1" "$log_file" "mode1"
    fi
}

# Mode 2: Sweat Benchmark
run_mode_2() {
    local operation=$1
    local rpc_url=$2
    local log_file="/tmp/err"
    
    if [ "$operation" == "prepare" ]; then
        run_background "RUST_LOG=info /home/ubuntu/near-synth-bm \
            benchmark-sweat create-contracts \
            --rpc-url $rpc_url \
            --num-oracles 5 \
            --oracle-deposit 1000000000000000000000000000000 \
            --user-data-dir /home/ubuntu/oracles/ \
            --signer-key-path /home/ubuntu/.near/validator_key.json \
            --wasm-file /home/ubuntu/sweat.wasm \
            --nonce 1000 && \
        RUST_LOG=info /home/ubuntu/near-synth-bm \
            benchmark-sweat create-users \
            --rpc-url $rpc_url \
            --oracle-data-dir /home/ubuntu/oracles/ \
            --users-per-oracle 1000 \
            --user-data-dir /home/ubuntu/users/ \
            --deposit 953060601875000000010000 && \
            echo 'Mode 2 preparation complete'" "$log_file" "mode2"
    else
        run_background "RUST_LOG=info /home/ubuntu/near-synth-bm \
            benchmark-sweat run-benchmark \
            --rpc-url $rpc_url \
            --oracle-data-dir /home/ubuntu/oracles/ \
            --user-data-dir /home/ubuntu/users/ \
            --batch-size 750 \
            --requests-per-second 10 \
            --total-batches 2000000" "$log_file" "mode2"
    fi
}

# Run the specified modes
if [ -n "$MODE" ]; then
    if [ "$MODE" == "1" ]; then
        run_mode_1 "$OPERATION" "$NODE_RPC"
    elif [ "$MODE" == "2" ]; then
        run_mode_2 "$OPERATION" "$NODE_RPC"
    fi
fi

echo "Script started in background. To check status:"
echo "  ps aux | grep near-synth-bm"
echo "To view logs:"
echo "  tail -f /tmp/err"