#!/bin/bash

# Configuration
LOG_FILE="/tmp/err"
RPC_URL="http://127.0.0.1:3030"  # Default RPC URL, can be overridden
SCRIPT_MODE=""
OPERATION=""

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
            SCRIPT_MODE="$2"
            shift 2
            ;;
        --operation)
            OPERATION="$2"
            shift 2
            ;;
        --rpc-url)
            RPC_URL="$2"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

# Validate inputs
if [[ ! "$SCRIPT_MODE" =~ ^[12]$ ]]; then
    echo "Error: Mode must be 1 or 2"
    usage
fi

if [[ ! "$OPERATION" =~ ^(prepare|run)$ ]]; then
    echo "Error: Operation must be prepare or run"
    usage
fi

# Function to run command in background with logging
run_background() {
    local cmd="$1"
    echo "Running command in background: $cmd"
    eval "$cmd" > "$LOG_FILE" 2>&1 &
    echo "Process ID: $!"
    echo "Logs are being written to $LOG_FILE"
}

# Function to check status
check_status() {
    echo "=== Checking status of background process ==="
    if ! ps -p $1 > /dev/null; then
        echo "Process $1 has finished"
        echo "Last 10 lines of log:"
        tail -n 10 "$LOG_FILE"
        return 1
    else
        echo "Process $1 is still running"
        echo "Last 5 lines of log:"
        tail -n 5 "$LOG_FILE"
        return 0
    fi
}

# Mode 1: Native Transfers
run_mode_1() {
    if [ "$OPERATION" == "prepare" ]; then
        run_background "RUST_LOG=debug /home/ubuntu/near-synth-bm create-sub-accounts \
            --rpc-url $RPC_URL \
            --signer-key-path /home/ubuntu/.near/validator_key.json \
            --nonce 10 \
            --sub-account-prefix '2,c,h,m,x' \
            --num-sub-accounts 500 \
            --deposit 953060601875000000010000000 \
            --channel-buffer-size 1200 \
            --requests-per-second 1250 \
            --user-data-dir user-data && \
            echo 'Preparation complete'"
    else
        run_background "RUST_LOG=info /home/ubuntu/near-synth-bm benchmark-native-transfers \
            --rpc-url $RPC_URL \
            --user-data-dir /home/ubuntu/user-data/ \
            --read-nonces-from-network \
            --num-transfers 90000000 \
            --channel-buffer-size 30000 \
            --requests-per-second 3000 \
            --amount 1"
    fi
}

# Mode 2: Sweat Benchmark
run_mode_2() {
    if [ "$OPERATION" == "prepare" ]; then
        run_background "RUST_LOG=info /home/ubuntu/near-synth-bm \
            benchmark-sweat create-contracts \
            --rpc-url $RPC_URL \
            --num-oracles 5 \
            --oracle-deposit 1000000000000000000000000000000 \
            --user-data-dir /home/ubuntu/oracles/ \
            --signer-key-path /home/ubuntu/.near/validator_key.json \
            --wasm-file /home/ubuntu/sweat.wasm \
            --nonce 10 && \
        RUST_LOG=info /home/ubuntu/near-synth-bm \
            benchmark-sweat create-users \
            --rpc-url $RPC_URL \
            --oracle-data-dir /home/ubuntu/oracles/ \
            --users-per-oracle 1000 \
            --user-data-dir /home/ubuntu/users/ \
            --deposit 953060601875000000010000 && \
            echo 'Preparation complete'"
    else
        run_background "RUST_LOG=info /home/ubuntu/near-synth-bm \
            benchmark-sweat run-benchmark \
            --rpc-url $RPC_URL \
            --oracle-data-dir /home/ubuntu/oracles/ \
            --user-data-dir /home/ubuntu/users/ \
            --batch-size 750 \
            --requests-per-second 10 \
            --total-batches 2000000"
    fi
}

# Select and run the appropriate mode
if [ "$SCRIPT_MODE" == "1" ]; then
    run_mode_1
elif [ "$SCRIPT_MODE" == "2" ]; then
    run_mode_2
fi

# Store the process ID
PID=$!
echo $PID > /tmp/benchmark_pid

echo "Script started in background. To check status:"
echo "  ./$(basename $0) --check-status"
echo "To view logs:"
echo "  tail -f $LOG_FILE"