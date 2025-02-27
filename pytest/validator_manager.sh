#!/bin/bash

# Set default compute zone
export CLOUDSDK_COMPUTE_ZONE=us-central1-a

# Function to list nodes and select validators
select_validators() {
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    echo "Network name: $UNIQUE_ID"
    
    echo "Available nodes:"
    # Get the list of nodes, filter for ready validators, exclude archival nodes, and extract just the node names
    SELECTED_VALIDATORS=$($MIRROR_CMD list-nodes | \
        grep "^validator" | \
        grep "|ready$" | \
        grep -v "archiv" | \
        cut -d'|' -f2 | \
        head -n 2)
    
    if [ -z "$SELECTED_VALIDATORS" ]; then
        echo "Error: No ready validator nodes found"
        exit 1
    fi

    # Save selected validators for future reference (just the node names, one per line)
    echo "$SELECTED_VALIDATORS" > "/tmp/benchmark_nodes_${UNIQUE_ID}"
    
    # Show selected validators in a nice format for humans
    echo -e "\nSelected validators:"
    while read -r node; do
        echo "  $node"
    done < "/tmp/benchmark_nodes_${UNIQUE_ID}"
}

# Function to check benchmark status on a specific node
check_node_status() {
    local node=$1
    local mode=$2
    echo "=== Status for $node (Mode $mode) ==="
    gcloud compute ssh ubuntu@"$node" --command="tail -n 5 /tmp/err" 2>/dev/null || \
        echo "Failed to connect to $node"
    echo "----------------------------------------"
}

# Function to check status of all benchmark nodes
check_all_status() {
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    local nodes_file="/tmp/benchmark_nodes_${UNIQUE_ID}"
    
    if [ ! -f "$nodes_file" ]; then
        echo "Error: No benchmark nodes file found at $nodes_file"
        exit 1
    fi
    
    { read -r node1; read -r node2; } < "$nodes_file"
    
    if [ -n "$node1" ]; then
        check_node_status "$node1" "1"
    fi
    if [ -n "$node2" ]; then
        check_node_status "$node2" "2"
    fi
}

# Function to run prepare phase with periodic status checks
run_prepare() {
    local rpc_host=$1
    local nonce=$2
    
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    local nodes_file="/tmp/benchmark_nodes_${UNIQUE_ID}"
    
    echo "Starting preparation phase..."
    echo "Network name: $UNIQUE_ID"
    
    # Read the two nodes
    { read -r node1; read -r node2; } < "$nodes_file"
    
    if [ -z "$node1" ] || [ -z "$node2" ]; then
        echo "Error: Need exactly two nodes for benchmarking"
        exit 1
    fi
    
    # Extract short names for SSH connections
    local short_name1=$(echo "$node1" | cut -d'.' -f1)
    local short_name2=$(echo "$node2" | cut -d'.' -f1)
    
    # Get all validator nodes that are ready (for fallback)
    local all_validators=$($MIRROR_CMD list-nodes | grep "^validator" | grep "|ready$" | cut -d'|' -f2)
    
    # First set up RPC URL on both nodes
    echo "Setting up RPC URL on nodes..."
    
    # Function to set up environment on a node
    setup_node_env() {
        local node=$1
        local short_name=$2
        local node_num=$3
        
        echo "Configuring RPC URL on $node (as $short_name)..."
        gcloud compute ssh ubuntu@"$short_name" --command="
            sudo sed -i '/^RPC_URL=/d' /etc/environment && \
            sudo sed -i '/^UNIQUE_ID=/d' /etc/environment && \
            sudo sh -c 'echo \"RPC_URL=http://${rpc_host}:3030\" >> /etc/environment' && \
            sudo sh -c 'echo \"UNIQUE_ID=${UNIQUE_ID}\" >> /etc/environment'" 2>/dev/null
        
        # If direct connection failed, try to find the node in the validator list
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name, trying to find matching validator..."
            for validator in $all_validators; do
                if [[ "$validator" == *"$node"* || "$node" == *"$validator"* ]]; then
                    echo "Found matching validator: $validator, trying to connect..."
                    gcloud compute ssh ubuntu@"$validator" --command="
                        sudo sed -i '/^RPC_URL=/d' /etc/environment && \
                        sudo sed -i '/^UNIQUE_ID=/d' /etc/environment && \
                        sudo sh -c 'echo \"RPC_URL=http://${rpc_host}:3030\" >> /etc/environment' && \
                        sudo sh -c 'echo \"UNIQUE_ID=${UNIQUE_ID}\" >> /etc/environment'" 2>/dev/null && \
                        echo "Successfully configured $validator" && \
                        # Update the node name for future use
                        if [ "$node_num" -eq 1 ]; then
                            node1="$validator"
                            short_name1="$validator"
                        else
                            node2="$validator"
                            short_name2="$validator"
                        fi && \
                        break || \
                        echo "Failed to connect to $validator"
                fi
            done
        fi
    }
    
    # Set up environment on both nodes
    setup_node_env "$node1" "$short_name1" 1
    setup_node_env "$node2" "$short_name2" 2
    
    # Prepare nonce argument if provided
    local nonce_arg=""
    if [ -n "$nonce" ]; then
        nonce_arg="--nonce $nonce"
    fi
    
    # Start prepare phase on nodes with different modes
    echo "Starting prepare on $node1 (Mode 1)..."
    gcloud compute ssh ubuntu@"$short_name1" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 1 --operation prepare --rpc-url http://${rpc_host}:3030 $nonce_arg > /tmp/err 2>&1 &" 2>/dev/null || \
        echo "Failed to start prepare on $short_name1"
    
    echo "Starting prepare on $node2 (Mode 2)..."
    gcloud compute ssh ubuntu@"$short_name2" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 2 --operation prepare --rpc-url http://${rpc_host}:3030 $nonce_arg > /tmp/err 2>&1 &" 2>/dev/null || \
        echo "Failed to start prepare on $short_name2"
    
    echo "Preparation started on all nodes. Monitoring status..."
    
    # Monitor status every 10 seconds until completion or error
    while true; do
        sleep 10
        echo -e "\n=== Status Check at $(date) ==="
        
        all_done=true
        
        # Check node1 (Mode 1)
        echo "=== Status for $node1 (Mode 1) ==="
        status1=$(gcloud compute ssh ubuntu@"$short_name1" --command="tail -n 20 /tmp/err" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name1"
            status1=""
        else
            echo "$status1"
        fi
        echo "----------------------------------------"
        
        # Check node2 (Mode 2)
        echo "=== Status for $node2 (Mode 2) ==="
        status2=$(gcloud compute ssh ubuntu@"$short_name2" --command="tail -n 20 /tmp/err" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name2"
            status2=""
        else
            echo "$status2"
        fi
        echo "----------------------------------------"
        
        # Check completion status for both nodes
        if [ -z "$status1" ] || ! echo "$status1" | grep -q "preparation complete"; then
            echo "$node1: Still preparing Mode 1..."
            all_done=false
        fi
        
        if [ -z "$status2" ] || ! echo "$status2" | grep -q "preparation complete"; then
            echo "$node2: Still preparing Mode 2..."
            all_done=false
        fi
        
        # Check for errors
        if echo "$status1$status2" | grep -q "RUST_BACKTRACE"; then
            echo "Error detected in one of the nodes"
            exit 1
        fi
        
        if $all_done; then
            echo "All nodes have completed preparation!"
            break
        fi
    done
}

# Function to run the benchmark
run_benchmark() {
    local rpc_host=$1
    
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    local nodes_file="/tmp/benchmark_nodes_${UNIQUE_ID}"
    
    echo "Starting benchmark execution..."
    echo "Network name: $UNIQUE_ID"
    
    # Read the two nodes
    { read -r node1; read -r node2; } < "$nodes_file"
    
    if [ -z "$node1" ] || [ -z "$node2" ]; then
        echo "Error: Need exactly two nodes for benchmarking"
        exit 1
    fi
    
    # Extract short names for SSH connections
    local short_name1=$(echo "$node1" | cut -d'.' -f1)
    local short_name2=$(echo "$node2" | cut -d'.' -f1)
    
    # Get all validator nodes that are ready (for fallback)
    local all_validators=$($MIRROR_CMD list-nodes | grep "^validator" | grep "|ready$" | cut -d'|' -f2)
    
    # Function to start benchmark on a node
    start_benchmark() {
        local node=$1
        local short_name=$2
        local mode=$3
        
        echo "Starting Mode $mode benchmark on $node (as $short_name)..."
        gcloud compute ssh ubuntu@"$short_name" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode $mode --operation run --rpc-url \$RPC_URL > /tmp/err 2>&1 &" 2>/dev/null
        
        # If direct connection failed, try to find the node in the validator list
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name, trying to find matching validator..."
            for validator in $all_validators; do
                if [[ "$validator" == *"$node"* || "$node" == *"$validator"* ]]; then
                    echo "Found matching validator: $validator, trying to connect..."
                    gcloud compute ssh ubuntu@"$validator" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode $mode --operation run --rpc-url \$RPC_URL > /tmp/err 2>&1 &" 2>/dev/null && \
                        echo "Successfully started benchmark on $validator" || \
                        echo "Failed to connect to $validator"
                    break
                fi
            done
        fi
    }
    
    # Start different modes on different nodes
    start_benchmark "$node1" "$short_name1" 1
    start_benchmark "$node2" "$short_name2" 2
    
    echo "Benchmark started on all nodes. Use './validator_manager.sh status' to check progress."
}

# Function to stop the benchmark
stop_benchmark() {
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    local nodes_file="/tmp/benchmark_nodes_${UNIQUE_ID}"
    
    echo "Stopping benchmark processes..."
    echo "Network name: $UNIQUE_ID"
    
    # Read the two nodes
    { read -r node1; read -r node2; } < "$nodes_file"
    
    if [ -z "$node1" ] && [ -z "$node2" ]; then
        echo "No benchmark nodes found in $nodes_file"
        exit 1
    fi
    
    # Stop processes on both nodes by calling benchmark.sh stop
    if [ -n "$node1" ]; then
        echo "Stopping processes on $node1..."
        # Try to use benchmark.sh stop first
        gcloud compute ssh ubuntu@"$node1" --command="/home/ubuntu/benchmark.sh --operation stop" 2>/dev/null
        
        # If that fails, try direct pkill as fallback
        if [ $? -ne 0 ]; then
            echo "Failed to run benchmark.sh stop on $node1, trying direct pkill..."
            gcloud compute ssh ubuntu@"$node1" --command="pkill -f near-synth-bm || true" 2>/dev/null || \
                echo "Failed to connect to $node1"
        fi
    fi
    
    if [ -n "$node2" ]; then
        echo "Stopping processes on $node2..."
        # Try to use benchmark.sh stop first
        gcloud compute ssh ubuntu@"$node2" --command="/home/ubuntu/benchmark.sh --operation stop" 2>/dev/null
        
        # If that fails, try direct pkill as fallback
        if [ $? -ne 0 ]; then
            echo "Failed to run benchmark.sh stop on $node2, trying direct pkill..."
            gcloud compute ssh ubuntu@"$node2" --command="pkill -f near-synth-bm || true" 2>/dev/null || \
                echo "Failed to connect to $node2"
        fi
    fi
    
    echo "Benchmark processes stopped on nodes."
}

# Main execution
case "$1" in
    "select")
        select_validators "$2"
        ;;
    "prepare")
        if [ -z "$2" ]; then
            echo "Error: RPC host is required for prepare command"
            echo "Usage: $0 prepare <rpc_host> [nonce]"
            echo "Example: $0 prepare 35.226.42.192 100"
            exit 1
        fi
        
        # Check if the nodes file exists for this network
        if [ ! -f "/tmp/benchmark_nodes_${UNIQUE_ID}" ]; then
            echo "Error: No benchmark nodes selected for network $UNIQUE_ID. Run select command first."
            exit 1
        fi
        
        run_prepare "$2" "$3"
        ;;
    "run")
        if [ -z "$2" ]; then
            echo "Error: RPC host is required for run command"
            echo "Usage: $0 run <rpc_host>"
            exit 1
        fi
        
        # Check if the nodes file exists for this network
        if [ ! -f "/tmp/benchmark_nodes_${UNIQUE_ID}" ]; then
            echo "Error: No benchmark nodes selected for network $UNIQUE_ID. Run select command first."
            exit 1
        fi
        
        run_benchmark "$2"
        ;;
    "status")
        check_all_status
        ;;
    "stop")
        stop_benchmark
        ;;
    *)
        echo "Usage: $0 {select [rpc_host]|prepare <rpc_host> [nonce]|run <rpc_host>|status|stop}"
        echo "  select [rpc_host]: Select validator nodes for benchmarking"
        echo "  prepare <rpc_host> [nonce]: Set up RPC URL and start preparation phase"
        echo "  run <rpc_host>: Start the benchmark execution"
        echo "  status: Check current status of all nodes"
        echo "  stop: Stop all benchmark processes on nodes"
        exit 1
        ;;
esac