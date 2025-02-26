#!/bin/bash

# Set default compute zone
export CLOUDSDK_COMPUTE_ZONE=us-central1-a

# Function to list nodes and select validators
select_validators() {
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
    echo "$SELECTED_VALIDATORS" > /tmp/benchmark_nodes
    
    # Show selected validators in a nice format for humans
    echo -e "\nSelected validators:"
    while read -r node; do
        echo "  $node"
    done < /tmp/benchmark_nodes
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
    { read -r node1; read -r node2; } < /tmp/benchmark_nodes
    
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
    echo "Starting preparation phase..."
    
    # Read the two nodes
    { read -r node1; read -r node2; } < /tmp/benchmark_nodes
    
    if [ -z "$node1" ] || [ -z "$node2" ]; then
        echo "Error: Need exactly two nodes for benchmarking"
        exit 1
    fi
    
    # First set up RPC URL on both nodes
    echo "Setting up RPC URL on nodes..."
    for node in "$node1" "$node2"; do
        echo "Configuring RPC URL on $node..."
        gcloud compute ssh ubuntu@"$node" --command="
            sudo sed -i '/^RPC_URL=/d' /etc/environment && \
            sudo sh -c 'echo \"RPC_URL=http://${rpc_host}:3030\" >> /etc/environment'" || \
            echo "Failed to set RPC_URL on $node"
    done
    
    # Start prepare phase on nodes with different modes
    echo "Starting prepare on $node1 (Mode 1)..."
    gcloud compute ssh ubuntu@"$node1" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 1 --operation prepare --rpc-url http://${rpc_host}:3030 > /tmp/err 2>&1 &"
    
    echo "Starting prepare on $node2 (Mode 2)..."
    gcloud compute ssh ubuntu@"$node2" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 2 --operation prepare --rpc-url http://${rpc_host}:3030 > /tmp/err 2>&1 &"
    
    echo "Preparation started on all nodes. Monitoring status..."
    
    # Monitor status every 10 seconds until completion or error
    while true; do
        sleep 10
        echo -e "\n=== Status Check at $(date) ==="
        
        all_done=true
        
        # Check node1 (Mode 1)
        echo "=== Status for $node1 (Mode 1) ==="
        status1=$(gcloud compute ssh ubuntu@"$node1" --command="tail -n 20 /tmp/err" 2>/dev/null)
        echo "$status1"
        echo "----------------------------------------"
        
        # Check node2 (Mode 2)
        echo "=== Status for $node2 (Mode 2) ==="
        status2=$(gcloud compute ssh ubuntu@"$node2" --command="tail -n 20 /tmp/err" 2>/dev/null)
        echo "$status2"
        echo "----------------------------------------"
        
        # Check completion status for both nodes
        if ! echo "$status1" | grep -q "preparation complete"; then
            echo "$node1: Still preparing Mode 1..."
            all_done=false
        fi
        
        if ! echo "$status2" | grep -q "preparation complete"; then
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
    echo "Starting benchmark execution..."
    
    # Read the two nodes
    { read -r node1; read -r node2; } < /tmp/benchmark_nodes
    
    if [ -z "$node1" ] || [ -z "$node2" ]; then
        echo "Error: Need exactly two nodes for benchmarking"
        exit 1
    fi
    
    # Start different modes on different nodes
    echo "Starting Mode 1 benchmark on $node1..."
    gcloud compute ssh ubuntu@"$node1" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 1 --operation run --rpc-url \$RPC_URL > /tmp/err 2>&1 &"
    
    echo "Starting Mode 2 benchmark on $node2..."
    gcloud compute ssh ubuntu@"$node2" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 2 --operation run --rpc-url \$RPC_URL > /tmp/err 2>&1 &"
    
    echo "Benchmark started on all nodes. Use './validator_manager.sh status' to check progress."
}

# Main execution
case "$1" in
    "select")
        select_validators
        ;;
    "prepare")
        if [ ! -f /tmp/benchmark_nodes ]; then
            echo "Error: No benchmark nodes selected. Run select command first."
            exit 1
        fi
        if [ -z "$2" ]; then
            echo "Error: RPC host is required for prepare command"
            echo "Usage: $0 prepare <rpc_host>"
            echo "Example: $0 prepare 35.226.42.192"
            exit 1
        fi
        run_prepare "$2"
        ;;
    "run")
        if [ ! -f /tmp/benchmark_nodes ]; then
            echo "Error: No benchmark nodes selected. Run select command first."
            exit 1
        fi
        run_benchmark
        ;;
    "status")
        if [ ! -f /tmp/benchmark_nodes ]; then
            echo "Error: No benchmark nodes selected. Run select command first."
            exit 1
        fi
        check_all_status
        ;;
    *)
        echo "Usage: $0 {prepare <rpc_host>|run|status}"
        echo "  prepare <rpc_host>: Set up RPC URL and start preparation phase"
        echo "  run: Start the benchmark execution"
        echo "  status: Check current status of all nodes"
        exit 1
        ;;
esac