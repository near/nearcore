#!/bin/bash

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
    echo "=== Status for $node ==="
    gcloud compute ssh ubuntu@"$node" --command="tail -n 5 /tmp/err" 2>/dev/null || \
        echo "Failed to connect to $node"
    echo "----------------------------------------"
}

# Function to check status of all benchmark nodes
check_all_status() {
    while read -r node; do
        check_node_status "$node"
    done < /tmp/benchmark_nodes
}

# Function to run prepare phase with periodic status checks
run_prepare() {
    echo "Starting preparation phase..."
    
    # Start prepare phase on all nodes in parallel
    while read -r node; do
        echo "Starting prepare on $node..."
        gcloud compute ssh ubuntu@"$node" --command="/home/ubuntu/benchmark.sh prepare > /tmp/err 2>&1 &" &
    done < /tmp/benchmark_nodes
    
    echo "Preparation started on all nodes. Monitoring status..."
    
    # Monitor status every 30 seconds until completion or error
    while true; do
        sleep 30
        echo -e "\n=== Status Check at $(date) ==="
        
        all_done=true
        while read -r node; do
            # Check if prepare phase is complete by looking for completion marker or error
            status=$(gcloud compute ssh ubuntu@"$node" --command="tail -n 20 /tmp/err" 2>/dev/null)
            
            if echo "$status" | grep -q "Preparation complete"; then
                echo "$node: Preparation completed successfully"
            elif echo "$status" | grep -q "Error:"; then
                echo "$node: Error detected"
                exit 1
            else
                echo "$node: Still preparing..."
                all_done=false
            fi
        done < /tmp/benchmark_nodes
        
        if $all_done; then
            echo "All nodes have completed preparation!"
            break
        fi
    done
}

# Function to run the benchmark
run_benchmark() {
    echo "Starting benchmark execution..."
    
    # Start benchmark on all nodes in parallel
    while read -r node; do
        echo "Starting benchmark on $node..."
        gcloud compute ssh ubuntu@"$node" --command="/home/ubuntu/benchmark.sh run > /tmp/err 2>&1 &" &
    done < /tmp/benchmark_nodes
    
    echo "Benchmark started on all nodes. Use './validator_manager.sh status' to check progress."
}

# Function to set up RPC URL on nodes
setup_rpc_url() {
    local rpc_host=$1
    echo "Setting up RPC URL on all nodes..."
    
    while read -r node; do
        echo "Configuring RPC URL on $node..."
        # Remove any existing RPC_URL line and add the new one
        gcloud compute ssh ubuntu@"$node" --command="
            sudo sed -i '/^RPC_URL=/d' /etc/environment && \
            sudo sh -c 'echo \"RPC_URL=http://${rpc_host}:3030\" >> /etc/environment'" || \
            echo "Failed to set RPC_URL on $node"
    done < /tmp/benchmark_nodes
    
    echo "RPC URL configuration complete. The setting will be available in all new sessions."
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
        setup_rpc_url "$2"
        run_prepare
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