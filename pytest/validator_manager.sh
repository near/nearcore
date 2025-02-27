#!/bin/bash

# Set default compute zone
# export CLOUDSDK_COMPUTE_ZONE=europe-west4-a

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
    # Get the list of nodes using gcloud compute instances list
    # Filter for nodes that include $UNIQUE_ID in the name
    ALL_NODES_INFO=$(gcloud compute instances list | cat | grep -i "$UNIQUE_ID")
    ALL_NODES=$(echo "$ALL_NODES_INFO" | awk '{print $1}')
    
    if [ -z "$ALL_NODES" ]; then
        echo "Error: No nodes found with UNIQUE_ID: $UNIQUE_ID"
        exit 1
    fi
    
    # Display all available nodes
    echo "$ALL_NODES"
    
    # Find the RPC node (dumper)
    RPC_NODE_LINE=$(echo "$ALL_NODES_INFO" | grep "dumper" | head -n 1)
    RPC_NODE=$(echo "$RPC_NODE_LINE" | awk '{print $1}')
    RPC_ZONE=$(echo "$RPC_NODE_LINE" | awk '{print $2}')
    
    if [ -z "$RPC_NODE" ]; then
        echo "Warning: No dumper node found for RPC. Will use a random node instead."
        RPC_NODE_LINE=$(echo "$ALL_NODES_INFO" | head -n 1)
        RPC_NODE=$(echo "$RPC_NODE_LINE" | awk '{print $1}')
        RPC_ZONE=$(echo "$RPC_NODE_LINE" | awk '{print $2}')
    fi
    
    # Find validator nodes (not archiv, not dumper, not traffic)
    VALIDATOR_LINES=$(echo "$ALL_NODES_INFO" | grep -v "archiv" | grep -v "dumper" | grep -v "traffic" | shuf | head -n 2)
    
    if [ -z "$VALIDATOR_LINES" ]; then
        echo "Error: No suitable validator nodes found"
        exit 1
    fi
    
    # Extract node names and zones
    VALIDATOR1=$(echo "$VALIDATOR_LINES" | head -n 1 | awk '{print $1}')
    VALIDATOR1_ZONE=$(echo "$VALIDATOR_LINES" | head -n 1 | awk '{print $2}')
    
    VALIDATOR2=$(echo "$VALIDATOR_LINES" | tail -n 1 | awk '{print $1}')
    VALIDATOR2_ZONE=$(echo "$VALIDATOR_LINES" | tail -n 1 | awk '{print $2}')
    
    # Save selected validators and RPC node for future reference
    # Format: node_name|zone
    echo "${VALIDATOR1}|${VALIDATOR1_ZONE}" > "/tmp/benchmark_nodes_${UNIQUE_ID}"
    echo "${VALIDATOR2}|${VALIDATOR2_ZONE}" >> "/tmp/benchmark_nodes_${UNIQUE_ID}"
    echo "${RPC_NODE}|${RPC_ZONE}" > "/tmp/benchmark_rpc_${UNIQUE_ID}"
    
    # Show selected validators in a nice format for humans
    echo -e "\nSelected validators:"
    echo "  $VALIDATOR1 (Zone: $VALIDATOR1_ZONE)"
    echo "  $VALIDATOR2 (Zone: $VALIDATOR2_ZONE)"
    
    echo -e "\nSelected RPC node:"
    echo "  $RPC_NODE (Zone: $RPC_ZONE)"
}

# Function to check benchmark status on a specific node
check_node_status() {
    local node_info=$1
    local mode=$2
    
    # Extract node name and zone
    local node=$(echo "$node_info" | cut -d'|' -f1)
    local zone=$(echo "$node_info" | cut -d'|' -f2)
    
    echo "=== Status for $node (Mode $mode, Zone: $zone) ==="
    gcloud compute ssh ubuntu@"$node" --zone="$zone" --command="tail -n 5 /tmp/err" 2>/dev/null || \
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
    
    { read -r node1_info; read -r node2_info; } < "$nodes_file"
    
    if [ -n "$node1_info" ]; then
        check_node_status "$node1_info" "1"
    fi
    if [ -n "$node2_info" ]; then
        check_node_status "$node2_info" "2"
    fi
}

# Function to run prepare phase with periodic status checks
run_prepare() {
    local nonce=$1
    
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    local nodes_file="/tmp/benchmark_nodes_${UNIQUE_ID}"
    local rpc_file="/tmp/benchmark_rpc_${UNIQUE_ID}"
    
    if [ ! -f "$nodes_file" ] || [ ! -f "$rpc_file" ]; then
        echo "Error: Missing benchmark nodes or RPC node files. Run select command first."
        exit 1
    fi
    
    # Read the RPC node
    local rpc_info=$(cat "$rpc_file")
    local rpc_node=$(echo "$rpc_info" | cut -d'|' -f1)
    local rpc_zone=$(echo "$rpc_info" | cut -d'|' -f2)
    
    # Get the RPC host IP
    local rpc_host=$(gcloud compute instances list | cat | grep "$rpc_node" | awk '{print $5}')
    
    if [ -z "$rpc_host" ]; then
        echo "Error: Could not get IP address for RPC node $rpc_node"
        exit 1
    fi
    
    echo "Starting preparation phase..."
    echo "Network name: $UNIQUE_ID"
    echo "RPC node: $rpc_node ($rpc_host, Zone: $rpc_zone)"
    
    # Read the two nodes
    { read -r node1_info; read -r node2_info; } < "$nodes_file"
    
    # Extract node names and zones
    local node1=$(echo "$node1_info" | cut -d'|' -f1)
    local zone1=$(echo "$node1_info" | cut -d'|' -f2)
    local node2=$(echo "$node2_info" | cut -d'|' -f1)
    local zone2=$(echo "$node2_info" | cut -d'|' -f2)
    
    if [ -z "$node1" ] || [ -z "$node2" ]; then
        echo "Error: Need exactly two nodes for benchmarking"
        exit 1
    fi
    
    # Extract short names for SSH connections
    local short_name1=$(echo "$node1" | cut -d'.' -f1)
    local short_name2=$(echo "$node2" | cut -d'.' -f1)
    
    # Get all validator nodes that are ready (for fallback)
    local all_validators_info=$(gcloud compute instances list | cat | grep -i "$UNIQUE_ID" | grep -v "archiv" | grep -v "dumper" | grep -v "traffic")
    local all_validators=$(echo "$all_validators_info" | awk '{print $1}')
    
    # First set up RPC URL on both nodes
    echo "Setting up RPC URL on nodes..."
    
    # Function to set up environment on a node
    setup_node_env() {
        local node=$1
        local short_name=$2
        local zone=$3
        local node_num=$4
        
        echo "Configuring RPC URL on $node (as $short_name, Zone: $zone)..."
        gcloud compute ssh ubuntu@"$short_name" --zone="$zone" --command="
            sudo sed -i '/^RPC_URL=/d' /etc/environment && \
            sudo sed -i '/^UNIQUE_ID=/d' /etc/environment && \
            sudo sh -c 'echo \"RPC_URL=http://${rpc_host}:3030\" >> /etc/environment' && \
            sudo sh -c 'echo \"UNIQUE_ID=${UNIQUE_ID}\" >> /etc/environment'" 2>/dev/null
        
        # If direct connection failed, try to find the node in the validator list
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name, trying to find matching validator..."
            while read -r validator_line; do
                local validator=$(echo "$validator_line" | awk '{print $1}')
                local validator_zone=$(echo "$validator_line" | awk '{print $2}')
                
                if [[ "$validator" == *"$node"* || "$node" == *"$validator"* ]]; then
                    echo "Found matching validator: $validator (Zone: $validator_zone), trying to connect..."
                    gcloud compute ssh ubuntu@"$validator" --zone="$validator_zone" --command="
                        sudo sed -i '/^RPC_URL=/d' /etc/environment && \
                        sudo sed -i '/^UNIQUE_ID=/d' /etc/environment && \
                        sudo sh -c 'echo \"RPC_URL=http://${rpc_host}:3030\" >> /etc/environment' && \
                        sudo sh -c 'echo \"UNIQUE_ID=${UNIQUE_ID}\" >> /etc/environment'" 2>/dev/null && \
                        echo "Successfully configured $validator" && \
                        # Update the node name for future use
                        if [ "$node_num" -eq 1 ]; then
                            node1="$validator"
                            short_name1="$validator"
                            zone1="$validator_zone"
                        else
                            node2="$validator"
                            short_name2="$validator"
                            zone2="$validator_zone"
                        fi && \
                        break || \
                        echo "Failed to connect to $validator"
                fi
            done <<< "$all_validators_info"
        fi
    }
    
    # Set up environment on both nodes
    setup_node_env "$node1" "$short_name1" "$zone1" 1
    setup_node_env "$node2" "$short_name2" "$zone2" 2
    
    # Prepare nonce argument if provided
    local nonce_arg=""
    if [ -n "$nonce" ]; then
        nonce_arg="--nonce $nonce"
    fi
    
    # Start prepare phase on nodes with different modes
    echo "Starting prepare on $node1 (Mode 1, Zone: $zone1)..."
    gcloud compute ssh ubuntu@"$short_name1" --zone="$zone1" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 1 --operation prepare --rpc-url http://${rpc_host}:3030 $nonce_arg > /tmp/err 2>&1 &" 2>/dev/null || \
        echo "Failed to start prepare on $short_name1"
    
    echo "Starting prepare on $node2 (Mode 2, Zone: $zone2)..."
    gcloud compute ssh ubuntu@"$short_name2" --zone="$zone2" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode 2 --operation prepare --rpc-url http://${rpc_host}:3030 $nonce_arg > /tmp/err 2>&1 &" 2>/dev/null || \
        echo "Failed to start prepare on $short_name2"
    
    echo "Preparation started on all nodes. Monitoring status..."
    
    # Monitor status every 10 seconds until completion or error
    while true; do
        sleep 10
        echo -e "\n=== Status Check at $(date) ==="
        
        all_done=true
        
        # Check node1 (Mode 1)
        echo "=== Status for $node1 (Mode 1, Zone: $zone1) ==="
        status1=$(gcloud compute ssh ubuntu@"$short_name1" --zone="$zone1" --command="tail -n 20 /tmp/err" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name1"
            status1=""
        else
            echo "$status1"
        fi
        echo "----------------------------------------"
        
        # Check node2 (Mode 2)
        echo "=== Status for $node2 (Mode 2, Zone: $zone2) ==="
        status2=$(gcloud compute ssh ubuntu@"$short_name2" --zone="$zone2" --command="tail -n 20 /tmp/err" 2>/dev/null)
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
    # Check if UNIQUE_ID is set
    if [ -z "$UNIQUE_ID" ]; then
        echo "Error: UNIQUE_ID environment variable is not set"
        echo "Please set it with: export UNIQUE_ID=<network_name>"
        exit 1
    fi
    
    local nodes_file="/tmp/benchmark_nodes_${UNIQUE_ID}"
    local rpc_file="/tmp/benchmark_rpc_${UNIQUE_ID}"
    
    if [ ! -f "$nodes_file" ] || [ ! -f "$rpc_file" ]; then
        echo "Error: Missing benchmark nodes or RPC node files. Run select command first."
        exit 1
    fi
    
    # Read the RPC node
    local rpc_info=$(cat "$rpc_file")
    local rpc_node=$(echo "$rpc_info" | cut -d'|' -f1)
    local rpc_zone=$(echo "$rpc_info" | cut -d'|' -f2)
    
    # Get the RPC host IP
    local rpc_host=$(gcloud compute instances list | cat | grep "$rpc_node" | awk '{print $5}')
    
    if [ -z "$rpc_host" ]; then
        echo "Error: Could not get IP address for RPC node $rpc_node"
        exit 1
    fi
    
    echo "Starting benchmark execution..."
    echo "Network name: $UNIQUE_ID"
    echo "RPC node: $rpc_node ($rpc_host, Zone: $rpc_zone)"
    
    # Read the two nodes
    { read -r node1_info; read -r node2_info; } < "$nodes_file"
    
    # Extract node names and zones
    local node1=$(echo "$node1_info" | cut -d'|' -f1)
    local zone1=$(echo "$node1_info" | cut -d'|' -f2)
    local node2=$(echo "$node2_info" | cut -d'|' -f1)
    local zone2=$(echo "$node2_info" | cut -d'|' -f2)
    
    if [ -z "$node1" ] || [ -z "$node2" ]; then
        echo "Error: Need exactly two nodes for benchmarking"
        exit 1
    fi
    
    # Extract short names for SSH connections
    local short_name1=$(echo "$node1" | cut -d'.' -f1)
    local short_name2=$(echo "$node2" | cut -d'.' -f1)
    
    # Get all validator nodes that are ready (for fallback)
    local all_validators_info=$(gcloud compute instances list | cat | grep -i "$UNIQUE_ID" | grep -v "archiv" | grep -v "dumper" | grep -v "traffic")
    
    # Function to start benchmark on a node
    start_benchmark() {
        local node=$1
        local short_name=$2
        local zone=$3
        local mode=$4
        
        echo "Starting Mode $mode benchmark on $node (as $short_name, Zone: $zone)..."
        gcloud compute ssh ubuntu@"$short_name" --zone="$zone" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode $mode --operation run --rpc-url http://${rpc_host}:3030 > /tmp/err 2>&1 &" 2>/dev/null
        
        # If direct connection failed, try to find the node in the validator list
        if [ $? -ne 0 ]; then
            echo "Failed to connect to $short_name, trying to find matching validator..."
            while read -r validator_line; do
                local validator=$(echo "$validator_line" | awk '{print $1}')
                local validator_zone=$(echo "$validator_line" | awk '{print $2}')
                
                if [[ "$validator" == *"$node"* || "$node" == *"$validator"* ]]; then
                    echo "Found matching validator: $validator (Zone: $validator_zone), trying to connect..."
                    gcloud compute ssh ubuntu@"$validator" --zone="$validator_zone" --command="source /etc/environment && /home/ubuntu/benchmark.sh --mode $mode --operation run --rpc-url http://${rpc_host}:3030 > /tmp/err 2>&1 &" 2>/dev/null && \
                        echo "Successfully started benchmark on $validator" || \
                        echo "Failed to connect to $validator"
                    break
                fi
            done <<< "$all_validators_info"
        fi
    }
    
    # Start different modes on different nodes
    start_benchmark "$node1" "$short_name1" "$zone1" 1
    start_benchmark "$node2" "$short_name2" "$zone2" 2
    
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
    { read -r node1_info; read -r node2_info; } < "$nodes_file"
    
    if [ -z "$node1_info" ] && [ -z "$node2_info" ]; then
        echo "No benchmark nodes found in $nodes_file"
        exit 1
    fi
    
    # Extract node names and zones
    if [ -n "$node1_info" ]; then
        local node1=$(echo "$node1_info" | cut -d'|' -f1)
        local zone1=$(echo "$node1_info" | cut -d'|' -f2)
        
        echo "Stopping processes on $node1 (Zone: $zone1)..."
        gcloud compute ssh ubuntu@"$node1" --zone="$zone1" --command="/home/ubuntu/benchmark.sh --operation stop" 2>/dev/null || \
            echo "Failed to connect to $node1"
    fi
    
    if [ -n "$node2_info" ]; then
        local node2=$(echo "$node2_info" | cut -d'|' -f1)
        local zone2=$(echo "$node2_info" | cut -d'|' -f2)
        
        echo "Stopping processes on $node2 (Zone: $zone2)..."
        gcloud compute ssh ubuntu@"$node2" --zone="$zone2" --command="/home/ubuntu/benchmark.sh --operation stop" 2>/dev/null || \
            echo "Failed to connect to $node2"
    fi
    
    echo "Benchmark processes stopped on nodes."
}

# Main execution
case "$1" in
    "select")
        select_validators
        ;;
    "prepare")
        # Check if the nodes file exists for this network
        if [ ! -f "/tmp/benchmark_nodes_${UNIQUE_ID}" ]; then
            echo "Error: No benchmark nodes selected for network $UNIQUE_ID. Run select command first."
            exit 1
        fi
        
        run_prepare "$2"  # $2 is now optional nonce
        ;;
    "run")
        # Check if the nodes file exists for this network
        if [ ! -f "/tmp/benchmark_nodes_${UNIQUE_ID}" ]; then
            echo "Error: No benchmark nodes selected for network $UNIQUE_ID. Run select command first."
            exit 1
        fi
        
        run_benchmark
        ;;
    "status")
        check_all_status
        ;;
    "stop")
        stop_benchmark
        ;;
    *)
        echo "Usage: $0 {select|prepare [nonce]|run|status|stop}"
        echo "  select: Select validator nodes for benchmarking based on UNIQUE_ID"
        echo "  prepare [nonce]: Start preparation phase with optional nonce"
        echo "  run: Start the benchmark execution"
        echo "  status: Check current status of all nodes"
        echo "  stop: Stop all benchmark processes on nodes"
        echo ""
        echo "Before running any command, set the UNIQUE_ID environment variable:"
        echo "  export UNIQUE_ID=<network_name>"
        echo "Example: export UNIQUE_ID=hoptnet"
        exit 1
        ;;
esac