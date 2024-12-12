#!/bin/bash

# This script sets specific sysctl parameters for running a validator.
# Run it as: sudo ./set_kernel_params.sh

# Increase maximum read and write buffer sizes
sysctl -w net.core.rmem_max=8388608
sysctl -w net.core.wmem_max=8388608

# Configure TCP read and write memory parameters
sysctl -w net.ipv4.tcp_rmem="4096 87380 8388608"
sysctl -w net.ipv4.tcp_wmem="4096 16384 8388608"

# Disable slow start after idle
sysctl -w net.ipv4.tcp_slow_start_after_idle=0

echo "Network settings have been updated."
