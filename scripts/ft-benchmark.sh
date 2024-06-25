#!/bin/bash
set -xeo pipefail

# For debugging purposes
whoami
date

# Otherwise nearup and cargo don't work even if installed properly
PATH=/home/ubuntu/.local/bin/:$PATH
export PATH=$PATH:$HOME/.cargo/bin

# Fetch the latest changes from the remote
git fetch

git pull

# some logging improvements
NEW_COMMIT_HASH=$(git rev-parse origin/master)
LOG_DIR=scripts/ft-benchmark-logs
MAIN_LOG_FILE=$LOG_DIR/${NEW_COMMIT_HASH}.log
exec > >(tee -a $MAIN_LOG_FILE) 2>&1
export DATABASE_URL_CLI=postgres://benchmark_runner@34.90.190.128/benchmarks
python3 scripts/run-ft-benchmark.py --user "scheduled_run_on_crt_ft_benchmark"
