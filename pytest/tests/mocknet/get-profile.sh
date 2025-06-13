#!/usr/bin/env bash
set -euo pipefail

# 0. Get record duration (default: 10s)
record_secs="${1:-10}"

# 1. Remove old files
rm -f "$HOME/perf."*

# 2. Allow perf to access necessary data
# cspell:ignore kptr
sudo sysctl -w kernel.perf_event_paranoid=0
sudo sysctl -w kernel.kptr_restrict=0

# 3. Get PID of neard
pid=$(pgrep neard)
if [ -z "$pid" ]; then
  echo "❌ neard process not found."
  exit 1
fi

# 4. Record perf
echo "▶️ Recording perf data for PID $pid for ${record_secs}s..."
perf record -e cpu-clock -F 1000 -g --call-graph dwarf,65528 \
  -p "$pid" -o "$HOME/perf.data" -- sleep "$record_secs" 2>/dev/null

# 5. Convert to script
echo "📜 Converting perf.data to perf.script..."
perf script -F +pid -i "$HOME/perf.data" > "$HOME/perf.script" 2>/dev/null

# 6. Compress
gzip -f "$HOME/perf.script"
echo "✅ Done: $HOME/perf.script.gz"
