#!/usr/bin/env bash
set -euo pipefail

# 0. Get record duration (default: 10s)
record_secs="${1:-10}"
mode="${2:-simple}"

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

# normal perf data
gather_simple_perf() {
  # 4. Record perf
  echo "▶️ Recording perf data for PID $pid for ${record_secs}s..."
  perf record -e cpu-clock -F 1000 -g --call-graph dwarf,65528 \
      -p "$pid" -o "perf.data" -- sleep "${record_secs}"

  # 5. Convert to script
  output_file="${HOME}/perf-$(hostname).script"
  echo "📜 Converting perf.data to ${output_file}"
  perf script -F +pid -i "perf.data" > "${output_file}" 2>/dev/null

  # 6. Compress
  gzip -f "${output_file}"
  echo "✅ Done: ${output_file}.gz"
}

# extended perf data
gather_extended_perf() {
  sudo sysctl kernel.sched_schedstats=1
  
  output_dir="./perf-data"
  mkdir -p ${output_dir}
  
  perf stat \
    -p ${pid} \
    --per-thread \
    -- sleep "${record_secs}" &> "${output_dir}/perf-stat.out"

  perf record -e cpu-clock -F1000 \
    -g --call-graph fp,65528 \
    -p ${pid} -o perf-on-cpu.data -- sleep "${record_secs}"
  nice -n 19 perf script -F +pid -i perf-on-cpu.data > "${output_dir}/perf-on-cpu.script"

  sudo perf record \
    -e sched:sched_stat_sleep -e sched:sched_switch -e sched:sched_process_exit \
    -g --call-graph fp,65528 \
    -p ${pid} \
    -o perf-off-cpu.data -- sleep "${record_secs}"
  sudo chown ${USER} perf-off-cpu.data
  nice -n 19 perf script -F +pid,+comm,+tid,+time,+ip,+sym -i perf-off-cpu.data > ${output_dir}/off-cpu-profile.script

  nice -n 19 tar czf perf-data.tar.gz "${output_dir}"
}

if [ "$mode" = "extended" ]; then
  gather_extended_perf
else
  gather_simple_perf
fi
