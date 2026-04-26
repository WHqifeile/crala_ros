#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="/tmp/autoware_l3"
SYSTEM_PID_FILE="${RUN_DIR}/system_available.pid"
AUTOMATION_PID_FILE="${RUN_DIR}/automation_request.pid"

set +u
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash >/dev/null 2>&1
set -u

ros2_cli() {
  if ros2 --help 2>&1 | grep -q -- '--no-daemon'; then
    ros2 --no-daemon "$@"
  else
    ros2 "$@"
  fi
}

cleanup_pid_file() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
    rm -f "${pid_file}"
  fi
}

echo "[L3] stopping background keepalive publishers"
cleanup_pid_file "${SYSTEM_PID_FILE}"
cleanup_pid_file "${AUTOMATION_PID_FILE}"

echo "[L3] clearing scenario flags"
timeout 3s ros2 topic pub --once /scenario/system_available std_msgs/msg/Bool '{data: false}' >/dev/null || true
timeout 3s ros2 topic pub --once /scenario/automation_request std_msgs/msg/Bool '{data: false}' >/dev/null || true
timeout 3s ros2 topic pub --once /scenario/td_request std_msgs/msg/Bool '{data: false}' >/dev/null || true
timeout 3s ros2 topic pub --once /scenario/force_mrm std_msgs/msg/Bool '{data: false}' >/dev/null || true
timeout 3s ros2 topic pub --once /scenario/reset_mrm std_msgs/msg/Bool '{data: true}' >/dev/null || true

echo "[L3] requested exit/reset"
