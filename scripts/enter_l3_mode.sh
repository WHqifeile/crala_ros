#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="/tmp/autoware_l3"
SYSTEM_PID_FILE="${RUN_DIR}/system_available.pid"
AUTOMATION_PID_FILE="${RUN_DIR}/automation_request.pid"
TIMEOUT_SEC="${1:-30}"
READ_TIMEOUT_SEC="${READ_TIMEOUT_SEC:-8}"

mkdir -p "${RUN_DIR}"

set +u
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash >/dev/null 2>&1
set -u

ros2_cli() {
  "${ROS2_BASE[@]}" "$@"
}

if ros2 --help 2>&1 | grep -q -- '--no-daemon'; then
  ROS2_BASE=(ros2 --no-daemon)
else
  ROS2_BASE=(ros2)
fi

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

read_string_topic() {
  local topic="$1"
  timeout "${READ_TIMEOUT_SEC}s" "${ROS2_BASE[@]}" topic echo --once "${topic}" 2>/dev/null | awk '
    /data:/{
      sub(/^data:[[:space:]]*/, "")
      gsub(/^'\''|'\''$/, "")
      print
      exit
    }
  '
}

read_bool_topic() {
  local topic="$1"
  timeout "${READ_TIMEOUT_SEC}s" "${ROS2_BASE[@]}" topic echo --once "${topic}" 2>/dev/null | awk '/data:/{print $2; exit}'
}

echo "[L3] clearing stale scenario flags"
ros2_cli topic pub --once /scenario/td_request std_msgs/msg/Bool '{data: false}' >/dev/null
ros2_cli topic pub --once /scenario/force_mrm std_msgs/msg/Bool '{data: false}' >/dev/null
ros2_cli topic pub --once /scenario/reset_mrm std_msgs/msg/Bool '{data: true}' >/dev/null

echo "[L3] starting background keepalive publishers"
cleanup_pid_file "${SYSTEM_PID_FILE}"
nohup /home/vci/sim/carla-humble-0.9.15-ws/scripts/publish_system_available.sh >"${RUN_DIR}/system_available.log" 2>&1 &
echo "$!" > "${SYSTEM_PID_FILE}"

cleanup_pid_file "${AUTOMATION_PID_FILE}"
nohup /home/vci/sim/carla-humble-0.9.15-ws/scripts/publish_automation_request.sh >"${RUN_DIR}/automation_request.log" 2>&1 &
echo "$!" > "${AUTOMATION_PID_FILE}"

deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  state="$(read_string_topic /l3/state || true)"
  reason="$(read_string_topic /l3/state_reason || true)"
  action="$(read_string_topic /driver/action_label || true)"
  driver_available="$(read_bool_topic /driver/available || true)"

  if [[ "${state}" == "ACTIVE" ]]; then
    echo "[L3] state=ACTIVE reason=${reason:-unknown} action=${action:-unknown} driver_available=${driver_available:-unknown}"
    exit 0
  fi

  echo "[L3] waiting state=${state:-unknown} reason=${reason:-unknown} action=${action:-unknown} driver_available=${driver_available:-unknown}"
  sleep 1
done

echo "[L3] timed out before ACTIVE"
echo "[L3] hint: Autoware must be route-ready and operation mode must be switchable"
echo "[L3] hint: if driver monitoring is enabled, DMS must report available=true and action should be normal"
exit 1
