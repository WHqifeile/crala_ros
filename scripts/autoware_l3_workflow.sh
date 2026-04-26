#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/vci/sim/carla-humble-0.9.15-ws"
RUN_DIR="/tmp/autoware_l3"
LOG_DIR="/home/vci/sim/logs"

CARLA_LOG="${LOG_DIR}/workflow_carla.log"
AUTOWARE_LOG="${LOG_DIR}/workflow_autoware.log"
FALLBACK_LOG="${LOG_DIR}/workflow_fallback.log"
PERCEPTION_STUB_LOG="${LOG_DIR}/workflow_perception_stub.log"
L3_LOG="${LOG_DIR}/workflow_l3.log"
IO_HUB_LOG="${LOG_DIR}/workflow_io_hub.log"

CARLA_PID_FILE="${RUN_DIR}/carla.pid"
AUTOWARE_PID_FILE="${RUN_DIR}/autoware.pid"
FALLBACK_PID_FILE="${RUN_DIR}/fallback.pid"
PERCEPTION_STUB_PID_FILE="${RUN_DIR}/perception_stub.pid"
L3_PID_FILE="${RUN_DIR}/l3_overlay.pid"
IO_HUB_PID_FILE="${RUN_DIR}/io_hub.pid"

mkdir -p "${RUN_DIR}" "${LOG_DIR}"

source_stack() {
  set +u
  source "${ROOT_DIR}/install/setup_autoware_l3_stack.bash" >/dev/null 2>&1
  set -u
}

ros2_cli() {
  ros2 "$@"
}

topic_probe() {
  source_stack
  python3 "${ROOT_DIR}/scripts/ros_wait_for_topic.py" "$@"
}

cleanup_pid_file() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}")"
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${pid_file}"
    fi
  fi
}

record_pid() {
  local pid_file="$1"
  local pid="$2"
  echo "${pid}" > "${pid_file}"
}

start_background() {
  local log_file="$1"
  local pid_file="$2"
  shift 2

  nohup "$@" >"${log_file}" 2>&1 &
  local pid=$!
  record_pid "${pid_file}" "${pid}"
  echo "${pid}"
}

wait_for_carla_rpc() {
  local timeout_sec="$1"
  if python3 - "${timeout_sec}" <<'PY' >/dev/null 2>&1
import sys
import socket
import time

timeout = int(sys.argv[1])
deadline = time.time() + timeout
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", 3000), timeout=2.0):
            pass
        sys.exit(0)
    except Exception:
        time.sleep(1.0)
sys.exit(1)
PY
  then
    return 0
  fi
  return 1
}

ensure_carla_world_ready() {
  bash -lc "set +u; source ${ROOT_DIR}/scripts/env_carla915.sh >/dev/null 2>&1; set -u; exec python3 ${ROOT_DIR}/scripts/ensure_carla_world.py --host 127.0.0.1 --port 3000 --map Town01 --timeout-sec 120 --tick-timeout-sec 30"
}

discover_pid() {
  local kind="$1"
  case "${kind}" in
    carla)
      pgrep -f '/home/vci/sim/carla-0.9.15/CarlaUE4.sh|CarlaUE4-Linux-Shipping' | head -n 1
      ;;
    autoware)
      pgrep -f 'ros2 launch autoware_launch e2e_simulator.launch.xml' | head -n 1
      ;;
    l3)
      pgrep -f 'l3_supervisor_node|external_dms_pose_node|l3_lightweight_stack.launch.py' | head -n 1
      ;;
    fallback)
      pgrep -f 'publish_fallback_trajectory.py' | head -n 1
      ;;
    perception_stub)
      pgrep -f 'publish_empty_objects.py' | head -n 1
      ;;
    io_hub)
      pgrep -f 'autoware_io_hub.py' | head -n 1
      ;;
    *)
      return 1
      ;;
  esac
}

wait_for_topic() {
  local topic="$1"
  local timeout_sec="$2"
  topic_probe --timeout-sec "${timeout_sec}" --count 1 "${topic}" >/dev/null 2>&1
}

wait_for_topic_listed() {
  local topic="$1"
  local timeout_sec="$2"
  topic_probe --timeout-sec "${timeout_sec}" --listed-only "${topic}" >/dev/null 2>&1
}

wait_for_topic_samples() {
  local topic="$1"
  local timeout_sec="$2"
  local required_samples="$3"
  topic_probe --timeout-sec "${timeout_sec}" --count "${required_samples}" "${topic}" >/dev/null 2>&1
}

wait_for_optional_topic_samples() {
  local topic="$1"
  local timeout_sec="$2"
  local required_samples="$3"
  if wait_for_topic_samples "${topic}" "${timeout_sec}" "${required_samples}"; then
    echo "[workflow] optional topic ready: ${topic}"
    return 0
  fi
  echo "[workflow] warning: optional topic did not stabilize: ${topic}"
  return 1
}

wait_for_autoware_runtime_ready() {
  echo "[workflow] waiting for stable /clock"
  if ! wait_for_topic_samples /clock 120 3; then
    echo "[workflow] /clock did not stabilize"
    return 1
  fi

  echo "[workflow] waiting for GNSS pose"
  if ! wait_for_topic_samples /sensing/gnss/pose_with_covariance 90 3; then
    echo "[workflow] GNSS pose did not stabilize"
    return 1
  fi

  echo "[workflow] waiting for vehicle velocity status"
  if ! wait_for_topic_samples /vehicle/status/velocity_status 90 3; then
    echo "[workflow] vehicle velocity status did not stabilize"
    return 1
  fi

  echo "[workflow] waiting for localization output (optional before explicit initialization)"
  wait_for_optional_topic_samples /localization/kinematic_state 20 2 || true

  echo "[workflow] waiting for LiDAR stream (optional during startup)"
  wait_for_optional_topic_samples /sensing/lidar/top/pointcloud_before_sync 20 1 || true

  if ! wait_for_topic /api/operation_mode/state 60; then
    echo "[workflow] warning: /api/operation_mode/state did not appear yet, continue to route initialization"
  fi

  wait_for_optional_topic_samples /sensing/camera/CAM_FRONT/image_raw 20 2 || true
  return 0
}

print_topic_once() {
  local topic="$1"
  topic_probe --timeout-sec 3 --count 1 --print-once "${topic}" 2>/dev/null || true
}

tail_log() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    tail -n 20 "${file}"
  else
    echo "log not found: ${file}"
  fi
}

do_start() {
  local device_path="${1:-/dev/video1}"
  local enter_timeout="${2:-45}"
  local skip_enter="${3:-false}"
  local with_dms="${4:-${WITH_DMS:-false}}"
  local ignore_dms_status="${5:-${IGNORE_DMS_STATUS:-false}}"
  local target_speed_mps="${TARGET_SPEED_MPS:-16.7}"
  local enable_fallback_publisher="${ENABLE_FALLBACK_PUBLISHER:-false}"
  local enable_perception_stub="${ENABLE_PERCEPTION_STUB:-${AUTOWARE_PERCEPTION:-false}}"

  echo "[workflow] stopping old stack"
  "${ROOT_DIR}/scripts/exit_l3_mode.sh" >/dev/null 2>&1 || true
  "${ROOT_DIR}/scripts/stop_autoware_l3_stack.sh" >/dev/null 2>&1 || true
  rm -f "${CARLA_PID_FILE}" "${AUTOWARE_PID_FILE}" "${FALLBACK_PID_FILE}" "${PERCEPTION_STUB_PID_FILE}" "${L3_PID_FILE}" "${IO_HUB_PID_FILE}"

  echo "[workflow] starting CARLA"
  local carla_pid
  carla_pid="$(start_background "${CARLA_LOG}" "${CARLA_PID_FILE}" env CARLA_QUALITY_LEVEL="${CARLA_QUALITY_LEVEL:-Low}" CARLA_RENDER_OFFSCREEN="${CARLA_RENDER_OFFSCREEN:-true}" "${ROOT_DIR}/scripts/start_carla_3000.sh")"
  record_pid "${CARLA_PID_FILE}" "${carla_pid}"
  echo "[workflow] CARLA pid=${carla_pid} log=${CARLA_LOG}"
  if ! wait_for_carla_rpc 120; then
    echo "[workflow] CARLA RPC did not become ready"
    tail_log "${CARLA_LOG}"
    return 1
  fi
  if ! ensure_carla_world_ready; then
    echo "[workflow] CARLA world did not become ready"
    tail_log "${CARLA_LOG}"
    return 1
  fi

  echo "[workflow] starting Autoware"
  local autoware_pid
  autoware_pid="$(start_background "${AUTOWARE_LOG}" "${AUTOWARE_PID_FILE}" "${ROOT_DIR}/scripts/start_autoware_e2e_carla.sh")"
  record_pid "${AUTOWARE_PID_FILE}" "${autoware_pid}"
  echo "[workflow] Autoware pid=${autoware_pid} log=${AUTOWARE_LOG}"
  if ! wait_for_autoware_runtime_ready; then
    echo "[workflow] Autoware runtime did not become ready"
    tail_log "${AUTOWARE_LOG}"
    return 1
  fi

  if [[ "${enable_fallback_publisher}" == "true" ]]; then
    echo "[workflow] starting fallback trajectory publisher"
    local fallback_pid
    fallback_pid="$(start_background "${FALLBACK_LOG}" "${FALLBACK_PID_FILE}" bash -lc "set +u; source /opt/ros/humble/setup.bash; source ${ROOT_DIR}/install/setup.bash; set -u; exec python3 ${ROOT_DIR}/scripts/publish_fallback_trajectory.py")"
    record_pid "${FALLBACK_PID_FILE}" "${fallback_pid}"
    echo "[workflow] fallback pid=${fallback_pid} log=${FALLBACK_LOG}"
  else
    echo "[workflow] fallback trajectory publisher disabled (ENABLE_FALLBACK_PUBLISHER=false)"
    rm -f "${FALLBACK_PID_FILE}"
  fi

  if [[ "${enable_perception_stub}" != "true" ]]; then
    echo "[workflow] starting empty-object perception stub (AUTOWARE_PERCEPTION=false)"
    local perception_stub_pid
    perception_stub_pid="$(start_background "${PERCEPTION_STUB_LOG}" "${PERCEPTION_STUB_PID_FILE}" bash -lc "set +u; source /opt/ros/humble/setup.bash; source ${ROOT_DIR}/install/setup.bash; set -u; exec python3 ${ROOT_DIR}/scripts/publish_empty_objects.py")"
    record_pid "${PERCEPTION_STUB_PID_FILE}" "${perception_stub_pid}"
    echo "[workflow] perception_stub pid=${perception_stub_pid} log=${PERCEPTION_STUB_LOG}"
  else
    echo "[workflow] perception stub disabled (real perception enabled)"
    rm -f "${PERCEPTION_STUB_PID_FILE}"
  fi

  echo "[workflow] starting L3 overlay (with_dms=${with_dms}, ignore_dms_status=${ignore_dms_status})"
  local l3_pid
  l3_pid="$(start_background "${L3_LOG}" "${L3_PID_FILE}" "${ROOT_DIR}/scripts/start_l3_overlay.sh" "${device_path}" "${with_dms}" "" "${ignore_dms_status}")"
  record_pid "${L3_PID_FILE}" "${l3_pid}"
  echo "[workflow] L3 pid=${l3_pid} log=${L3_LOG}"
  if ! wait_for_topic /l3/state 45; then
    echo "[workflow] /l3/state did not appear"
    tail_log "${L3_LOG}"
    return 1
  fi

  echo "[workflow] starting Autoware IO hub"
  local io_hub_pid
  io_hub_pid="$(start_background "${IO_HUB_LOG}" "${IO_HUB_PID_FILE}" bash -lc "set +u; source /opt/ros/humble/setup.bash; source ${ROOT_DIR}/install/setup_autoware_l3_stack.bash; set -u; exec python3 ${ROOT_DIR}/scripts/autoware_io_hub.py")"
  record_pid "${IO_HUB_PID_FILE}" "${io_hub_pid}"
  echo "[workflow] io_hub pid=${io_hub_pid} log=${IO_HUB_LOG}"

  echo "[workflow] stack is up"
  if [[ "${skip_enter}" == "true" ]]; then
    echo "[workflow] enter_l3 skipped"
    return 0
  fi

  echo "[workflow] preparing localization and route"
  local route_args=(
    --goal-distances "${ROUTE_GOAL_DISTANCES:-15,25,35,50,80,120}"
    --lateral-offsets "${ROUTE_LATERAL_OFFSETS:-0.0}"
    --carla-y-mode "${ROUTE_CARLA_Y_MODE:-invert}"
  )
  if [[ "${ROUTE_ENABLE_TURNS:-true}" == "true" ]]; then
    route_args+=(--enable-turns)
  fi
  if [[ "${ROUTE_ENABLE_LANE_CHANGE:-true}" == "true" ]]; then
    route_args+=(--enable-lane-change)
  fi
  if ! "${ROOT_DIR}/scripts/prepare_autoware_route.sh" "${route_args[@]}"; then
    echo "[workflow] primary route setup failed, retry with conservative route options"
    if ! "${ROOT_DIR}/scripts/prepare_autoware_route.sh" \
      --goal-distances "${ROUTE_GOAL_DISTANCES_RETRY:-15,25,35,50,80,120}" \
      --lateral-offsets "${ROUTE_LATERAL_OFFSETS_RETRY:-0.0}" \
      --carla-y-mode "${ROUTE_CARLA_Y_MODE_RETRY:-invert}"; then
      echo "[workflow] automatic localization/route setup failed after retry"
      print_topic_once /api/operation_mode/state
      return 1
    fi
  fi

  echo "[workflow] requesting L3 entry"
  if ! "${ROOT_DIR}/scripts/enter_l3_mode.sh" "${enter_timeout}"; then
    echo "[workflow] enter_l3 did not reach ACTIVE"
    echo "[workflow] current L3 state:"
    print_topic_once /l3/state
    print_topic_once /l3/state_reason
    print_topic_once /hmi/status_text
    return 1
  fi

  echo "[workflow] setting target speed limit to ${target_speed_mps} m/s"
  "${ROOT_DIR}/scripts/set_autoware_speed_limit.sh" "${target_speed_mps}" || true

  echo "[workflow] requesting autonomous operation mode"
  if ! ros2_cli service call /api/operation_mode/change_to_autonomous autoware_adapi_v1_msgs/srv/ChangeOperationMode "{}" >/dev/null; then
    echo "[workflow] change_to_autonomous failed"
    print_topic_once /api/operation_mode/state
    return 1
  fi

  echo "[workflow] verifying planning/control outputs"
  if ! wait_for_topic_samples /planning/trajectory 25 2; then
    echo "[workflow] /planning/trajectory not ready; retry route refresh once"
    local recover_route_args=(
      --skip-localization-init
      --goal-distances "${ROUTE_RECOVER_GOAL_DISTANCES:-35,50,80,120}"
      --lateral-offsets "${ROUTE_RECOVER_LATERAL_OFFSETS:-0.0}"
      --carla-y-mode "${ROUTE_CARLA_Y_MODE:-invert}"
    )
    if [[ "${ROUTE_ENABLE_TURNS:-false}" == "true" ]]; then
      recover_route_args+=(--enable-turns)
    fi
    if [[ "${ROUTE_ENABLE_LANE_CHANGE:-false}" == "true" ]]; then
      recover_route_args+=(--enable-lane-change)
    fi

    "${ROOT_DIR}/scripts/prepare_autoware_route.sh" "${recover_route_args[@]}" || true
    "${ROOT_DIR}/scripts/set_autoware_speed_limit.sh" "${target_speed_mps}" || true
    ros2_cli service call /api/operation_mode/change_to_autonomous autoware_adapi_v1_msgs/srv/ChangeOperationMode "{}" >/dev/null || true
  fi

  if ! wait_for_topic_samples /planning/trajectory 30 2; then
    echo "[workflow] /planning/trajectory still unavailable after recovery"
    print_topic_once /planning/trajectory
    print_topic_once /planning/mission_planning/route
    print_topic_once /planning/scenario_planning/scenario
    return 1
  fi

  if ! wait_for_topic_samples /control/command/control_cmd 30 2; then
    echo "[workflow] /control/command/control_cmd unavailable after planning recovered"
    print_topic_once /control/command/control_cmd
    return 1
  fi
}

do_status() {
  cleanup_pid_file "${CARLA_PID_FILE}"
  cleanup_pid_file "${AUTOWARE_PID_FILE}"
  cleanup_pid_file "${FALLBACK_PID_FILE}"
  cleanup_pid_file "${PERCEPTION_STUB_PID_FILE}"
  cleanup_pid_file "${L3_PID_FILE}"
  cleanup_pid_file "${IO_HUB_PID_FILE}"

  if [[ ! -f "${CARLA_PID_FILE}" ]]; then
    carla_pid="$(discover_pid carla || true)"
    [[ -n "${carla_pid}" ]] && record_pid "${CARLA_PID_FILE}" "${carla_pid}"
  fi
  if [[ ! -f "${AUTOWARE_PID_FILE}" ]]; then
    autoware_pid="$(discover_pid autoware || true)"
    [[ -n "${autoware_pid}" ]] && record_pid "${AUTOWARE_PID_FILE}" "${autoware_pid}"
  fi
  if [[ ! -f "${L3_PID_FILE}" ]]; then
    l3_pid="$(discover_pid l3 || true)"
    [[ -n "${l3_pid}" ]] && record_pid "${L3_PID_FILE}" "${l3_pid}"
  fi
  if [[ ! -f "${FALLBACK_PID_FILE}" ]]; then
    fallback_pid="$(discover_pid fallback || true)"
    [[ -n "${fallback_pid}" ]] && record_pid "${FALLBACK_PID_FILE}" "${fallback_pid}"
  fi
  if [[ ! -f "${PERCEPTION_STUB_PID_FILE}" ]]; then
    perception_stub_pid="$(discover_pid perception_stub || true)"
    [[ -n "${perception_stub_pid}" ]] && record_pid "${PERCEPTION_STUB_PID_FILE}" "${perception_stub_pid}"
  fi
  if [[ ! -f "${IO_HUB_PID_FILE}" ]]; then
    io_hub_pid="$(discover_pid io_hub || true)"
    [[ -n "${io_hub_pid}" ]] && record_pid "${IO_HUB_PID_FILE}" "${io_hub_pid}"
  fi

  echo "[workflow] pid files"
  for pid_file in "${CARLA_PID_FILE}" "${AUTOWARE_PID_FILE}" "${FALLBACK_PID_FILE}" "${PERCEPTION_STUB_PID_FILE}" "${L3_PID_FILE}" "${IO_HUB_PID_FILE}"; do
    if [[ -f "${pid_file}" ]]; then
      echo "  $(basename "${pid_file}") pid=$(cat "${pid_file}")"
    else
      echo "  $(basename "${pid_file}") missing"
    fi
  done

  echo "[workflow] ROS topics"
  print_topic_once /l3/state
  print_topic_once /l3/state_reason
  print_topic_once /hmi/status_text
  print_topic_once /driver/action_label
  print_topic_once /api/operation_mode/state
  print_topic_once /vehicle/status/velocity_status
  print_topic_once /planning/trajectory
  print_topic_once /control/command/control_cmd
  print_topic_once /autoware/io_hub/ready
  print_topic_once /autoware/output/trajectory
  print_topic_once /autoware/output/control_cmd
}

do_stop() {
  echo "[workflow] stopping stack"
  if [[ -f "${IO_HUB_PID_FILE}" ]]; then
    kill "$(cat "${IO_HUB_PID_FILE}")" >/dev/null 2>&1 || true
  fi
  if [[ -f "${FALLBACK_PID_FILE}" ]]; then
    kill "$(cat "${FALLBACK_PID_FILE}")" >/dev/null 2>&1 || true
  fi
  if [[ -f "${PERCEPTION_STUB_PID_FILE}" ]]; then
    kill "$(cat "${PERCEPTION_STUB_PID_FILE}")" >/dev/null 2>&1 || true
  fi
  rm -f "${IO_HUB_PID_FILE}" "${FALLBACK_PID_FILE}" "${PERCEPTION_STUB_PID_FILE}"
  "${ROOT_DIR}/scripts/exit_l3_mode.sh" >/dev/null 2>&1 || true
  "${ROOT_DIR}/scripts/stop_autoware_l3_stack.sh"
}

do_enter() {
  local timeout_sec="${1:-45}"
  "${ROOT_DIR}/scripts/enter_l3_mode.sh" "${timeout_sec}"
}

do_prepare() {
  "${ROOT_DIR}/scripts/prepare_autoware_route.sh"
}

do_exit() {
  "${ROOT_DIR}/scripts/exit_l3_mode.sh"
}

do_tor() {
  "${ROOT_DIR}/scripts/publish_td_request.sh"
}

do_mrm() {
  "${ROOT_DIR}/scripts/publish_force_mrm.sh"
}

do_clear() {
  "${ROOT_DIR}/scripts/clear_scenario_flags.sh"
}

usage() {
  cat <<EOF
Usage:
  $(basename "$0") start [device_path] [enter_timeout_sec] [with_dms] [ignore_dms_status]
  $(basename "$0") boot [device_path] [with_dms] [ignore_dms_status]
  $(basename "$0") restart [device_path] [enter_timeout_sec] [with_dms] [ignore_dms_status]
  $(basename "$0") status
  $(basename "$0") prepare
  $(basename "$0") enter [timeout_sec]
  $(basename "$0") exit
  $(basename "$0") tor
  $(basename "$0") mrm
  $(basename "$0") clear
  $(basename "$0") stop

Notes:
  start   = stop old stack, start everything, then prepare route + enter L3 + change_to_autonomous
  boot    = stop old stack and start everything, but do not request L3
  restart = same as start
  with_dms= true/false, default false (camera optional mode)
  ignore_dms_status=true/false, default false; true means do not gate L3 on /driver/*
EOF
}

subcommand="${1:-start}"
case "${subcommand}" in
  start)
    do_start "${2:-/dev/video1}" "${3:-45}" "false" "${4:-${WITH_DMS:-false}}" "${5:-${IGNORE_DMS_STATUS:-false}}"
    ;;
  boot)
    do_start "${2:-/dev/video1}" "45" "true" "${3:-${WITH_DMS:-false}}" "${4:-${IGNORE_DMS_STATUS:-false}}"
    ;;
  restart)
    do_start "${2:-/dev/video1}" "${3:-45}" "false" "${4:-${WITH_DMS:-false}}" "${5:-${IGNORE_DMS_STATUS:-false}}"
    ;;
  status)
    do_status
    ;;
  prepare)
    do_prepare
    ;;
  enter)
    do_enter "${2:-45}"
    ;;
  exit)
    do_exit
    ;;
  tor)
    do_tor
    ;;
  mrm)
    do_mrm
    ;;
  clear)
    do_clear
    ;;
  stop)
    do_stop
    ;;
  *)
    usage
    exit 1
    ;;
esac
