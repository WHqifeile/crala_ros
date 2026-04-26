#!/usr/bin/env bash
set -euo pipefail

TARGET_MPS="${1:-16.7}"
ROOT_DIR="/home/vci/sim/carla-humble-0.9.15-ws"

set +u
source /opt/ros/humble/setup.bash
source "${ROOT_DIR}/install/setup_autoware_l3_stack.bash" >/dev/null 2>&1
set -u

export RMW_IMPLEMENTATION="${RMW_IMPLEMENTATION:-rmw_cyclonedds_cpp}"

set_param_if_node_exists() {
  local node="$1"
  local param="$2"
  local value="$3"
  if ros2 node list 2>/dev/null | grep -qx "${node}"; then
    ros2 param set "${node}" "${param}" "${value}" >/dev/null 2>&1 || true
    echo "[speed] ${node} ${param}=${value}"
  else
    echo "[speed] skip (node not found): ${node}"
  fi
}

set_param_if_node_exists /planning/scenario_planning/external_velocity_limit_selector max_vel "${TARGET_MPS}"
set_param_if_node_exists /planning/scenario_planning/velocity_smoother max_vel "${TARGET_MPS}"

