#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Use this file with: source /home/vci/sim/carla-humble-0.9.15-ws/scripts/env_ros_l3.sh"
  exit 1
fi

export AMENT_TRACE_SETUP_FILES="${AMENT_TRACE_SETUP_FILES:-}"
export AMENT_PYTHON_EXECUTABLE="${AMENT_PYTHON_EXECUTABLE:-/usr/bin/python3}"

set +u
source /opt/ros/humble/setup.bash
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup.bash
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash
set -u

export RMW_IMPLEMENTATION="${RMW_IMPLEMENTATION:-rmw_cyclonedds_cpp}"
