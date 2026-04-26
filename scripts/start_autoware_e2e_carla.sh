#!/usr/bin/env bash
set -euo pipefail

set +u
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash
set -u

# Ensure autoware_carla_interface can import the CARLA Python API.
CARLA_PY_SITE="/home/vci/sim/venvs/carla015/lib/python3.10/site-packages"
if [[ -d "${CARLA_PY_SITE}" ]]; then
  export PYTHONPATH="${CARLA_PY_SITE}:${PYTHONPATH:-}"
fi

exec ros2 launch autoware_launch e2e_simulator.launch.xml \
  map_path:=/home/vci/autoware_map/Town01 \
  vehicle_model:=sample_vehicle \
  sensor_model:=carla_sensor_kit \
  simulator_type:=carla \
  carla_map:=Town01 \
  port:=3000 \
  ego_vehicle_role_name:="${CARLA_EGO_ROLE_NAME:-ego_vehicle}" \
  spawn_point:="${CARLA_EGO_SPAWN_POINT:-229.78,2.02,0.3,0,0,0}" \
  rviz:=false \
  perception:="${AUTOWARE_PERCEPTION:-false}"
