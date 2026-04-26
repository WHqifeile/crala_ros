#!/usr/bin/env bash
set -euo pipefail

DEVICE_PATH="${1:-/dev/video1}"
WITH_DMS="${2:-${WITH_DMS:-false}}"
REQUIRE_DRIVER_MONITORING="${3:-${REQUIRE_DRIVER_MONITORING:-${WITH_DMS}}}"
IGNORE_DMS_STATUS="${4:-${IGNORE_DMS_STATUS:-false}}"

if [[ "${IGNORE_DMS_STATUS}" == "true" ]]; then
  REQUIRE_DRIVER_MONITORING="false"
fi

set +u
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash
set -u

exec ros2 launch l3_supervisor l3_lightweight_stack.launch.py \
  role_name:=ego_vehicle \
  with_dms:="${WITH_DMS}" \
  require_driver_monitoring:="${REQUIRE_DRIVER_MONITORING}" \
  device_path:="${DEVICE_PATH}"
