#!/usr/bin/env bash
set -euo pipefail

set +u
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash >/dev/null 2>&1
source /home/vci/sim/carla-humble-0.9.15-ws/scripts/env_carla915.sh >/dev/null 2>&1
set -u

export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

exec python3 /home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_prepare_route.py "$@"
