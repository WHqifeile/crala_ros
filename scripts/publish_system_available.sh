#!/usr/bin/env bash
set -euo pipefail

set +u
source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash
set -u

exec ros2 topic pub -r 2 /scenario/system_available std_msgs/msg/Bool '{data: true}'
