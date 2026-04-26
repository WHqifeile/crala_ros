#!/usr/bin/env bash
set -euo pipefail

source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash

exec ros2 topic pub -r 2 /scenario/force_mrm std_msgs/msg/Bool '{data: true}'
