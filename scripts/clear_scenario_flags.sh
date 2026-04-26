#!/usr/bin/env bash
set -euo pipefail

source /home/vci/sim/carla-humble-0.9.15-ws/install/setup_autoware_l3_stack.bash

ros2 topic pub --once /scenario/td_request std_msgs/msg/Bool '{data: false}'
ros2 topic pub --once /scenario/force_mrm std_msgs/msg/Bool '{data: false}'
ros2 topic pub --once /scenario/reset_mrm std_msgs/msg/Bool '{data: true}'
