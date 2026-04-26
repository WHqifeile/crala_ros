#!/usr/bin/env bash
set -euo pipefail

pkill -9 -f 'CarlaUE4|CarlaUE4-Linux-Shipping' || true
pkill -f 'autoware_carla_interface|e2e_simulator.launch.xml|autoware_launch' || true
pkill -f 'component_container(_mt)?' || true
pkill -f 'map_projection_loader|lanelet2_map_loader|pointcloud_map_loader' || true
pkill -f 'autoware_raw_vehicle_cmd_converter|operation_mode_transition_manager' || true
pkill -f 'l3_supervisor_node|external_dms_pose_node' || true
pkill -f 'publish_fallback_trajectory.py' || true
pkill -f 'publish_empty_objects.py' || true
pkill -f 'autoware_io_hub.py' || true
pkill -f 'ros2( --no-daemon)? topic pub -r 2 /scenario/system_available' || true
pkill -f 'ros2( --no-daemon)? topic pub -r 2 /scenario/automation_request' || true
pkill -f 'rviz2' || true

sleep 2

lsof -ti:3000 | xargs -r kill -9 || true
lsof -ti:3001 | xargs -r kill -9 || true
lsof -ti:3002 | xargs -r kill -9 || true
rm -f /tmp/autoware_l3/system_available.pid /tmp/autoware_l3/automation_request.pid
find /dev/shm -maxdepth 1 -name 'fastrtps_*' -delete 2>/dev/null || true
