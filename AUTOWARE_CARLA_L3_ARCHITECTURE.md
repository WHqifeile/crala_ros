# Autoware + CARLA + DMS + L3 Supervisor

## Target Architecture

This workspace now targets the following split of responsibilities:

- `CARLA + official CARLA ROS bridge`
  - world simulation
  - ego vehicle state and sensor topics
  - low-level vehicle actuation back into CARLA
- `Autoware + official autoware_carla_interface`
  - perception, planning, control
  - normal autonomous driving commands
- `external_dms_pose`
  - driver monitoring from a real in-cabin camera
  - publishes `/driver/*`
- `l3_supervisor`
  - fuses `/driver/*`, `/carla/*`, optional `/scenario/*`
  - keeps Autoware engaged in `ACTIVE`
  - issues takeover request in `TOR`
  - overrides with direct braking in `MRM`

## Control Ownership

Normal path:

- `Autoware -> official Autoware/CARLA interface -> CARLA`

Supervisory path:

- `DMS + scene/system inputs + vehicle state -> l3_supervisor`

Override path:

- `l3_supervisor -> /carla/<role>/vehicle_control_cmd`

The supervisor no longer uses CARLA built-in autopilot as the nominal automation backend.

## Current Topics

Supervisor inputs:

- `/driver/available`
- `/driver/drowsy`
- `/driver/distracted`
- `/driver/action_label`
- `/driver/head_pose`
- `/driver/gaze_vector`
- `/carla/status`
- `/carla/<role>/vehicle_status`
- `/carla/<role>/vehicle_control_manual_override`
- `/carla/<role>/vehicle_control_cmd_manual`
- optional `/scenario/system_available`
- optional `/scenario/automation_request`
- optional `/scenario/td_request`
- optional `/scenario/force_mrm`
- optional `/scenario/reset_mrm`

Supervisor outputs:

- `/l3/automation_allowed`
- `/l3/automation_engaged`
- `/l3/driver_ready`
- `/l3/system_available`
- `/l3/td_request`
- `/l3/takeover_completed`
- `/l3/mrm_request`
- `/l3/state`
- `/l3/state_reason`
- `/l3/tor_reason`
- `/hmi/status_text`

MRM override output:

- `/carla/<role>/vehicle_control_cmd`

Autoware handoff:

- preferred: Autoware AD API operation-mode services
- fallback: `/autoware/engage`

## What Was Removed

Removed from the source workspace:

- `scenario_injector`
- the old `simple_l3_stack.launch.py`
- the previous CARLA-autopilot nominal-control path inside `l3_supervisor`

This means the stack is no longer organized around manual test injection as the primary control path.

## Startup Boundary

You still need an Autoware workspace sourced in the same shell if you want the supervisor to drive the official Autoware interfaces.

Without Autoware sourced:

- `external_dms_pose` works
- `l3_supervisor` works
- `MRM` direct braking to CARLA works
- Autoware engage/operation-mode handoff stays disabled

With Autoware sourced:

- `l3_supervisor` automatically tries:
  1. AD API operation-mode services
  2. `/autoware/engage` topic fallback

## No-Camera Mode (Current Default)

The stack now supports a no-camera mode so you can run Autoware + CARLA + L3 switching even if DMS is not ready:

- launch default `with_dms=false`
- `l3_supervisor` parameter `require_driver_monitoring=false`

In this mode:

- L3 activation does not block on `/driver/*`
- you can still do manual takeover from keyboard/vehicle control mode changes
- you can later re-enable DMS by setting `with_dms:=true` and `require_driver_monitoring:=true`

## Minimal Runtime Split

Terminal 1:

- CARLA server

Terminal 2:

- `carla_ros_bridge`

Terminal 3:

- ego vehicle spawn

Terminal 4:

- `carla_manual_control`

Terminal 5:

- source Autoware workspace
- source this workspace
- `ros2 launch l3_supervisor l3_lightweight_stack.launch.py role_name:=hero device_path:=/dev/video1`

## Recommended Next Step

Install and source an official Autoware workspace that includes `autoware_carla_interface`, then run the supervisor in the same environment.
