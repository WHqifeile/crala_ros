# Autoware + CARLA + DMS + L3 Runbook

## Target Architecture

- `CARLA 0.9.15` provides the simulator and ego vehicle.
- `autoware_carla_interface` is the official Autoware-CARLA bridge.
- `external_dms_pose` publishes `/driver/*`.
- `l3_supervisor` consumes `/driver/*`, `/vehicle/status/*`, `/api/operation_mode/*`, and `/scenario/*`.
- `l3_supervisor` publishes `/l3/*` and `/hmi/status_text`.
- `l3_supervisor` sends MRM override to `/control/command/actuation_cmd`.

## Local Paths

- CARLA root: `/home/vci/sim/carla-0.9.15`
- Autoware workspace: `/home/vci/sim/autoware`
- Custom overlay: `/home/vci/sim/carla-humble-0.9.15-ws`
- Town01 map path: `/home/vci/autoware_map/Town01`

## Prepared Scripts

- Unified workflow manager: [autoware_l3_workflow.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh)
- Stop stack: [stop_autoware_l3_stack.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/stop_autoware_l3_stack.sh)
- Start CARLA: [start_carla_3000.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/start_carla_3000.sh)
- Start Autoware official CARLA launch: [start_autoware_e2e_carla.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/start_autoware_e2e_carla.sh)
- Start DMS + L3 overlay: [start_l3_overlay.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/start_l3_overlay.sh)
- Enter L3 with one command: [enter_l3_mode.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/enter_l3_mode.sh)
- Exit L3 and clear requests: [exit_l3_mode.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/exit_l3_mode.sh)
- Inject takeover request: [publish_td_request.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/publish_td_request.sh)
- Inject MRM: [publish_force_mrm.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/publish_force_mrm.sh)
- Clear scenario flags: [clear_scenario_flags.sh](/home/vci/sim/carla-humble-0.9.15-ws/scripts/clear_scenario_flags.sh)

## Recommended Start Order

### One-command workflow

Start the full stack and try to enter L3:

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh start /dev/video1 45
```

Only boot the stack without requesting L3:

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh boot /dev/video1
```

Check state at any time:

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh status
```

Trigger scenario events from the same entry point:

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh tor
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh mrm
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh clear
```

Exit L3 or stop everything:

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh exit
/home/vci/sim/carla-humble-0.9.15-ws/scripts/autoware_l3_workflow.sh stop
```

### 1. Stop any previous stack

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/stop_autoware_l3_stack.sh
```

### 2. Start CARLA 0.9.15

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/start_carla_3000.sh
```

### 3. Start official Autoware-CARLA interface

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/start_autoware_e2e_carla.sh
```

### 4. Start DMS + L3 overlay

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/start_l3_overlay.sh /dev/video1
```

### 5. Enter L3 with one command

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/enter_l3_mode.sh
```

This script does four things:

- clears stale `TOR/MRM` flags
- keeps `/scenario/system_available=true` alive
- keeps `/scenario/automation_request=true` alive
- waits until `/l3/state` becomes `ACTIVE`

If it times out, it prints the current `state`, `state_reason`, and `/driver/action_label`.

### 6. Exit L3 and clear triggers

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/exit_l3_mode.sh
```

## Expected Topics

### Driver side

- `/driver/available`
- `/driver/drowsy`
- `/driver/distracted`
- `/driver/action_label`
- `/driver/head_pose`
- `/driver/gaze_vector`

### Autoware / vehicle side

- `/clock`
- `/vehicle/status/velocity_status`
- `/vehicle/status/control_mode`
- `/api/operation_mode/state`
- `/control/command/actuation_cmd`

### L3 side

- `/l3/state`
- `/l3/state_reason`
- `/l3/td_request`
- `/l3/mrm_request`
- `/hmi/status_text`

## Scenario Injection

### Trigger TOR

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/publish_td_request.sh
```

### Trigger MRM

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/publish_force_mrm.sh
```

### Clear all scenario flags

```bash
/home/vci/sim/carla-humble-0.9.15-ws/scripts/clear_scenario_flags.sh
```

## Current Known Blocker

- The local `CARLA 0.9.15` binary starts, binds `3000/3001/3002`, and loads GPU memory, but can remain in a long startup phase before RPC responds.
- A previous Vulkan startup failure was captured in `/home/vci/sim/logs/carla015-3000.log` with `VK_ERROR_FEATURE_NOT_PRESENT`.
- Current best local launch variant is:

```bash
env DISPLAY=:0 XAUTHORITY=/run/user/1000/gdm/Xauthority SDL_VIDEODRIVER=x11 __GLX_VENDOR_LIBRARY_NAME=nvidia \
  /home/vci/sim/carla-0.9.15/CarlaUE4.sh -prefernvidia -quality-level=Low -carla-rpc-port=3000
```

- If CARLA still does not answer RPC after several minutes, the remaining issue is in CARLA runtime startup on this machine, not in Autoware or `l3_supervisor`.
