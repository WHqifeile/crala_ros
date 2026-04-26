#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from typing import Optional

import rclpy
from autoware_adapi_v1_msgs.msg import OperationModeState
from autoware_control_msgs.msg import Control
from autoware_perception_msgs.msg import PredictedObjects
from autoware_planning_msgs.msg import Trajectory
from nav_msgs.msg import Odometry
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from rclpy.parameter import Parameter
from std_msgs.msg import Bool, String


class AutowareIoHub(Node):
    def __init__(self) -> None:
        super().__init__("autoware_io_hub")
        self.set_parameters([Parameter("use_sim_time", value=True)])

        self.freshness_sec = float(self.declare_parameter("freshness_sec", 1.5).value)

        self.last_odom_ts: Optional[float] = None
        self.last_objects_ts: Optional[float] = None
        self.last_trajectory_ts: Optional[float] = None
        self.last_control_ts: Optional[float] = None
        self.last_mode_ts: Optional[float] = None

        self.latest_mode: Optional[OperationModeState] = None

        # Canonicalized inputs for Layer4 Hazard Assessor.
        self.pub_in_odom = self.create_publisher(Odometry, "/autoware/input/ego_odometry", 10)
        self.pub_in_objects = self.create_publisher(PredictedObjects, "/autoware/input/objects", 10)

        # Canonicalized outputs from Autoware execution layer.
        self.pub_out_trajectory = self.create_publisher(Trajectory, "/autoware/output/trajectory", 10)
        self.pub_out_control = self.create_publisher(Control, "/autoware/output/control_cmd", 10)
        self.pub_out_mode = self.create_publisher(String, "/autoware/output/system_state", 10)

        # IO hub health for orchestration.
        self.pub_ready = self.create_publisher(Bool, "/autoware/io_hub/ready", 10)
        self.pub_status = self.create_publisher(String, "/autoware/io_hub/status", 10)

        self.create_subscription(Odometry, "/localization/kinematic_state", self._on_odom, 10)
        self.create_subscription(
            PredictedObjects,
            "/perception/object_recognition/objects",
            self._on_objects,
            10,
        )
        self.create_subscription(Trajectory, "/planning/trajectory", self._on_trajectory, 10)
        self.create_subscription(Control, "/control/command/control_cmd", self._on_control, 10)
        self.create_subscription(OperationModeState, "/api/operation_mode/state", self._on_mode, 10)

        self.create_timer(1.0, self._on_health_timer)
        self.get_logger().info("autoware_io_hub started")

    def _now(self) -> float:
        return time.monotonic()

    def _is_fresh(self, ts: Optional[float]) -> bool:
        return ts is not None and (self._now() - ts) <= self.freshness_sec

    def _on_odom(self, msg: Odometry) -> None:
        self.last_odom_ts = self._now()
        self.pub_in_odom.publish(msg)

    def _on_objects(self, msg: PredictedObjects) -> None:
        self.last_objects_ts = self._now()
        self.pub_in_objects.publish(msg)

    def _on_trajectory(self, msg: Trajectory) -> None:
        self.last_trajectory_ts = self._now()
        self.pub_out_trajectory.publish(msg)

    def _on_control(self, msg: Control) -> None:
        self.last_control_ts = self._now()
        self.pub_out_control.publish(msg)

    def _on_mode(self, msg: OperationModeState) -> None:
        self.last_mode_ts = self._now()
        self.latest_mode = msg
        summary = {
            "mode": int(msg.mode),
            "is_autoware_control_enabled": bool(msg.is_autoware_control_enabled),
            "is_in_transition": bool(msg.is_in_transition),
            "is_stop_mode_available": bool(msg.is_stop_mode_available),
            "is_autonomous_mode_available": bool(msg.is_autonomous_mode_available),
            "is_local_mode_available": bool(msg.is_local_mode_available),
            "is_remote_mode_available": bool(msg.is_remote_mode_available),
        }
        out = String()
        out.data = json.dumps(summary, ensure_ascii=False)
        self.pub_out_mode.publish(out)

    def _on_health_timer(self) -> None:
        required_fresh = {
            "ego_odometry": self._is_fresh(self.last_odom_ts),
            "objects": self._is_fresh(self.last_objects_ts),
            "trajectory": self._is_fresh(self.last_trajectory_ts),
            "control_cmd": self._is_fresh(self.last_control_ts),
        }
        optional_fresh = {
            "operation_mode": self._is_fresh(self.last_mode_ts),
        }
        ready = all(required_fresh.values())

        msg_ready = Bool()
        msg_ready.data = ready
        self.pub_ready.publish(msg_ready)

        status = {
            "ready": ready,
            "freshness_sec": self.freshness_sec,
            "required_fresh": required_fresh,
            "optional_fresh": optional_fresh,
        }
        msg_status = String()
        msg_status.data = json.dumps(status, ensure_ascii=False)
        self.pub_status.publish(msg_status)


def main() -> None:
    rclpy.init()
    node = AutowareIoHub()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
