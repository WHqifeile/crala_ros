from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Optional

from l3_interfaces.msg import GroundTruthObjectArray
from l3_interfaces.msg import HazardContext
from l3_interfaces.msg import RoadInfo
from nav_msgs.msg import Odometry, Path
import rclpy
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from std_msgs.msg import Bool, Float32, String


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def heading_unit_vector(yaw_deg: float) -> tuple[float, float]:
    yaw_rad = math.radians(yaw_deg)
    return (math.cos(yaw_rad), math.sin(yaw_rad))


def yaw_deg_from_quaternion(z: float, w: float) -> float:
    return math.degrees(2.0 * math.atan2(z, w))


@dataclass
class ProjectionResult:
    longitudinal_s: float
    lateral_l: float
    distance_to_path: float


class HazardAssessor:
    def __init__(
        self,
        lookahead_m: float = 50.0,
        lateral_margin_m: float = 0.5,
        static_speed_threshold_mps: float = 0.3,
    ) -> None:
        self.lookahead_m = lookahead_m
        self.lateral_margin_m = lateral_margin_m
        self.static_speed_threshold_mps = static_speed_threshold_mps

    def _project_point_to_path(
        self,
        point: tuple[float, float],
        path: list[tuple[float, float]],
    ) -> Optional[ProjectionResult]:
        if len(path) < 2:
            return None

        best_projection: Optional[ProjectionResult] = None
        traversed = 0.0
        px, py = point

        for index in range(len(path) - 1):
            x0, y0 = path[index]
            x1, y1 = path[index + 1]
            dx = x1 - x0
            dy = y1 - y0
            segment_length = math.hypot(dx, dy)
            if segment_length < 1e-6:
                continue

            ux = dx / segment_length
            uy = dy / segment_length
            rel_x = px - x0
            rel_y = py - y0
            along = clamp(rel_x * ux + rel_y * uy, 0.0, segment_length)
            proj_x = x0 + along * ux
            proj_y = y0 + along * uy
            off_x = px - proj_x
            off_y = py - proj_y
            lateral = off_x * (-uy) + off_y * ux
            distance = math.hypot(off_x, off_y)
            projection = ProjectionResult(
                longitudinal_s=traversed + along,
                lateral_l=lateral,
                distance_to_path=distance,
            )
            if best_projection is None or projection.distance_to_path < best_projection.distance_to_path:
                best_projection = projection
            traversed += segment_length

        return best_projection

    def assess(self, scene_state: dict[str, Any]) -> dict[str, Any]:
        ego_state = scene_state["ego_state"]
        road_info = scene_state["road_info"]
        route_points = scene_state["route_info"]["points"]
        objects = scene_state["objects_ground_truth"]

        path = [(ego_state["x"], ego_state["y"])]
        path.extend((point["x"], point["y"]) for point in route_points)
        if len(path) < 2:
            return self._clear_result()

        ego_heading = heading_unit_vector(ego_state["yaw"])
        corridor_half_width = max(float(road_info.get("lane_width", 3.5)) / 2.0 + self.lateral_margin_m, 1.5)
        best_match: Optional[dict[str, Any]] = None

        for obj in objects:
            rel_x = obj["x"] - ego_state["x"]
            rel_y = obj["y"] - ego_state["y"]
            forward_distance = rel_x * ego_heading[0] + rel_y * ego_heading[1]
            if forward_distance <= 0.0 or forward_distance > self.lookahead_m:
                continue
            if float(obj.get("speed", 0.0)) > self.static_speed_threshold_mps:
                continue

            projection = self._project_point_to_path((obj["x"], obj["y"]), path)
            if projection is None:
                continue
            if projection.longitudinal_s <= 0.0 or projection.longitudinal_s > self.lookahead_m:
                continue
            if abs(projection.lateral_l) > corridor_half_width:
                continue

            candidate = {"object": obj, "projection": projection}
            if best_match is None or projection.longitudinal_s < best_match["projection"].longitudinal_s:
                best_match = candidate

        if best_match is None:
            return self._clear_result()

        hazard_distance = round(float(best_match["projection"].longitudinal_s), 3)
        ego_speed = max(float(ego_state.get("speed", 0.0)), 0.0)
        hazard_ttc = None
        if ego_speed > 0.1:
            hazard_ttc = round(hazard_distance / ego_speed, 3)

        if hazard_distance < 12.0 or (hazard_ttc is not None and hazard_ttc < 1.5):
            risk_level = "URGENT"
            fallback_strategy = "MRM_STOP"
        elif hazard_distance < 25.0 or (hazard_ttc is not None and hazard_ttc < 3.0):
            risk_level = "HIGH"
            fallback_strategy = "KEEP_LANE_BRAKE"
        else:
            risk_level = "MEDIUM"
            fallback_strategy = "KEEP_LANE_BRAKE"

        takeover_deadline_s = 4.0
        if hazard_ttc is not None:
            takeover_deadline_s = max(min(hazard_ttc, 4.0), 1.0)

        return {
            "takeover_required": True,
            "obstacle_in_path": True,
            "lane_blocked": True,
            "trajectory_feasible": False,
            "hazard_distance_m": hazard_distance,
            "hazard_ttc_s": hazard_ttc,
            "takeover_deadline_s": round(float(takeover_deadline_s), 3),
            "reason_code": "STATIC_OBSTACLE_PATH_BLOCKED",
            "risk_level": risk_level,
            "fallback_strategy": fallback_strategy,
            "matched_object": best_match["object"],
        }

    @staticmethod
    def _clear_result() -> dict[str, Any]:
        return {
            "takeover_required": False,
            "obstacle_in_path": False,
            "lane_blocked": False,
            "trajectory_feasible": True,
            "hazard_distance_m": None,
            "hazard_ttc_s": None,
            "takeover_deadline_s": None,
            "reason_code": "CLEAR",
            "risk_level": "LOW",
            "fallback_strategy": "NONE",
            "matched_object": None,
        }


class HazardAssessorNode(Node):
    def __init__(self) -> None:
        super().__init__("hazard_assessor")
        self.declare_parameter("role_name", "ego_vehicle")
        self.declare_parameter("publish_rate_hz", 10.0)
        self.declare_parameter("hazard_lookahead_m", 50.0)
        self.declare_parameter("scene_timeout_sec", 1.0)

        self.role_name = str(self.get_parameter("role_name").value)
        self.publish_rate_hz = float(self.get_parameter("publish_rate_hz").value)
        self.scene_timeout_sec = float(self.get_parameter("scene_timeout_sec").value)
        self.assessor = HazardAssessor(lookahead_m=float(self.get_parameter("hazard_lookahead_m").value))

        self.scene_available = False
        self.latest_scene_stamp_sec: Optional[float] = None
        self.ego_state: Optional[dict[str, Any]] = None
        self.road_info: Optional[dict[str, Any]] = None
        self.route_info: Optional[dict[str, Any]] = None
        self.objects_ground_truth: Optional[list[dict[str, Any]]] = None

        self.create_subscription(Bool, "/carla/scene/available", self._on_scene_available, 10)
        self.create_subscription(Odometry, f"/carla/{self.role_name}/odometry", self._on_odometry, 10)
        self.create_subscription(Path, f"/carla/{self.role_name}/waypoints", self._on_waypoints, 10)
        self.create_subscription(RoadInfo, "/carla/scene/road_info", self._on_road_info, 10)
        self.create_subscription(GroundTruthObjectArray, "/carla/scene/objects_ground_truth", self._on_objects_ground_truth, 10)

        self.hazard_context_pub = self.create_publisher(HazardContext, "/carla/hazard/context", 10)
        self.hazard_pub = self.create_publisher(String, "/carla/scene/hazard", 10)
        self.takeover_required_pub = self.create_publisher(Bool, "/carla/hazard/takeover_required", 10)
        self.reason_code_pub = self.create_publisher(String, "/carla/hazard/reason_code", 10)
        self.hazard_distance_pub = self.create_publisher(Float32, "/carla/hazard/distance_m", 10)
        self.hazard_ttc_pub = self.create_publisher(Float32, "/carla/hazard/ttc_s", 10)
        self.scenario_system_available_pub = self.create_publisher(Bool, "/scenario/system_available", 10)
        self.scenario_automation_request_pub = self.create_publisher(Bool, "/scenario/automation_request", 10)
        self.scenario_td_request_pub = self.create_publisher(Bool, "/scenario/td_request", 10)
        self.scenario_hazard_reason_pub = self.create_publisher(String, "/scenario/hazard_reason", 10)

        self.timer = self.create_timer(1.0 / max(self.publish_rate_hz, 1.0), self._tick)
        self.get_logger().info("hazard_assessor started.")

    def _now_sec(self) -> float:
        return self.get_clock().now().nanoseconds / 1e9

    def _mark_scene_update(self) -> None:
        self.latest_scene_stamp_sec = self._now_sec()

    def _on_scene_available(self, msg: Bool) -> None:
        self.scene_available = msg.data
        self._mark_scene_update()

    def _on_odometry(self, msg: Odometry) -> None:
        position = msg.pose.pose.position
        orientation = msg.pose.pose.orientation
        linear = msg.twist.twist.linear
        self.ego_state = {
            "x": float(position.x),
            "y": float(position.y),
            "yaw": float(yaw_deg_from_quaternion(float(orientation.z), float(orientation.w))),
            "speed": float(math.sqrt(linear.x ** 2 + linear.y ** 2 + linear.z ** 2)),
        }
        self._mark_scene_update()

    def _on_road_info(self, msg: RoadInfo) -> None:
        self.road_info = {
            "road_id": int(msg.road_id),
            "lane_id": int(msg.lane_id),
            "lane_width": float(msg.lane_width),
            "is_junction": bool(msg.is_junction),
        }
        self._mark_scene_update()

    def _on_waypoints(self, msg: Path) -> None:
        points = []
        for pose in msg.poses[1:]:
            points.append(
                {
                    "x": float(pose.pose.position.x),
                    "y": float(pose.pose.position.y),
                    "yaw": float(
                        yaw_deg_from_quaternion(
                            float(pose.pose.orientation.z),
                            float(pose.pose.orientation.w),
                        )
                    ),
                }
            )
        self.route_info = {
            "lookahead_m": None,
            "points": points,
        }
        self._mark_scene_update()

    def _on_objects_ground_truth(self, msg: GroundTruthObjectArray) -> None:
        self.objects_ground_truth = [
            {
                "id": int(obj.id),
                "role_name": str(obj.role_name),
                "object_type": str(obj.object_type),
                "x": float(obj.x),
                "y": float(obj.y),
                "yaw": float(obj.yaw_deg),
                "speed": float(obj.speed_mps),
                "bbox": {
                    "extent_x": float(obj.bbox_x),
                    "extent_y": float(obj.bbox_y),
                    "extent_z": float(obj.bbox_z),
                },
            }
            for obj in msg.objects
        ]
        self._mark_scene_update()

    def _scene_fresh(self) -> bool:
        return self.latest_scene_stamp_sec is not None and (self._now_sec() - self.latest_scene_stamp_sec) <= self.scene_timeout_sec

    def _scene_ready(self) -> bool:
        return (
            self.scene_available
            and self._scene_fresh()
            and self.ego_state is not None
            and self.road_info is not None
            and self.route_info is not None
            and self.objects_ground_truth is not None
        )

    @staticmethod
    def _publish_json(publisher, payload: dict[str, Any]) -> None:
        msg = String()
        msg.data = json.dumps(payload, ensure_ascii=True)
        publisher.publish(msg)

    def _publish_scalar_hazard(self, hazard: dict[str, Any]) -> None:
        self.takeover_required_pub.publish(Bool(data=bool(hazard["takeover_required"])))
        self.reason_code_pub.publish(String(data=str(hazard["reason_code"])))
        self.hazard_distance_pub.publish(
            Float32(data=float(hazard["hazard_distance_m"]) if hazard["hazard_distance_m"] is not None else float("nan"))
        )
        self.hazard_ttc_pub.publish(
            Float32(data=float(hazard["hazard_ttc_s"]) if hazard["hazard_ttc_s"] is not None else float("nan"))
        )

    def _publish_hazard_context(self, hazard: dict[str, Any]) -> None:
        msg = HazardContext()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "map"
        msg.takeover_required = bool(hazard["takeover_required"])
        msg.obstacle_in_path = bool(hazard["obstacle_in_path"])
        msg.lane_blocked = bool(hazard["lane_blocked"])
        msg.trajectory_feasible = bool(hazard["trajectory_feasible"])
        msg.hazard_distance_m = float(hazard["hazard_distance_m"]) if hazard["hazard_distance_m"] is not None else float("nan")
        msg.hazard_ttc_s = float(hazard["hazard_ttc_s"]) if hazard["hazard_ttc_s"] is not None else float("nan")
        msg.takeover_deadline_s = float(hazard["takeover_deadline_s"]) if hazard["takeover_deadline_s"] is not None else float("nan")
        msg.reason_code = str(hazard["reason_code"])
        msg.risk_level = str(hazard["risk_level"])
        msg.fallback_strategy = str(hazard["fallback_strategy"])
        self.hazard_context_pub.publish(msg)

    def _tick(self) -> None:
        if not self._scene_ready():
            clear = HazardAssessor._clear_result()
            self.scenario_system_available_pub.publish(Bool(data=False))
            self.scenario_automation_request_pub.publish(Bool(data=False))
            self.scenario_td_request_pub.publish(Bool(data=False))
            self.scenario_hazard_reason_pub.publish(String(data="SCENE_UNAVAILABLE"))
            self._publish_json(self.hazard_pub, clear)
            self._publish_hazard_context(clear)
            self._publish_scalar_hazard(clear)
            return

        scene_state = {
            "ego_state": self.ego_state,
            "road_info": self.road_info,
            "route_info": self.route_info,
            "objects_ground_truth": self.objects_ground_truth,
        }
        hazard = self.assessor.assess(scene_state)

        self._publish_json(self.hazard_pub, hazard)
        self._publish_hazard_context(hazard)
        self._publish_scalar_hazard(hazard)
        self.scenario_system_available_pub.publish(Bool(data=True))
        self.scenario_automation_request_pub.publish(Bool(data=True))
        self.scenario_td_request_pub.publish(Bool(data=bool(hazard["takeover_required"])))
        self.scenario_hazard_reason_pub.publish(String(data=str(hazard["reason_code"])))


def main() -> None:
    rclpy.init()
    node = HazardAssessorNode()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
