#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

CARLA_ROOT = Path("/home/vci/sim/carla-0.9.15")
CARLA_PYTHONAPI = CARLA_ROOT / "PythonAPI" / "carla"

if str(CARLA_PYTHONAPI) not in sys.path:
    sys.path.append(str(CARLA_PYTHONAPI))

import carla
import numpy as np
import pygame

try:
    from shapely.geometry import Polygon as _ShapelyPolygon  # noqa: F401
except ModuleNotFoundError:
    import types

    def _segments_intersect(
        p1: tuple[float, float],
        p2: tuple[float, float],
        q1: tuple[float, float],
        q2: tuple[float, float],
    ) -> bool:
        def orientation(a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]) -> float:
            return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])

        def on_segment(a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]) -> bool:
            return (
                min(a[0], c[0]) - 1e-6 <= b[0] <= max(a[0], c[0]) + 1e-6
                and min(a[1], c[1]) - 1e-6 <= b[1] <= max(a[1], c[1]) + 1e-6
            )

        o1 = orientation(p1, p2, q1)
        o2 = orientation(p1, p2, q2)
        o3 = orientation(q1, q2, p1)
        o4 = orientation(q1, q2, p2)

        if (o1 > 0) != (o2 > 0) and (o3 > 0) != (o4 > 0):
            return True
        if abs(o1) < 1e-6 and on_segment(p1, q1, p2):
            return True
        if abs(o2) < 1e-6 and on_segment(p1, q2, p2):
            return True
        if abs(o3) < 1e-6 and on_segment(q1, p1, q2):
            return True
        if abs(o4) < 1e-6 and on_segment(q1, p2, q2):
            return True
        return False

    def _point_in_polygon(point: tuple[float, float], polygon: list[tuple[float, float]]) -> bool:
        x, y = point
        inside = False
        for index in range(len(polygon)):
            x1, y1 = polygon[index]
            x2, y2 = polygon[(index + 1) % len(polygon)]
            intersects = (y1 > y) != (y2 > y)
            if intersects:
                x_at_y = (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-9) + x1
                if x < x_at_y:
                    inside = not inside
        return inside

    class _FallbackPolygon:
        def __init__(self, points: list[list[float]] | list[tuple[float, ...]]) -> None:
            self.points = [(float(point[0]), float(point[1])) for point in points]

        def intersects(self, other: "_FallbackPolygon") -> bool:
            if len(self.points) < 3 or len(other.points) < 3:
                return False
            for index in range(len(self.points)):
                p1 = self.points[index]
                p2 = self.points[(index + 1) % len(self.points)]
                for other_index in range(len(other.points)):
                    q1 = other.points[other_index]
                    q2 = other.points[(other_index + 1) % len(other.points)]
                    if _segments_intersect(p1, p2, q1, q2):
                        return True
            if _point_in_polygon(self.points[0], other.points):
                return True
            if _point_in_polygon(other.points[0], self.points):
                return True
            return False

    geometry_module = types.ModuleType("shapely.geometry")
    geometry_module.Polygon = _FallbackPolygon
    shapely_module = types.ModuleType("shapely")
    shapely_module.geometry = geometry_module
    sys.modules["shapely"] = shapely_module
    sys.modules["shapely.geometry"] = geometry_module
    _ShapelyPolygon = _FallbackPolygon

from agents.navigation.basic_agent import BasicAgent


WINDOW_WIDTH = 1280
WINDOW_HEIGHT = 720
ROUTE_POINT_COUNT = 20
ROUTE_STEP_M = 2.0


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def normalize_angle_deg(angle_deg: float) -> float:
    while angle_deg > 180.0:
        angle_deg -= 360.0
    while angle_deg < -180.0:
        angle_deg += 360.0
    return angle_deg


def speed_mps(actor: carla.Actor) -> float:
    velocity = actor.get_velocity()
    return math.sqrt(velocity.x ** 2 + velocity.y ** 2 + velocity.z ** 2)


def distance_2d_xy(x0: float, y0: float, x1: float, y1: float) -> float:
    return math.hypot(x1 - x0, y1 - y0)


def location_xy(location: carla.Location) -> tuple[float, float]:
    return (float(location.x), float(location.y))


def heading_unit_vector(yaw_deg: float) -> tuple[float, float]:
    yaw_rad = math.radians(yaw_deg)
    return (math.cos(yaw_rad), math.sin(yaw_rad))


def quaternion_from_yaw_deg(yaw_deg: float) -> tuple[float, float, float, float]:
    half_yaw = math.radians(yaw_deg) / 2.0
    return (0.0, 0.0, math.sin(half_yaw), math.cos(half_yaw))


def safe_enum_name(value: Any) -> str:
    if value is None:
        return "None"
    text = str(value)
    if "." in text:
        return text.split(".")[-1]
    return text


def lane_marking_to_dict(marking: Any) -> dict[str, Any]:
    if marking is None:
        return {
            "type": "None",
            "color": "None",
            "lane_change": "None",
            "width": 0.0,
        }
    return {
        "type": safe_enum_name(getattr(marking, "type", None)),
        "color": safe_enum_name(getattr(marking, "color", None)),
        "lane_change": safe_enum_name(getattr(marking, "lane_change", None)),
        "width": round(float(getattr(marking, "width", 0.0)), 3),
    }


def waypoint_to_lane_summary(waypoint: Optional[carla.Waypoint]) -> Optional[dict[str, Any]]:
    if waypoint is None:
        return None
    return {
        "road_id": int(waypoint.road_id),
        "section_id": int(waypoint.section_id),
        "lane_id": int(waypoint.lane_id),
        "lane_type": safe_enum_name(waypoint.lane_type),
        "lane_width": round(float(waypoint.lane_width), 3),
        "lane_change": safe_enum_name(waypoint.lane_change),
        "is_junction": bool(waypoint.is_junction),
    }


def signed_lateral_offset_to_waypoint(location: carla.Location, waypoint: carla.Waypoint) -> float:
    wp_loc = waypoint.transform.location
    wp_yaw = waypoint.transform.rotation.yaw
    yaw_rad = math.radians(wp_yaw)

    # CARLA map yaw follows the waypoint tangent direction. This computes
    # signed lateral offset in the waypoint-local left-positive frame.
    left_x = -math.sin(yaw_rad)
    left_y = math.cos(yaw_rad)

    dx = float(location.x - wp_loc.x)
    dy = float(location.y - wp_loc.y)

    return dx * left_x + dy * left_y


RISK_RANK = {
    "LOW": 0,
    "MEDIUM": 1,
    "HIGH": 2,
    "CRITICAL": 3,
}


def max_risk_level(levels: list[str]) -> str:
    if not levels:
        return "LOW"
    return max(levels, key=lambda level: RISK_RANK.get(level, 0))


def scene_recommendation_from_risk(risk_level: str) -> str:
    if risk_level == "CRITICAL":
        return "MRM_OR_EMERGENCY_BRAKE_CANDIDATE"
    if risk_level == "HIGH":
        return "TOR_CANDIDATE"
    if risk_level == "MEDIUM":
        return "MONITOR_AND_PREPARE_TOR"
    return "NO_ACTION"


def road_option_name(road_option: Any) -> str:
    text = safe_enum_name(road_option)
    if text.startswith("RoadOption."):
        return text.split(".", 1)[1]
    return text


def waypoint_to_route_point(waypoint: carla.Waypoint, road_option: Any = None) -> dict[str, Any]:
    transform = waypoint.transform
    location = transform.location
    point = {
        "x": round(float(location.x), 3),
        "y": round(float(location.y), 3),
        "z": round(float(location.z), 3),
        "yaw": round(float(transform.rotation.yaw), 3),
        "road_id": int(waypoint.road_id),
        "section_id": int(waypoint.section_id),
        "lane_id": int(waypoint.lane_id),
        "lane_type": safe_enum_name(waypoint.lane_type),
        "lane_change": safe_enum_name(waypoint.lane_change),
        "lane_width": round(float(waypoint.lane_width), 3),
        "is_junction": bool(waypoint.is_junction),
    }
    if road_option is not None:
        point["road_option"] = road_option_name(road_option)
    return point


def bbox_polygon_from_pose(
    x: float,
    y: float,
    yaw_deg: float,
    extent_x: float,
    extent_y: float,
) -> list[list[float]]:
    yaw_rad = math.radians(yaw_deg)
    forward_x = math.cos(yaw_rad)
    forward_y = math.sin(yaw_rad)
    left_x = -math.sin(yaw_rad)
    left_y = math.cos(yaw_rad)

    corners = []
    for long_sign, lat_sign in ((1.0, 1.0), (1.0, -1.0), (-1.0, -1.0), (-1.0, 1.0)):
        corners.append(
            [
                round(float(x + long_sign * extent_x * forward_x + lat_sign * extent_y * left_x), 3),
                round(float(y + long_sign * extent_x * forward_y + lat_sign * extent_y * left_y), 3),
            ]
        )
    return corners


def build_path_corridor_polygon(path: list[tuple[float, float]], half_width: float) -> Optional[Any]:
    if len(path) < 2:
        return None

    left_points: list[tuple[float, float]] = []
    right_points: list[tuple[float, float]] = []
    for index, (x, y) in enumerate(path):
        if index == 0:
            tx = path[1][0] - x
            ty = path[1][1] - y
        elif index == len(path) - 1:
            tx = x - path[index - 1][0]
            ty = y - path[index - 1][1]
        else:
            tx = path[index + 1][0] - path[index - 1][0]
            ty = path[index + 1][1] - path[index - 1][1]

        length = math.hypot(tx, ty)
        if length < 1e-6:
            continue
        nx = -ty / length
        ny = tx / length
        left_points.append((x + nx * half_width, y + ny * half_width))
        right_points.append((x - nx * half_width, y - ny * half_width))

    if len(left_points) < 2 or len(right_points) < 2:
        return None
    return _ShapelyPolygon(left_points + list(reversed(right_points)))


def polygon_intersects(points: list[list[float]] | list[tuple[float, float]], polygon: Any) -> bool:
    if polygon is None or len(points) < 3:
        return False
    return bool(_ShapelyPolygon(points).intersects(polygon))


def build_clear_external_risk(sim_time_s: float) -> dict[str, Any]:
    return {
        "sim_time_s": round(float(sim_time_s), 3),
        "has_external_hazard": False,
        "primary_hazard_type": "CLEAR",
        "external_risk_level": "LOW",
        "scene_recommendation": "NO_ACTION",
        "hazard_distance_m": None,
        "hazard_ttc_s": None,
        "matched_object": None,
        "hazards": [],
    }


@dataclass
class DriverState:
    driver_available: bool = True
    hands_on: bool = True
    attention_on_road: bool = True

    def response_ready(self) -> bool:
        return self.driver_available and self.hands_on and self.attention_on_road


@dataclass
class ControlIntent:
    quit_requested: bool = False
    toggle_auto_requested: bool = False
    reroute_requested: bool = False
    respawn_obstacle_requested: bool = False
    reset_mrm_requested: bool = False
    scripted_toggle_manual_requested: bool = False


@dataclass
class ProjectionResult:
    longitudinal_s: float
    lateral_l: float
    distance_to_path: float


@dataclass
class AutoDriveDecision:
    action: str = "KEEP_LANE"
    lead_vehicle_id: Optional[int] = None
    lead_distance_m: Optional[float] = None
    lead_speed_mps: Optional[float] = None
    relative_speed_mps: Optional[float] = None
    ttc_s: Optional[float] = None
    target_lane_id: Optional[int] = None
    override_control: Optional[carla.VehicleControl] = None


class BasicAgentRosPublisher:
    def __init__(self, role_name: str, topic_namespace: str) -> None:
        try:
            import rclpy
            from geometry_msgs.msg import PoseStamped
            from nav_msgs.msg import Path as RosPath
            from std_msgs.msg import String
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "ROS publishing requires ROS 2 Python packages. Source "
                "/opt/ros/humble/setup.bash and this workspace setup before running."
            ) from exc

        try:
            from l3_interfaces.msg import RoadInfo
        except ModuleNotFoundError:
            RoadInfo = None

        self.rclpy = rclpy
        self.PoseStamped = PoseStamped
        self.RosPath = RosPath
        self.String = String
        self.RoadInfo = RoadInfo
        self.role_name = role_name
        self.topic_namespace = "/" + topic_namespace.strip("/")
        self.ros_initialized_here = False

        if not rclpy.ok():
            rclpy.init(args=[])
            self.ros_initialized_here = True

        self.node = rclpy.create_node("basic_agent_scene_bridge")
        self.scene_state_pub = self.node.create_publisher(String, f"{self.topic_namespace}/scene/state", 10)
        self.ego_waypoints_pub = self.node.create_publisher(
            RosPath,
            f"{self.topic_namespace}/{self.role_name}/waypoints",
            10,
        )
        self.road_info_pub = None
        if RoadInfo is not None:
            self.road_info_pub = self.node.create_publisher(
                RoadInfo,
                f"{self.topic_namespace}/scene/road_info",
                10,
            )

        # JSON semantic outputs for the external scene understanding layer.
        # These are intentionally not final L3/DMS fused decisions.
        self.external_semantics_pub = self.node.create_publisher(
            String,
            f"{self.topic_namespace}/scene/external_semantics",
            10,
        )
        self.external_risk_pub = self.node.create_publisher(
            String,
            f"{self.topic_namespace}/scene/external_risk",
            10,
        )

    def publish(
        self,
        scene_state: dict[str, Any],
        external_understanding: dict[str, Any],
    ) -> None:
        stamp = self.node.get_clock().now().to_msg()
        self._publish_scene_state(scene_state)
        self._publish_waypoints(scene_state, stamp)
        self._publish_road_info(scene_state["road_info"], stamp)

        self._publish_json(
            self.external_semantics_pub,
            external_understanding["external_semantics"],
        )
        self._publish_json(
            self.external_risk_pub,
            external_understanding["external_risk"],
        )

        self.rclpy.spin_once(self.node, timeout_sec=0.0)

    def _publish_json(self, publisher: Any, payload: dict[str, Any]) -> None:
        msg = self.String()
        msg.data = json.dumps(payload, ensure_ascii=True)
        publisher.publish(msg)

    def _publish_scene_state(self, scene_state: dict[str, Any]) -> None:
        msg = self.String()
        msg.data = json.dumps(scene_state, ensure_ascii=True)
        self.scene_state_pub.publish(msg)

    def _publish_waypoints(self, scene_state: dict[str, Any], stamp: Any) -> None:
        path_msg = self.RosPath()
        path_msg.header.stamp = stamp
        path_msg.header.frame_id = "map"

        ego_state = scene_state["ego_state"]
        ego_pose = self.PoseStamped()
        ego_pose.header = path_msg.header
        ego_pose.pose.position.x = float(ego_state["x"])
        ego_pose.pose.position.y = float(ego_state["y"])
        qx, qy, qz, qw = quaternion_from_yaw_deg(float(ego_state["yaw"]))
        ego_pose.pose.orientation.x = qx
        ego_pose.pose.orientation.y = qy
        ego_pose.pose.orientation.z = qz
        ego_pose.pose.orientation.w = qw
        path_msg.poses.append(ego_pose)

        for point in scene_state["route_info"].get("points", []):
            pose = self.PoseStamped()
            pose.header = path_msg.header
            pose.pose.position.x = float(point["x"])
            pose.pose.position.y = float(point["y"])
            qx, qy, qz, qw = quaternion_from_yaw_deg(float(point["yaw"]))
            pose.pose.orientation.x = qx
            pose.pose.orientation.y = qy
            pose.pose.orientation.z = qz
            pose.pose.orientation.w = qw
            path_msg.poses.append(pose)

        self.ego_waypoints_pub.publish(path_msg)

    def _publish_road_info(self, road_info: dict[str, Any], stamp: Any) -> None:
        if self.road_info_pub is None or self.RoadInfo is None:
            return
        msg = self.RoadInfo()
        msg.header.stamp = stamp
        msg.header.frame_id = "map"
        msg.road_id = int(road_info["road_id"])
        msg.lane_id = int(road_info["lane_id"])
        msg.lane_width = float(road_info["lane_width"])
        msg.is_junction = bool(road_info["is_junction"])
        self.road_info_pub.publish(msg)

    def close(self) -> None:
        self.node.destroy_node()
        if self.ros_initialized_here and self.rclpy.ok():
            self.rclpy.shutdown()


class EventRecorder:
    def __init__(self, log_root: Path) -> None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = log_root / f"basic_agent_hmi_{timestamp}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.run_dir / "events.jsonl"
        self.ticks_path = self.run_dir / "ticks.jsonl"
        self.summary_path = self.run_dir / "summary.json"
        self._event_file = self.events_path.open("w", encoding="utf-8")
        self._tick_file = self.ticks_path.open("w", encoding="utf-8")
        self.event_times: dict[str, float] = {}
        self.metrics = {
            "risk_appeared_count": 0,
            "tor_count": 0,
            "escalated_tor_count": 0,
            "mrm_count": 0,
            "takeover_success_count": 0,
            "collision_count": 0,
        }

    def record_event(self, event_type: str, sim_time_s: float, payload: Optional[dict[str, Any]] = None) -> None:
        record = {
            "sim_time_s": round(sim_time_s, 3),
            "event": event_type,
            "payload": payload or {},
        }
        self._event_file.write(json.dumps(record, ensure_ascii=True) + "\n")
        self._event_file.flush()

        if event_type == "risk_appeared":
            self.metrics["risk_appeared_count"] += 1
            self.event_times.setdefault("t0_risk_appeared", sim_time_s)
        elif event_type == "tor_active":
            self.metrics["tor_count"] += 1
            self.event_times.setdefault("t1_tor_active", sim_time_s)
        elif event_type == "escalated_tor":
            self.metrics["escalated_tor_count"] += 1
            self.event_times.setdefault("t2_escalated_tor", sim_time_s)
        elif event_type == "driver_first_response":
            self.event_times.setdefault("t3_driver_first_response", sim_time_s)
        elif event_type == "takeover_success":
            self.metrics["takeover_success_count"] += 1
            self.event_times.setdefault("t4_takeover_success", sim_time_s)
        elif event_type == "mrm_triggered":
            self.metrics["mrm_count"] += 1
            self.event_times.setdefault("t5_mrm_triggered", sim_time_s)
        elif event_type == "collision":
            self.metrics["collision_count"] += 1
        elif event_type == "experiment_end":
            self.event_times.setdefault("t6_experiment_end", sim_time_s)

    def record_tick(self, record: dict[str, Any]) -> None:
        self._tick_file.write(json.dumps(record, ensure_ascii=True) + "\n")
        self._tick_file.flush()

    def finalize(self, sim_time_s: float) -> None:
        self.record_event("experiment_end", sim_time_s, {"event_times": self.event_times})
        risk_count = max(self.metrics["risk_appeared_count"], 1)
        summary = {
            "run_dir": str(self.run_dir),
            "event_times": self.event_times,
            "metrics": self.metrics,
            "rates": {
                "first_warning_rate": self.metrics["tor_count"] / risk_count,
                "escalated_warning_rate": self.metrics["escalated_tor_count"] / risk_count,
                "mrm_rate": self.metrics["mrm_count"] / risk_count,
                "takeover_success_rate": self.metrics["takeover_success_count"] / risk_count,
                "collision_rate": 1.0 if self.metrics["collision_count"] > 0 else 0.0,
            },
        }
        self.summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        self._event_file.close()
        self._tick_file.close()


class CollisionSensor:
    def __init__(self, world: carla.World, ego_vehicle: carla.Vehicle, recorder: EventRecorder) -> None:
        blueprint = world.get_blueprint_library().find("sensor.other.collision")
        self.sensor = world.spawn_actor(blueprint, carla.Transform(), attach_to=ego_vehicle)
        self._recorder = recorder
        self._last_collision_frame: Optional[int] = None
        self.sensor.listen(self._on_collision)

    def _on_collision(self, event: carla.CollisionEvent) -> None:
        frame = int(event.frame)
        if self._last_collision_frame == frame:
            return
        self._last_collision_frame = frame
        other_actor = event.other_actor
        impulse = event.normal_impulse
        self._recorder.record_event(
            "collision",
            float(event.timestamp),
            {
                "other_actor_id": int(other_actor.id),
                "other_actor_type": other_actor.type_id,
                "normal_impulse": {
                    "x": round(float(impulse.x), 3),
                    "y": round(float(impulse.y), 3),
                    "z": round(float(impulse.z), 3),
                },
            },
        )

    def destroy(self) -> None:
        if self.sensor.is_alive:
            self.sensor.stop()
            self.sensor.destroy()


class ChaseCamera:
    def __init__(self, world: carla.World, ego_vehicle: carla.Vehicle) -> None:
        self._surface: Optional[pygame.Surface] = None
        self._lock = threading.Lock()
        blueprint = world.get_blueprint_library().find("sensor.camera.rgb")
        blueprint.set_attribute("image_size_x", str(WINDOW_WIDTH))
        blueprint.set_attribute("image_size_y", str(WINDOW_HEIGHT))
        blueprint.set_attribute("fov", "100")
        camera_transform = carla.Transform(
            carla.Location(x=-6.5, z=2.6),
            carla.Rotation(pitch=-12.0),
        )
        self.sensor = world.spawn_actor(blueprint, camera_transform, attach_to=ego_vehicle)
        self.sensor.listen(self._on_image)

    def _on_image(self, image: carla.Image) -> None:
        array = np.frombuffer(image.raw_data, dtype=np.uint8)
        array = np.reshape(array, (image.height, image.width, 4))
        rgb = array[:, :, :3][:, :, ::-1]
        surface = pygame.surfarray.make_surface(np.swapaxes(rgb, 0, 1))
        with self._lock:
            self._surface = surface

    def render(self, display: pygame.Surface) -> None:
        with self._lock:
            surface = self._surface
        if surface is None:
            display.fill((15, 15, 15))
            return
        display.blit(surface, (0, 0))

    def destroy(self) -> None:
        if self.sensor.is_alive:
            self.sensor.stop()
            self.sensor.destroy()


class KeyboardController:
    def __init__(self) -> None:
        self._manual_control = carla.VehicleControl()
        self._reverse = False

    def poll_events(self, driver_state: DriverState) -> ControlIntent:
        intent = ControlIntent()
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                intent.quit_requested = True
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    intent.quit_requested = True
                elif event.key == pygame.K_p:
                    intent.toggle_auto_requested = True
                elif event.key == pygame.K_r:
                    intent.reroute_requested = True
                elif event.key == pygame.K_o:
                    intent.respawn_obstacle_requested = True
                elif event.key == pygame.K_y:
                    intent.reset_mrm_requested = True
                elif event.key == pygame.K_1:
                    driver_state.driver_available = not driver_state.driver_available
                elif event.key == pygame.K_2:
                    driver_state.hands_on = not driver_state.hands_on
                elif event.key == pygame.K_3:
                    driver_state.attention_on_road = not driver_state.attention_on_road
                elif event.key == pygame.K_q:
                    self._reverse = not self._reverse
        return intent

    def manual_override_requested(self) -> bool:
        keys = pygame.key.get_pressed()
        return bool(
            keys[pygame.K_w]
            or keys[pygame.K_a]
            or keys[pygame.K_s]
            or keys[pygame.K_d]
            or keys[pygame.K_SPACE]
        )

    def build_manual_control(self) -> carla.VehicleControl:
        keys = pygame.key.get_pressed()
        control = self._manual_control

        if keys[pygame.K_w]:
            control.throttle = 0.65
        else:
            control.throttle = 0.0

        if keys[pygame.K_s]:
            control.brake = 0.6
        else:
            control.brake = 0.0

        if keys[pygame.K_a]:
            control.steer = clamp(control.steer - 0.05, -0.8, 0.8)
        elif keys[pygame.K_d]:
            control.steer = clamp(control.steer + 0.05, -0.8, 0.8)
        else:
            control.steer *= 0.65

        control.hand_brake = bool(keys[pygame.K_SPACE])
        control.reverse = self._reverse
        return control


class GroundTruthProvider:
    def __init__(self, world: carla.World, ego_vehicle: carla.Vehicle, role_name: str) -> None:
        self.world = world
        self.map = world.get_map()
        self.ego_vehicle = ego_vehicle
        self.role_name = role_name

    def _choose_next_waypoint(self, waypoint: carla.Waypoint, step_m: float) -> Optional[carla.Waypoint]:
        candidates = waypoint.next(step_m)
        if not candidates:
            return None
        current_yaw = waypoint.transform.rotation.yaw
        return min(
            candidates,
            key=lambda candidate: abs(normalize_angle_deg(candidate.transform.rotation.yaw - current_yaw)),
        )

    def _forward_route_points(self, start_waypoint: carla.Waypoint, count: int, step_m: float) -> list[dict[str, Any]]:
        route_points: list[dict[str, Any]] = []
        waypoint = start_waypoint
        for _ in range(count):
            waypoint = self._choose_next_waypoint(waypoint, step_m)
            if waypoint is None:
                break
            route_points.append(waypoint_to_route_point(waypoint, road_option="LANEFOLLOW"))
        return route_points

    def _road_info(self, ego_location: carla.Location, ego_waypoint: carla.Waypoint) -> dict[str, Any]:
        left_lane = ego_waypoint.get_left_lane()
        right_lane = ego_waypoint.get_right_lane()
        ego_lateral_offset_m = signed_lateral_offset_to_waypoint(ego_location, ego_waypoint)

        traffic_light_state = None
        traffic_light_id = None
        if self.ego_vehicle.is_at_traffic_light():
            traffic_light = self.ego_vehicle.get_traffic_light()
            if traffic_light is not None:
                traffic_light_state = safe_enum_name(traffic_light.get_state())
                traffic_light_id = int(traffic_light.id)

        return {
            "map_name": self.map.name,
            "road_id": int(ego_waypoint.road_id),
            "section_id": int(ego_waypoint.section_id),
            "lane_id": int(ego_waypoint.lane_id),
            "s": round(float(ego_waypoint.s), 3),

            "lane_type": safe_enum_name(ego_waypoint.lane_type),
            "lane_change": safe_enum_name(ego_waypoint.lane_change),
            "lane_width": round(float(ego_waypoint.lane_width), 3),
            "is_junction": bool(ego_waypoint.is_junction),

            "speed_limit_kmh": round(float(self.ego_vehicle.get_speed_limit()), 3),
            "ego_lateral_offset_m": round(float(ego_lateral_offset_m), 3),
            "ego_lane_center_x": round(float(ego_waypoint.transform.location.x), 3),
            "ego_lane_center_y": round(float(ego_waypoint.transform.location.y), 3),
            "ego_lane_yaw": round(float(ego_waypoint.transform.rotation.yaw), 3),

            "left_lane_marking": lane_marking_to_dict(ego_waypoint.left_lane_marking),
            "right_lane_marking": lane_marking_to_dict(ego_waypoint.right_lane_marking),

            "left_lane": waypoint_to_lane_summary(left_lane),
            "right_lane": waypoint_to_lane_summary(right_lane),

            "left_lane_available": bool(
                left_lane is not None
                and left_lane.lane_type == carla.LaneType.Driving
                and not left_lane.is_junction
            ),
            "right_lane_available": bool(
                right_lane is not None
                and right_lane.lane_type == carla.LaneType.Driving
                and not right_lane.is_junction
            ),

            "traffic_light": {
                "is_at_traffic_light": bool(self.ego_vehicle.is_at_traffic_light()),
                "traffic_light_id": traffic_light_id,
                "state": traffic_light_state,
            },
        }

    def _objects_ground_truth(self, ego_location: carla.Location, max_distance_m: float) -> list[dict[str, Any]]:
        objects: list[dict[str, Any]] = []

        ego_transform = self.ego_vehicle.get_transform()
        ego_heading = heading_unit_vector(float(ego_transform.rotation.yaw))

        for actor in self.world.get_actors():
            if actor.id == self.ego_vehicle.id:
                continue

            if not (actor.type_id.startswith("vehicle.") or actor.type_id.startswith("walker.")):
                continue

            actor_location = actor.get_location()
            distance_to_ego = float(actor_location.distance(ego_location))
            if distance_to_ego > max_distance_m:
                continue

            actor_transform = actor.get_transform()
            velocity = actor.get_velocity()
            acceleration = actor.get_acceleration()
            bbox_extent = actor.bounding_box.extent
            bbox_local = actor.bounding_box.location
            bbox_yaw_rad = math.radians(float(actor_transform.rotation.yaw))
            bbox_forward_x = math.cos(bbox_yaw_rad)
            bbox_forward_y = math.sin(bbox_yaw_rad)
            bbox_left_x = -math.sin(bbox_yaw_rad)
            bbox_left_y = math.cos(bbox_yaw_rad)
            bbox_world_x = (
                float(actor_location.x)
                + float(bbox_local.x) * bbox_forward_x
                + float(bbox_local.y) * bbox_left_x
            )
            bbox_world_y = (
                float(actor_location.y)
                + float(bbox_local.x) * bbox_forward_y
                + float(bbox_local.y) * bbox_left_y
            )
            bbox_world_z = float(actor_location.z + bbox_local.z)

            rel_x = float(actor_location.x - ego_location.x)
            rel_y = float(actor_location.y - ego_location.y)
            forward_distance = rel_x * ego_heading[0] + rel_y * ego_heading[1]
            lateral_distance = rel_x * (-ego_heading[1]) + rel_y * ego_heading[0]

            actor_waypoint = self.map.get_waypoint(
                actor_location,
                project_to_road=True,
                lane_type=carla.LaneType.Driving,
            )

            objects.append(
                {
                    "id": int(actor.id),
                    "role_name": actor.attributes.get("role_name", ""),
                    "object_type": actor.type_id,

                    "x": round(float(actor_location.x), 3),
                    "y": round(float(actor_location.y), 3),
                    "z": round(float(actor_location.z), 3),
                    "yaw": round(float(actor_transform.rotation.yaw), 3),

                    "vx": round(float(velocity.x), 3),
                    "vy": round(float(velocity.y), 3),
                    "vz": round(float(velocity.z), 3),
                    "ax": round(float(acceleration.x), 3),
                    "ay": round(float(acceleration.y), 3),
                    "az": round(float(acceleration.z), 3),
                    "speed": round(speed_mps(actor), 3),

                    "distance_to_ego_m": round(distance_to_ego, 3),
                    "forward_distance_m": round(float(forward_distance), 3),
                    "lateral_distance_m": round(float(lateral_distance), 3),

                    "road_id": int(actor_waypoint.road_id) if actor_waypoint is not None else None,
                    "section_id": int(actor_waypoint.section_id) if actor_waypoint is not None else None,
                    "lane_id": int(actor_waypoint.lane_id) if actor_waypoint is not None else None,
                    "lane_type": safe_enum_name(actor_waypoint.lane_type) if actor_waypoint is not None else None,
                    "lane_change": safe_enum_name(actor_waypoint.lane_change) if actor_waypoint is not None else None,
                    "is_junction": bool(actor_waypoint.is_junction) if actor_waypoint is not None else None,

                    "bbox": {
                        "extent_x": round(float(bbox_extent.x), 3),
                        "extent_y": round(float(bbox_extent.y), 3),
                        "extent_z": round(float(bbox_extent.z), 3),
                        "center_x": round(float(bbox_world_x), 3),
                        "center_y": round(float(bbox_world_y), 3),
                        "center_z": round(float(bbox_world_z), 3),
                    },
                    "bbox_polygon": bbox_polygon_from_pose(
                        float(bbox_world_x),
                        float(bbox_world_y),
                        float(actor_transform.rotation.yaw),
                        float(bbox_extent.x),
                        float(bbox_extent.y),
                    ),
                }
            )

        objects.sort(key=lambda entry: entry["id"])
        return objects

    def get_scene_state(
        self,
        snapshot: carla.WorldSnapshot,
        planned_route_points: Optional[list[dict[str, Any]]] = None,
        planned_route_source: str = "map_forward_sampling",
    ) -> dict[str, Any]:
        ego_transform = self.ego_vehicle.get_transform()
        ego_location = ego_transform.location
        ego_velocity = self.ego_vehicle.get_velocity()
        ego_acceleration = self.ego_vehicle.get_acceleration()

        ego_waypoint = self.map.get_waypoint(
            ego_location,
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        if planned_route_points is not None:
            route_points = planned_route_points[:ROUTE_POINT_COUNT]
            route_source = planned_route_source
        else:
            route_points = self._forward_route_points(ego_waypoint, ROUTE_POINT_COUNT, ROUTE_STEP_M)
            route_source = "map_forward_sampling"

        scene_state = {
            "sim_time_s": round(float(snapshot.timestamp.elapsed_seconds), 3),
            "ego_state": {
                "id": int(self.ego_vehicle.id),
                "role_name": self.role_name,

                "x": round(float(ego_location.x), 3),
                "y": round(float(ego_location.y), 3),
                "z": round(float(ego_location.z), 3),
                "yaw": round(float(ego_transform.rotation.yaw), 3),

                "vx": round(float(ego_velocity.x), 3),
                "vy": round(float(ego_velocity.y), 3),
                "vz": round(float(ego_velocity.z), 3),
                "ax": round(float(ego_acceleration.x), 3),
                "ay": round(float(ego_acceleration.y), 3),
                "az": round(float(ego_acceleration.z), 3),

                "speed": round(speed_mps(self.ego_vehicle), 3),
                "speed_limit_kmh": round(float(self.ego_vehicle.get_speed_limit()), 3),
            },
            "road_info": self._road_info(ego_location, ego_waypoint),
            "route_info": {
                "lookahead_m": round(ROUTE_POINT_COUNT * ROUTE_STEP_M, 3),
                "step_m": round(float(ROUTE_STEP_M), 3),
                "point_count": int(len(route_points)),
                "source": route_source,
                "points": route_points,
            },
            "objects_ground_truth": self._objects_ground_truth(ego_location, max_distance_m=80.0),
        }
        return scene_state


class ExternalSceneUnderstanding:
    """Deterministic external scene semantic understanding for ROS2.

    This class deliberately does not fuse DMS and does not make the final L3
    takeover decision. It produces structured external semantics and external
    risk candidates for a downstream L3 fusion node.
    """

    def __init__(
        self,
        lookahead_m: float = 50.0,
        lateral_margin_m: float = 0.5,
        static_speed_threshold_mps: float = 0.5,
        pedestrian_prediction_horizon_s: float = 3.0,
        pedestrian_prediction_step_s: float = 0.5,
        cut_in_prediction_horizon_s: float = 2.5,
        vehicle_prediction_horizon_s: float = 3.0,
        prediction_step_s: float = 0.5,
        comfortable_decel_mps2: float = 3.5,
        emergency_decel_mps2: float = 6.0,
        reaction_time_s: float = 1.0,
        safety_margin_m: float = 4.0,
    ) -> None:
        self.lookahead_m = lookahead_m
        self.lateral_margin_m = lateral_margin_m
        self.static_speed_threshold_mps = static_speed_threshold_mps
        self.pedestrian_prediction_horizon_s = pedestrian_prediction_horizon_s
        self.pedestrian_prediction_step_s = pedestrian_prediction_step_s
        self.cut_in_prediction_horizon_s = cut_in_prediction_horizon_s
        self.vehicle_prediction_horizon_s = vehicle_prediction_horizon_s
        self.prediction_step_s = prediction_step_s
        self.comfortable_decel_mps2 = comfortable_decel_mps2
        self.emergency_decel_mps2 = emergency_decel_mps2
        self.reaction_time_s = reaction_time_s
        self.safety_margin_m = safety_margin_m

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

    def _build_path(self, scene_state: dict[str, Any]) -> list[tuple[float, float]]:
        ego_state = scene_state["ego_state"]
        route_points = scene_state["route_info"].get("points", [])
        path = [(float(ego_state["x"]), float(ego_state["y"]))]
        path.extend((float(point["x"]), float(point["y"])) for point in route_points)
        return path

    def _corridor_half_width(self, scene_state: dict[str, Any]) -> float:
        lane_width = float(scene_state["road_info"].get("lane_width", 3.5))
        return max(lane_width / 2.0 + self.lateral_margin_m, 1.5)

    def _corridor_polygon(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> Optional[Any]:
        return build_path_corridor_polygon(path, self._corridor_half_width(scene_state))

    def _prediction_times(self, horizon_s: float, step_s: Optional[float] = None) -> list[float]:
        step = step_s or self.prediction_step_s
        count = max(int(math.ceil(horizon_s / step)), 1)
        return [round(min(index * step, horizon_s), 3) for index in range(1, count + 1)]

    def _object_future_pose(self, obj: dict[str, Any], t_s: float) -> tuple[float, float, float]:
        x = float(obj["x"]) + float(obj.get("vx", 0.0)) * t_s + 0.5 * float(obj.get("ax", 0.0)) * t_s * t_s
        y = float(obj["y"]) + float(obj.get("vy", 0.0)) * t_s + 0.5 * float(obj.get("ay", 0.0)) * t_s * t_s
        yaw = float(obj.get("yaw", 0.0))
        vx = float(obj.get("vx", 0.0)) + float(obj.get("ax", 0.0)) * t_s
        vy = float(obj.get("vy", 0.0)) + float(obj.get("ay", 0.0)) * t_s
        if math.hypot(vx, vy) > 0.5:
            yaw = math.degrees(math.atan2(vy, vx))
        return x, y, yaw

    def _object_polygon(self, obj: dict[str, Any], t_s: float = 0.0) -> list[list[float]]:
        bbox = obj.get("bbox", {})
        x, y, yaw = self._object_future_pose(obj, t_s)
        return bbox_polygon_from_pose(
            x,
            y,
            yaw,
            float(bbox.get("extent_x", 0.5)),
            float(bbox.get("extent_y", 0.5)),
        )

    def _best_polygon_projection(
        self,
        points: list[list[float]],
        path: list[tuple[float, float]],
        fallback_point: tuple[float, float],
    ) -> Optional[ProjectionResult]:
        candidates: list[ProjectionResult] = []
        fallback_projection = self._project_point_to_path(fallback_point, path)
        if fallback_projection is not None:
            candidates.append(fallback_projection)
        for point in points:
            projection = self._project_point_to_path((float(point[0]), float(point[1])), path)
            if projection is not None:
                candidates.append(projection)
        if not candidates:
            return None
        valid_forward = [candidate for candidate in candidates if candidate.longitudinal_s >= 0.0]
        candidates = valid_forward or candidates
        return min(candidates, key=lambda candidate: candidate.longitudinal_s)

    def _path_tangent_at_projection(
        self,
        path: list[tuple[float, float]],
        longitudinal_s: float,
    ) -> tuple[float, float]:
        if len(path) < 2:
            return (1.0, 0.0)
        traversed = 0.0
        for index in range(len(path) - 1):
            x0, y0 = path[index]
            x1, y1 = path[index + 1]
            dx = x1 - x0
            dy = y1 - y0
            segment_length = math.hypot(dx, dy)
            if segment_length < 1e-6:
                continue
            if traversed + segment_length >= longitudinal_s:
                return (dx / segment_length, dy / segment_length)
            traversed += segment_length
        x0, y0 = path[-2]
        x1, y1 = path[-1]
        length = math.hypot(x1 - x0, y1 - y0)
        if length < 1e-6:
            return (1.0, 0.0)
        return ((x1 - x0) / length, (y1 - y0) / length)

    def _relative_path_kinematics(
        self,
        scene_state: dict[str, Any],
        obj: dict[str, Any],
        path: list[tuple[float, float]],
        projection: ProjectionResult,
    ) -> dict[str, float]:
        tangent_x, tangent_y = self._path_tangent_at_projection(path, projection.longitudinal_s)
        ego_state = scene_state["ego_state"]
        ego_v = float(ego_state.get("vx", 0.0)) * tangent_x + float(ego_state.get("vy", 0.0)) * tangent_y
        ego_a = float(ego_state.get("ax", 0.0)) * tangent_x + float(ego_state.get("ay", 0.0)) * tangent_y
        obj_v = float(obj.get("vx", 0.0)) * tangent_x + float(obj.get("vy", 0.0)) * tangent_y
        obj_a = float(obj.get("ax", 0.0)) * tangent_x + float(obj.get("ay", 0.0)) * tangent_y
        closing_speed = max(ego_v - obj_v, 0.0)
        relative_accel = ego_a - obj_a
        return {
            "ego_along_speed_mps": round(float(ego_v), 3),
            "object_along_speed_mps": round(float(obj_v), 3),
            "ego_along_accel_mps2": round(float(ego_a), 3),
            "object_along_accel_mps2": round(float(obj_a), 3),
            "closing_speed_mps": round(float(closing_speed), 3),
            "relative_accel_mps2": round(float(relative_accel), 3),
        }

    def _time_to_collision(self, distance_m: float, closing_speed_mps: float, relative_accel_mps2: float = 0.0) -> Optional[float]:
        if distance_m <= 0.0:
            return 0.0
        if abs(relative_accel_mps2) < 1e-3:
            if closing_speed_mps <= 0.1:
                return None
            return distance_m / closing_speed_mps

        # Solve 0.5*a*t^2 + v*t - d = 0 for the earliest positive t.
        a = 0.5 * relative_accel_mps2
        b = closing_speed_mps
        c = -distance_m
        discriminant = b * b - 4.0 * a * c
        if discriminant < 0.0:
            return None
        sqrt_discriminant = math.sqrt(discriminant)
        roots = [
            (-b - sqrt_discriminant) / (2.0 * a),
            (-b + sqrt_discriminant) / (2.0 * a),
        ]
        positive_roots = [root for root in roots if root > 0.0]
        return min(positive_roots) if positive_roots else None

    def _adaptive_thresholds(self, scene_state: dict[str, Any]) -> dict[str, float]:
        ego_speed = max(float(scene_state["ego_state"].get("speed", 0.0)), 0.0)
        comfortable_braking_distance = (
            ego_speed * self.reaction_time_s
            + ego_speed * ego_speed / max(2.0 * self.comfortable_decel_mps2, 1e-6)
            + self.safety_margin_m
        )
        emergency_braking_distance = (
            ego_speed * 0.5
            + ego_speed * ego_speed / max(2.0 * self.emergency_decel_mps2, 1e-6)
            + self.safety_margin_m * 0.5
        )
        traffic_density = len(scene_state.get("objects_ground_truth", []))
        density_margin = min(traffic_density * 0.15, 4.0)
        critical_distance = min(self.lookahead_m, emergency_braking_distance + 2.0)
        high_distance = min(
            self.lookahead_m,
            max(critical_distance + 1.0, comfortable_braking_distance + 6.0 + density_margin),
        )
        medium_distance = min(
            self.lookahead_m,
            max(high_distance + 1.0, comfortable_braking_distance + 18.0 + density_margin),
        )

        return {
            "ego_speed_mps": round(float(ego_speed), 3),
            "comfortable_braking_distance_m": round(float(comfortable_braking_distance), 3),
            "emergency_braking_distance_m": round(float(emergency_braking_distance), 3),
            "medium_distance_m": round(float(medium_distance), 3),
            "high_distance_m": round(float(high_distance), 3),
            "critical_distance_m": round(float(critical_distance), 3),
            "high_ttc_s": 3.5,
            "critical_ttc_s": 1.8,
        }

    def _risk_from_distance_ttc(
        self,
        distance_m: Optional[float],
        ttc_s: Optional[float],
        scene_state: Optional[dict[str, Any]] = None,
    ) -> str:
        if distance_m is None:
            return "LOW"
        if scene_state is None:
            if distance_m < 12.0 or (ttc_s is not None and ttc_s < 1.8):
                return "CRITICAL"
            if distance_m < 30.0 or (ttc_s is not None and ttc_s < 3.5):
                return "HIGH"
            if distance_m < self.lookahead_m:
                return "MEDIUM"
            return "LOW"

        thresholds = self._adaptive_thresholds(scene_state)
        if distance_m <= thresholds["critical_distance_m"] or (
            ttc_s is not None and ttc_s <= thresholds["critical_ttc_s"]
        ):
            return "CRITICAL"
        if distance_m <= thresholds["high_distance_m"] or (
            ttc_s is not None and ttc_s <= thresholds["high_ttc_s"]
        ):
            return "HIGH"
        if distance_m <= thresholds["medium_distance_m"]:
            return "MEDIUM"
        return "LOW"

    def _route_semantics(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> dict[str, Any]:
        route_points = scene_state["route_info"].get("points", [])
        yaw_values = [float(point["yaw"]) for point in route_points if "yaw" in point]
        road_options = [str(point.get("road_option", "UNKNOWN")) for point in route_points]

        max_yaw_change = 0.0
        for index in range(len(yaw_values) - 1):
            yaw_change = abs(normalize_angle_deg(yaw_values[index + 1] - yaw_values[index]))
            max_yaw_change = max(max_yaw_change, yaw_change)

        if len(path) < 2:
            route_shape = "UNAVAILABLE"
        elif max_yaw_change < 8.0:
            route_shape = "STRAIGHT"
        elif max_yaw_change < 25.0:
            route_shape = "CURVED"
        else:
            route_shape = "SHARP_CURVE_OR_TURN"

        upcoming_maneuver = "LANEFOLLOW"
        for option in road_options:
            if option not in ("LANEFOLLOW", "STRAIGHT", "VOID", "UNKNOWN"):
                upcoming_maneuver = option
                break

        return {
            "route_valid": len(path) >= 2,
            "route_source": scene_state["route_info"].get("source", "unknown"),
            "lookahead_m": scene_state["route_info"].get("lookahead_m"),
            "point_count": len(route_points),
            "max_yaw_change_deg": round(float(max_yaw_change), 3),
            "route_shape": route_shape,
            "upcoming_maneuver": upcoming_maneuver,
            "road_options": road_options[:10],
        }

    def _object_summary(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> dict[str, Any]:
        del path  # currently summarized from object ego-relative fields
        objects = scene_state["objects_ground_truth"]
        vehicle_count = sum(1 for obj in objects if obj["object_type"].startswith("vehicle."))
        walker_count = sum(1 for obj in objects if obj["object_type"].startswith("walker."))

        nearest_object_distance = None
        nearest_vehicle_ahead = None
        nearest_walker_ahead = None

        for obj in objects:
            dist = float(obj.get("distance_to_ego_m", 999.0))
            if nearest_object_distance is None or dist < nearest_object_distance:
                nearest_object_distance = dist

            forward = float(obj.get("forward_distance_m", -999.0))
            if forward <= 0.0:
                continue

            if obj["object_type"].startswith("vehicle."):
                if nearest_vehicle_ahead is None or forward < nearest_vehicle_ahead:
                    nearest_vehicle_ahead = forward
            elif obj["object_type"].startswith("walker."):
                if nearest_walker_ahead is None or forward < nearest_walker_ahead:
                    nearest_walker_ahead = forward

        return {
            "vehicle_count": vehicle_count,
            "walker_count": walker_count,
            "nearest_object_distance_m": round(nearest_object_distance, 3) if nearest_object_distance is not None else None,
            "nearest_vehicle_ahead_m": round(nearest_vehicle_ahead, 3) if nearest_vehicle_ahead is not None else None,
            "nearest_walker_ahead_m": round(nearest_walker_ahead, 3) if nearest_walker_ahead is not None else None,
        }

    def build_external_semantics(self, scene_state: dict[str, Any]) -> dict[str, Any]:
        path = self._build_path(scene_state)
        road_info = scene_state["road_info"]
        route_context = self._route_semantics(scene_state, path)

        if road_info.get("is_junction"):
            scene_type = "JUNCTION_APPROACH_OR_IN_JUNCTION"
        elif route_context["route_shape"] == "STRAIGHT":
            scene_type = "LANE_FOLLOWING"
        elif route_context["route_shape"] == "CURVED":
            scene_type = "CURVE_FOLLOWING"
        elif route_context["route_shape"] == "SHARP_CURVE_OR_TURN":
            scene_type = "TURN_OR_SHARP_CURVE"
        else:
            scene_type = "ROUTE_UNAVAILABLE"

        return {
            "sim_time_s": scene_state["sim_time_s"],
            "scene_type": scene_type,
            "road_context": {
                "map_name": road_info.get("map_name"),
                "road_id": road_info.get("road_id"),
                "section_id": road_info.get("section_id"),
                "lane_id": road_info.get("lane_id"),
                "lane_type": road_info.get("lane_type"),
                "lane_change": road_info.get("lane_change"),
                "lane_width": road_info.get("lane_width"),
                "speed_limit_kmh": road_info.get("speed_limit_kmh"),
                "is_junction": road_info.get("is_junction"),
                "ego_lateral_offset_m": road_info.get("ego_lateral_offset_m"),
                "left_lane_available": road_info.get("left_lane_available"),
                "right_lane_available": road_info.get("right_lane_available"),
                "left_lane_marking": road_info.get("left_lane_marking"),
                "right_lane_marking": road_info.get("right_lane_marking"),
            },
            "route_context": route_context,
            "risk_thresholds": self._adaptive_thresholds(scene_state),
            "traffic_rule_context": road_info.get("traffic_light", {}),
            "object_summary": self._object_summary(scene_state, path),
        }

    def _detect_route_risk(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        route_points = scene_state["route_info"].get("points", [])
        if len(path) < 2 or len(route_points) == 0:
            hazards.append(
                {
                    "hazard_type": "ROUTE_UNAVAILABLE",
                    "risk_level": "HIGH",
                    "distance_m": None,
                    "ttc_s": None,
                    "matched_object_id": None,
                    "matched_object": None,
                    "details": {"reason": "No valid forward route points are available."},
                }
            )
        return hazards

    def _detect_static_obstacle(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        if len(path) < 2:
            return hazards

        ego_state = scene_state["ego_state"]
        corridor_half_width = self._corridor_half_width(scene_state)
        corridor_polygon = self._corridor_polygon(scene_state, path)
        best: Optional[dict[str, Any]] = None

        for obj in scene_state["objects_ground_truth"]:
            if float(obj["speed"]) > self.static_speed_threshold_mps:
                continue
            if float(obj.get("forward_distance_m", -999.0)) <= 0.0:
                continue

            bbox_polygon = obj.get("bbox_polygon") or self._object_polygon(obj)
            if corridor_polygon is not None and not polygon_intersects(bbox_polygon, corridor_polygon):
                continue

            projection = self._best_polygon_projection(
                bbox_polygon,
                path,
                (float(obj["x"]), float(obj["y"])),
            )
            if projection is None:
                continue
            if projection.longitudinal_s <= 0.0 or projection.longitudinal_s > self.lookahead_m:
                continue

            ego_speed = max(float(ego_state["speed"]), 0.0)
            ttc_s = None
            if ego_speed > 0.1:
                ttc_s = projection.longitudinal_s / ego_speed

            risk_level = self._risk_from_distance_ttc(projection.longitudinal_s, ttc_s, scene_state)
            candidate = {
                "hazard_type": "STATIC_OBSTACLE_PATH_BLOCKED",
                "risk_level": risk_level,
                "distance_m": round(float(projection.longitudinal_s), 3),
                "ttc_s": round(float(ttc_s), 3) if ttc_s is not None else None,
                "matched_object_id": int(obj["id"]),
                "matched_object": obj,
                "details": {
                    "lateral_l_m": round(float(projection.lateral_l), 3),
                    "corridor_half_width_m": round(float(corridor_half_width), 3),
                    "geometry_test": "bbox_polygon_intersects_planned_path_corridor",
                },
            }

            if best is None or candidate["distance_m"] < best["distance_m"]:
                best = candidate

        if best is not None:
            hazards.append(best)
        return hazards

    def _detect_lead_vehicle_risk(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        if len(path) < 2:
            return hazards

        ego_state = scene_state["ego_state"]
        corridor_half_width = self._corridor_half_width(scene_state)
        corridor_polygon = self._corridor_polygon(scene_state, path)
        best: Optional[dict[str, Any]] = None

        for obj in scene_state["objects_ground_truth"]:
            if not obj["object_type"].startswith("vehicle."):
                continue
            if float(obj.get("forward_distance_m", -999.0)) <= 0.0:
                continue

            bbox_polygon = obj.get("bbox_polygon") or self._object_polygon(obj)
            if corridor_polygon is not None and not polygon_intersects(bbox_polygon, corridor_polygon):
                continue

            projection = self._best_polygon_projection(
                bbox_polygon,
                path,
                (float(obj["x"]), float(obj["y"])),
            )
            if projection is None:
                continue
            if projection.longitudinal_s <= 0.0 or projection.longitudinal_s > self.lookahead_m:
                continue

            kinematics = self._relative_path_kinematics(scene_state, obj, path, projection)
            relative_speed = float(kinematics["closing_speed_mps"])
            relative_accel = float(kinematics["relative_accel_mps2"])
            ttc_s = self._time_to_collision(projection.longitudinal_s, relative_speed, relative_accel)

            lead_decelerating = float(kinematics["object_along_accel_mps2"]) < -1.5
            close_enough = projection.longitudinal_s <= self._adaptive_thresholds(scene_state)["medium_distance_m"]
            if relative_speed <= 0.1 and not (lead_decelerating and close_enough):
                continue

            if ttc_s is not None and ttc_s > 6.0 and projection.longitudinal_s > 30.0 and not lead_decelerating:
                continue

            risk_level = self._risk_from_distance_ttc(projection.longitudinal_s, ttc_s, scene_state)
            hazard_type = "LEAD_VEHICLE_DECELERATING" if lead_decelerating else "LOW_TTC_TO_LEAD_VEHICLE"
            candidate = {
                "hazard_type": hazard_type,
                "risk_level": risk_level,
                "distance_m": round(float(projection.longitudinal_s), 3),
                "ttc_s": round(float(ttc_s), 3) if ttc_s is not None else None,
                "matched_object_id": int(obj["id"]),
                "matched_object": obj,
                "details": {
                    "lead_speed_mps": obj["speed"],
                    "ego_speed_mps": ego_state["speed"],
                    "relative_speed_mps": round(float(relative_speed), 3),
                    "relative_accel_mps2": round(float(relative_accel), 3),
                    "object_along_accel_mps2": kinematics["object_along_accel_mps2"],
                    "lateral_l_m": round(float(projection.lateral_l), 3),
                    "corridor_half_width_m": round(float(corridor_half_width), 3),
                    "geometry_test": "bbox_polygon_intersects_planned_path_corridor",
                },
            }

            if best is None or candidate["distance_m"] < best["distance_m"]:
                best = candidate

        if best is not None:
            hazards.append(best)
        return hazards

    def _detect_pedestrian_crossing(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        if len(path) < 2:
            return hazards

        corridor_half_width = self._corridor_half_width(scene_state)
        corridor_polygon = self._corridor_polygon(scene_state, path)
        best: Optional[dict[str, Any]] = None
        step_count = int(self.pedestrian_prediction_horizon_s / self.pedestrian_prediction_step_s)

        for obj in scene_state["objects_ground_truth"]:
            if not obj["object_type"].startswith("walker."):
                continue

            for step in range(1, step_count + 1):
                t = step * self.pedestrian_prediction_step_s
                px, py, _ = self._object_future_pose(obj, t)
                predicted_polygon = self._object_polygon(obj, t)
                if corridor_polygon is not None and not polygon_intersects(predicted_polygon, corridor_polygon):
                    continue

                projection = self._best_polygon_projection(predicted_polygon, path, (px, py))
                if projection is None:
                    continue
                if projection.longitudinal_s <= 0.0 or projection.longitudinal_s > self.lookahead_m:
                    continue

                risk_level = self._risk_from_distance_ttc(projection.longitudinal_s, t, scene_state)
                candidate = {
                    "hazard_type": "PEDESTRIAN_PREDICTED_TO_ENTER_PATH",
                    "risk_level": risk_level,
                    "distance_m": round(float(projection.longitudinal_s), 3),
                    "ttc_s": round(float(t), 3),
                    "matched_object_id": int(obj["id"]),
                    "matched_object": obj,
                    "details": {
                        "predicted_x": round(float(px), 3),
                        "predicted_y": round(float(py), 3),
                        "prediction_time_s": round(float(t), 3),
                        "lateral_l_m": round(float(projection.lateral_l), 3),
                        "corridor_half_width_m": round(float(corridor_half_width), 3),
                        "prediction_model": "constant_acceleration_bbox",
                    },
                }

                if best is None or candidate["ttc_s"] < best["ttc_s"]:
                    best = candidate
                break

        if best is not None:
            hazards.append(best)
        return hazards

    def _detect_cut_in_vehicle(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        if len(path) < 2:
            return hazards

        corridor_half_width = self._corridor_half_width(scene_state)
        corridor_polygon = self._corridor_polygon(scene_state, path)
        best: Optional[dict[str, Any]] = None

        for obj in scene_state["objects_ground_truth"]:
            if not obj["object_type"].startswith("vehicle."):
                continue

            current_polygon = obj.get("bbox_polygon") or self._object_polygon(obj)
            current_projection = self._best_polygon_projection(
                current_polygon,
                path,
                (float(obj["x"]), float(obj["y"])),
            )
            if current_projection is None:
                continue
            if current_projection.longitudinal_s <= 0.0 or current_projection.longitudinal_s > self.lookahead_m:
                continue

            # If it is already inside the ego path corridor, lead/static detectors handle it.
            if corridor_polygon is not None and polygon_intersects(current_polygon, corridor_polygon):
                continue

            predicted_projection = None
            predicted_x = float(obj["x"])
            predicted_y = float(obj["y"])
            predicted_t = self.cut_in_prediction_horizon_s
            for t in self._prediction_times(self.cut_in_prediction_horizon_s):
                predicted_x, predicted_y, _ = self._object_future_pose(obj, t)
                predicted_polygon = self._object_polygon(obj, t)
                if corridor_polygon is not None and not polygon_intersects(predicted_polygon, corridor_polygon):
                    continue
                predicted_projection = self._best_polygon_projection(predicted_polygon, path, (predicted_x, predicted_y))
                if predicted_projection is None:
                    continue
                if predicted_projection.longitudinal_s <= 0.0 or predicted_projection.longitudinal_s > self.lookahead_m:
                    continue
                predicted_t = t
                break

            if predicted_projection is None:
                continue

            if abs(predicted_projection.lateral_l) >= abs(current_projection.lateral_l) - 0.3:
                continue

            risk_level = self._risk_from_distance_ttc(predicted_projection.longitudinal_s, predicted_t, scene_state)
            candidate = {
                "hazard_type": "VEHICLE_PREDICTED_CUT_IN",
                "risk_level": risk_level,
                "distance_m": round(float(predicted_projection.longitudinal_s), 3),
                "ttc_s": round(float(predicted_t), 3),
                "matched_object_id": int(obj["id"]),
                "matched_object": obj,
                "details": {
                    "current_lateral_l_m": round(float(current_projection.lateral_l), 3),
                    "predicted_lateral_l_m": round(float(predicted_projection.lateral_l), 3),
                    "predicted_x": round(float(predicted_x), 3),
                    "predicted_y": round(float(predicted_y), 3),
                    "corridor_half_width_m": round(float(corridor_half_width), 3),
                    "prediction_model": "constant_acceleration_bbox",
                },
            }

            if best is None or candidate["distance_m"] < best["distance_m"]:
                best = candidate

        if best is not None:
            hazards.append(best)
        return hazards

    def _detect_predicted_path_conflict(self, scene_state: dict[str, Any], path: list[tuple[float, float]]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        if len(path) < 2:
            return hazards

        corridor_polygon = self._corridor_polygon(scene_state, path)
        corridor_half_width = self._corridor_half_width(scene_state)
        best: Optional[dict[str, Any]] = None

        for obj in scene_state["objects_ground_truth"]:
            if float(obj.get("speed", 0.0)) <= self.static_speed_threshold_mps:
                continue

            current_polygon = obj.get("bbox_polygon") or self._object_polygon(obj)
            if corridor_polygon is not None and polygon_intersects(current_polygon, corridor_polygon):
                continue

            for t in self._prediction_times(self.vehicle_prediction_horizon_s):
                predicted_x, predicted_y, _ = self._object_future_pose(obj, t)
                predicted_polygon = self._object_polygon(obj, t)
                if corridor_polygon is not None and not polygon_intersects(predicted_polygon, corridor_polygon):
                    continue

                projection = self._best_polygon_projection(predicted_polygon, path, (predicted_x, predicted_y))
                if projection is None:
                    continue
                if projection.longitudinal_s <= 0.0 or projection.longitudinal_s > self.lookahead_m:
                    continue

                risk_level = self._risk_from_distance_ttc(projection.longitudinal_s, t, scene_state)
                hazard_type = (
                    "CROSSING_WALKER_PREDICTED_PATH_CONFLICT"
                    if obj["object_type"].startswith("walker.")
                    else "DYNAMIC_OBJECT_PREDICTED_PATH_CONFLICT"
                )
                candidate = {
                    "hazard_type": hazard_type,
                    "risk_level": risk_level,
                    "distance_m": round(float(projection.longitudinal_s), 3),
                    "ttc_s": round(float(t), 3),
                    "matched_object_id": int(obj["id"]),
                    "matched_object": obj,
                    "details": {
                        "predicted_x": round(float(predicted_x), 3),
                        "predicted_y": round(float(predicted_y), 3),
                        "prediction_time_s": round(float(t), 3),
                        "lateral_l_m": round(float(projection.lateral_l), 3),
                        "corridor_half_width_m": round(float(corridor_half_width), 3),
                        "prediction_model": "constant_acceleration_bbox",
                    },
                }
                if best is None or candidate["ttc_s"] < best["ttc_s"]:
                    best = candidate
                break

        if best is not None:
            hazards.append(best)
        return hazards

    def _detect_traffic_rule_risk(self, scene_state: dict[str, Any]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        road_info = scene_state["road_info"]
        traffic_light = road_info.get("traffic_light", {})
        state = traffic_light.get("state")

        if state in ("Red", "Yellow"):
            ego_speed = float(scene_state["ego_state"]["speed"])
            risk_level = "HIGH" if ego_speed > 2.0 else "MEDIUM"
            hazards.append(
                {
                    "hazard_type": "TRAFFIC_LIGHT_REQUIRES_CAUTION",
                    "risk_level": risk_level,
                    "distance_m": None,
                    "ttc_s": None,
                    "matched_object_id": traffic_light.get("traffic_light_id"),
                    "matched_object": None,
                    "details": {
                        "traffic_light_state": state,
                        "ego_speed_mps": round(float(ego_speed), 3),
                    },
                }
            )

        return hazards

    def _detect_lane_departure_risk(self, scene_state: dict[str, Any]) -> list[dict[str, Any]]:
        hazards: list[dict[str, Any]] = []
        road_info = scene_state["road_info"]
        lateral_offset = road_info.get("ego_lateral_offset_m")
        lane_width = road_info.get("lane_width")

        if lateral_offset is None or lane_width is None:
            return hazards

        lateral_abs = abs(float(lateral_offset))
        lane_half_width = float(lane_width) / 2.0

        if lateral_abs > lane_half_width:
            hazards.append(
                {
                    "hazard_type": "LANE_DEPARTURE",
                    "risk_level": "HIGH",
                    "distance_m": None,
                    "ttc_s": None,
                    "matched_object_id": None,
                    "matched_object": None,
                    "details": {
                        "ego_lateral_offset_m": round(float(lateral_offset), 3),
                        "lane_half_width_m": round(float(lane_half_width), 3),
                    },
                }
            )
        elif lateral_abs > lane_half_width * 0.8:
            hazards.append(
                {
                    "hazard_type": "LANE_DEPARTURE_MARGIN_LOW",
                    "risk_level": "MEDIUM",
                    "distance_m": None,
                    "ttc_s": None,
                    "matched_object_id": None,
                    "matched_object": None,
                    "details": {
                        "ego_lateral_offset_m": round(float(lateral_offset), 3),
                        "lane_half_width_m": round(float(lane_half_width), 3),
                    },
                }
            )

        return hazards

    def assess_external_risk(self, scene_state: dict[str, Any]) -> dict[str, Any]:
        path = self._build_path(scene_state)

        hazards: list[dict[str, Any]] = []
        hazards.extend(self._detect_route_risk(scene_state, path))
        hazards.extend(self._detect_static_obstacle(scene_state, path))
        hazards.extend(self._detect_lead_vehicle_risk(scene_state, path))
        hazards.extend(self._detect_pedestrian_crossing(scene_state, path))
        hazards.extend(self._detect_cut_in_vehicle(scene_state, path))
        hazards.extend(self._detect_predicted_path_conflict(scene_state, path))
        hazards.extend(self._detect_traffic_rule_risk(scene_state))
        hazards.extend(self._detect_lane_departure_risk(scene_state))

        if not hazards:
            return build_clear_external_risk(float(scene_state["sim_time_s"]))

        primary = max(
            hazards,
            key=lambda hazard: (
                RISK_RANK.get(hazard["risk_level"], 0),
                -999.0 if hazard["distance_m"] is None else -float(hazard["distance_m"]),
            ),
        )
        external_risk_level = max_risk_level([hazard["risk_level"] for hazard in hazards])

        return {
            "sim_time_s": scene_state["sim_time_s"],
            "has_external_hazard": True,
            "primary_hazard_type": primary["hazard_type"],
            "external_risk_level": external_risk_level,
            "scene_recommendation": scene_recommendation_from_risk(external_risk_level),
            "risk_thresholds": self._adaptive_thresholds(scene_state),
            "hazard_distance_m": primary.get("distance_m"),
            "hazard_ttc_s": primary.get("ttc_s"),
            "matched_object": primary.get("matched_object"),
            "hazards": hazards,
        }

    def understand(self, scene_state: dict[str, Any]) -> dict[str, Any]:
        external_semantics = self.build_external_semantics(scene_state)
        external_risk = self.assess_external_risk(scene_state)
        return {
            "sim_time_s": scene_state["sim_time_s"],
            "semantic_model": "basic_agent_planned_route_dynamic_risk_v2",
            "external_semantics": external_semantics,
            "external_risk": external_risk,
        }


class AutoDriveManager:
    def __init__(
        self,
        world: carla.World,
        ego_vehicle: carla.Vehicle,
        carla_map: carla.Map,
        agent: BasicAgent,
        recorder: EventRecorder,
        args: argparse.Namespace,
    ) -> None:
        self.world = world
        self.ego_vehicle = ego_vehicle
        self.map = carla_map
        self.agent = agent
        self.recorder = recorder
        self.args = args
        self.lane_change_cooldown_until_s = 0.0
        self.lane_change_active_until_s = 0.0
        self.last_emergency_brake_until_s = 0.0

    def bind_agent(self, agent: BasicAgent) -> None:
        self.agent = agent

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

    def _find_lead_vehicle(self, scene_state: dict[str, Any]) -> Optional[AutoDriveDecision]:
        ego_state = scene_state["ego_state"]
        road_info = scene_state["road_info"]
        route_points = scene_state["route_info"]["points"]
        path = [(ego_state["x"], ego_state["y"])]
        path.extend((point["x"], point["y"]) for point in route_points)
        if len(path) < 2:
            return None

        ego_heading = heading_unit_vector(ego_state["yaw"])
        corridor_half_width = max(road_info["lane_width"] / 2.0 + 0.75, 1.75)
        corridor_polygon = build_path_corridor_polygon(path, corridor_half_width)
        best_decision: Optional[AutoDriveDecision] = None

        for obj in scene_state["objects_ground_truth"]:
            if not obj["object_type"].startswith("vehicle."):
                continue

            rel_x = obj["x"] - ego_state["x"]
            rel_y = obj["y"] - ego_state["y"]
            forward_distance = rel_x * ego_heading[0] + rel_y * ego_heading[1]
            if forward_distance <= 0.0 or forward_distance > self.args.auto_follow_lookahead_m:
                continue

            bbox_polygon = obj.get("bbox_polygon")
            if bbox_polygon is None:
                bbox = obj.get("bbox", {})
                bbox_polygon = bbox_polygon_from_pose(
                    float(obj["x"]),
                    float(obj["y"]),
                    float(obj.get("yaw", 0.0)),
                    float(bbox.get("extent_x", 0.5)),
                    float(bbox.get("extent_y", 0.5)),
                )

            if corridor_polygon is not None and not polygon_intersects(bbox_polygon, corridor_polygon):
                continue

            projection_candidates = []
            center_projection = self._project_point_to_path((obj["x"], obj["y"]), path)
            if center_projection is not None:
                projection_candidates.append(center_projection)
            for point in bbox_polygon:
                corner_projection = self._project_point_to_path((float(point[0]), float(point[1])), path)
                if corner_projection is not None:
                    projection_candidates.append(corner_projection)
            if not projection_candidates:
                continue

            projection = min(
                [candidate for candidate in projection_candidates if candidate.longitudinal_s >= 0.0]
                or projection_candidates,
                key=lambda candidate: candidate.longitudinal_s,
            )
            if projection is None:
                continue
            if projection.longitudinal_s <= 0.0 or projection.longitudinal_s > self.args.auto_follow_lookahead_m:
                continue

            relative_speed_mps = max(ego_state["speed"] - obj["speed"], 0.0)
            ttc_s = None
            if relative_speed_mps > 0.1:
                ttc_s = projection.longitudinal_s / relative_speed_mps

            decision = AutoDriveDecision(
                action="KEEP_LANE",
                lead_vehicle_id=int(obj["id"]),
                lead_distance_m=round(float(projection.longitudinal_s), 3),
                lead_speed_mps=round(float(obj["speed"]), 3),
                relative_speed_mps=round(float(relative_speed_mps), 3),
                ttc_s=round(float(ttc_s), 3) if ttc_s is not None else None,
            )

            if best_decision is None or decision.lead_distance_m < best_decision.lead_distance_m:
                best_decision = decision

        return best_decision

    def _lane_is_safe(self, target_waypoint: carla.Waypoint) -> bool:
        ego_location = self.ego_vehicle.get_location()
        ego_transform = self.ego_vehicle.get_transform()
        ego_forward = ego_transform.get_forward_vector()
        for actor in self.world.get_actors().filter("vehicle.*"):
            if actor.id == self.ego_vehicle.id:
                continue
            actor_location = actor.get_location()
            if actor_location.distance(ego_location) > self.args.lane_change_check_radius_m:
                continue
            actor_waypoint = self.map.get_waypoint(
                actor_location,
                project_to_road=True,
                lane_type=carla.LaneType.Driving,
            )
            if actor_waypoint.road_id != target_waypoint.road_id or actor_waypoint.lane_id != target_waypoint.lane_id:
                continue

            rel = actor_location - ego_location
            longitudinal = rel.x * ego_forward.x + rel.y * ego_forward.y + rel.z * ego_forward.z
            if longitudinal >= 0.0 and longitudinal < self.args.lane_change_clear_ahead_m:
                return False
            if longitudinal < 0.0 and abs(longitudinal) < self.args.lane_change_clear_behind_m:
                return False
        return True

    def _candidate_lane_change(self, sim_time_s: float) -> tuple[Optional[str], Optional[int]]:
        if sim_time_s < self.lane_change_cooldown_until_s or sim_time_s < self.lane_change_active_until_s:
            return None, None

        ego_waypoint = self.map.get_waypoint(
            self.ego_vehicle.get_location(),
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        if ego_waypoint.is_junction:
            return None, None

        lane_change_policy = str(ego_waypoint.lane_change)
        candidates: list[tuple[str, Optional[carla.Waypoint]]] = []
        if lane_change_policy in ("Right", "Both"):
            candidates.append(("right", ego_waypoint.get_right_lane()))
        if lane_change_policy in ("Left", "Both"):
            candidates.append(("left", ego_waypoint.get_left_lane()))

        for direction, waypoint in candidates:
            if waypoint is None or waypoint.lane_type != carla.LaneType.Driving:
                continue
            if waypoint.is_junction:
                continue
            if not self._lane_is_safe(waypoint):
                continue
            return direction, int(waypoint.lane_id)

        return None, None

    def decide(self, sim_time_s: float, scene_state: dict[str, Any], hazard: dict[str, Any]) -> AutoDriveDecision:
        lead_vehicle = self._find_lead_vehicle(scene_state)
        if lead_vehicle is None:
            return AutoDriveDecision(action="KEEP_LANE")

        distance_m = lead_vehicle.lead_distance_m or 0.0
        relative_speed_mps = lead_vehicle.relative_speed_mps or 0.0
        ttc_s = lead_vehicle.ttc_s

        needs_lane_change = (
            distance_m <= self.args.lane_change_trigger_distance_m
            and (
                relative_speed_mps >= self.args.lane_change_min_relative_speed_mps
                or (lead_vehicle.lead_speed_mps or 0.0) <= self.args.lane_change_lead_speed_threshold_mps
            )
        )
        if needs_lane_change:
            direction, target_lane_id = self._candidate_lane_change(sim_time_s)
            if direction is not None:
                self.agent.lane_change(
                    direction,
                    same_lane_time=0.2,
                    other_lane_time=3.0,
                    lane_change_time=2.0,
                )
                self.lane_change_cooldown_until_s = sim_time_s + self.args.lane_change_cooldown_s
                self.lane_change_active_until_s = sim_time_s + self.args.lane_change_active_window_s
                self.recorder.record_event(
                    "auto_lane_change_requested",
                    sim_time_s,
                    {
                        "direction": direction,
                        "target_lane_id": target_lane_id,
                        "lead_vehicle_id": lead_vehicle.lead_vehicle_id,
                        "lead_distance_m": distance_m,
                        "lead_speed_mps": lead_vehicle.lead_speed_mps,
                        "relative_speed_mps": relative_speed_mps,
                        "ttc_s": ttc_s,
                    },
                )
                lead_vehicle.action = f"LANE_CHANGE_{direction.upper()}"
                lead_vehicle.target_lane_id = target_lane_id
                return lead_vehicle

        emergency_stop_needed = False
        if distance_m <= self.args.emergency_brake_distance_m:
            emergency_stop_needed = True
        if ttc_s is not None and ttc_s <= self.args.emergency_brake_ttc_s:
            emergency_stop_needed = True
        if hazard.get("risk_level") == "CRITICAL":
            emergency_stop_needed = True
        if hazard["takeover_required"] and (hazard["hazard_distance_m"] or 999.0) <= self.args.emergency_brake_distance_m:
            emergency_stop_needed = True

        if emergency_stop_needed:
            if sim_time_s >= self.last_emergency_brake_until_s:
                self.recorder.record_event(
                    "auto_emergency_brake",
                    sim_time_s,
                    {
                        "lead_vehicle_id": lead_vehicle.lead_vehicle_id,
                        "lead_distance_m": distance_m,
                        "relative_speed_mps": relative_speed_mps,
                        "ttc_s": ttc_s,
                        "hazard_distance_m": hazard["hazard_distance_m"],
                    },
                )
                self.last_emergency_brake_until_s = sim_time_s + 0.5
            lead_vehicle.action = "EMERGENCY_BRAKE"
            lead_vehicle.override_control = TakeoverManager.build_mrm_control(scene_state["ego_state"]["speed"])
            return lead_vehicle

        return lead_vehicle


class TakeoverManager:
    def __init__(self, recorder: EventRecorder, tor_timeout_s: float = 2.0, escalation_timeout_s: float = 4.0) -> None:
        self.recorder = recorder
        self.tor_timeout_s = tor_timeout_s
        self.escalation_timeout_s = escalation_timeout_s
        self.state = "AUTO"
        self.reason_code = "CLEAR"
        self.active_hazard_since_s: Optional[float] = None
        self.driver_response_logged = False
        self.driver_ready_at_risk_start = False

    def reset(self) -> None:
        self.state = "AUTO"
        self.reason_code = "CLEAR"
        self.active_hazard_since_s = None
        self.driver_response_logged = False
        self.driver_ready_at_risk_start = False

    def update(
        self,
        sim_time_s: float,
        drive_mode: str,
        driver_state: DriverState,
        hazard: dict[str, Any],
        reset_mrm: bool = False,
    ) -> dict[str, Any]:
        takeover_required = bool(hazard["takeover_required"]) and drive_mode == "AUTO"

        if reset_mrm and self.state == "MRM":
            self.recorder.record_event("mrm_reset", sim_time_s, {"reason": "user_reset"})
            self.reset()

        if drive_mode != "AUTO" and self.state == "AUTO":
            return {
                "state": self.state,
                "reason_code": "MANUAL_MODE",
                "takeover_ready": False,
            }

        if not hazard["takeover_required"]:
            if self.state in ("TOR_ACTIVE", "ESCALATED_TOR"):
                self.recorder.record_event("hazard_cleared", sim_time_s, {"reason": self.reason_code})
            if self.state != "MRM":
                self.reset()
            return {
                "state": self.state,
                "reason_code": self.reason_code,
                "takeover_ready": False,
            }

        self.reason_code = str(hazard["reason_code"])

        if self.active_hazard_since_s is None:
            self.active_hazard_since_s = sim_time_s
            self.driver_ready_at_risk_start = driver_state.response_ready()
            self.recorder.record_event("risk_appeared", sim_time_s, {"hazard": hazard})

        if self.state == "AUTO" and takeover_required:
            self.state = "TOR_ACTIVE"
            self.recorder.record_event("tor_active", sim_time_s, {"hazard": hazard})

        if self.state in ("TOR_ACTIVE", "ESCALATED_TOR") and not self.driver_response_logged:
            if drive_mode == "MANUAL" or (
                not self.driver_ready_at_risk_start and driver_state.response_ready()
            ):
                self.driver_response_logged = True
                self.recorder.record_event(
                    "driver_first_response",
                    sim_time_s,
                    {
                        "driver_state": asdict(driver_state),
                        "drive_mode": drive_mode,
                    },
                )

        if self.state in ("TOR_ACTIVE", "ESCALATED_TOR", "MRM"):
            if drive_mode == "MANUAL" and driver_state.response_ready():
                self.recorder.record_event(
                    "takeover_success",
                    sim_time_s,
                    {
                        "tor_state": self.state,
                        "driver_state": asdict(driver_state),
                    },
                )
                self.reset()
                return {
                    "state": self.state,
                    "reason_code": "TAKEOVER_SUCCESS",
                    "takeover_ready": True,
                }

        if self.active_hazard_since_s is None:
            return {
                "state": self.state,
                "reason_code": self.reason_code,
                "takeover_ready": False,
            }

        hazard_age_s = sim_time_s - self.active_hazard_since_s
        if self.state == "TOR_ACTIVE" and hazard_age_s >= self.tor_timeout_s:
            self.state = "ESCALATED_TOR"
            self.recorder.record_event("escalated_tor", sim_time_s, {"hazard_age_s": round(hazard_age_s, 3)})
        elif self.state == "ESCALATED_TOR" and hazard_age_s >= self.escalation_timeout_s:
            self.state = "MRM"
            self.recorder.record_event("mrm_triggered", sim_time_s, {"hazard_age_s": round(hazard_age_s, 3)})

        return {
            "state": self.state,
            "reason_code": self.reason_code,
            "takeover_ready": self.state in ("TOR_ACTIVE", "ESCALATED_TOR"),
        }

    @staticmethod
    def build_mrm_control(current_speed_mps: float) -> carla.VehicleControl:
        control = carla.VehicleControl()
        control.throttle = 0.0
        control.brake = 0.85
        control.steer = 0.0
        control.hand_brake = current_speed_mps < 0.2
        return control


class ScenarioManager:
    def __init__(
        self,
        client: carla.Client,
        world: carla.World,
        ego_vehicle: carla.Vehicle,
        recorder: EventRecorder,
        args: argparse.Namespace,
    ) -> None:
        self.client = client
        self.world = world
        self.map = world.get_map()
        self.ego_vehicle = ego_vehicle
        self.recorder = recorder
        self.args = args
        self.obstacle_actor: Optional[carla.Vehicle] = None
        self.traffic_manager = self.client.get_trafficmanager(self.args.tm_port)
        self.traffic_actor_ids: list[int] = []
        self._owned_actor_ids: set[int] = set()

    def _preferred_vehicle_blueprints(self) -> list[carla.ActorBlueprint]:
        library = self.world.get_blueprint_library()
        preferred_ids = [
            "vehicle.audi.tt",
            "vehicle.tesla.model3",
            "vehicle.mercedes.coupe",
        ]
        blueprints = []
        for blueprint_id in preferred_ids:
            blueprint = library.find(blueprint_id)
            if blueprint is not None:
                blueprints.append(blueprint)
        if not blueprints:
            blueprints = list(library.filter("vehicle.*"))
        return blueprints

    def destroy_obstacle(self) -> None:
        if self.obstacle_actor is not None and self.obstacle_actor.is_alive:
            self.obstacle_actor.destroy()
        self.obstacle_actor = None

    def destroy_traffic(self) -> None:
        if not self.traffic_actor_ids:
            return
        actors = self.world.get_actors(self.traffic_actor_ids)
        for actor in actors:
            if actor is not None and actor.is_alive:
                actor.destroy()
        self.traffic_actor_ids.clear()

    def destroy_owned_actors(self) -> None:
        self.destroy_obstacle()
        self.destroy_traffic()
        self._owned_actor_ids.clear()

    def spawn_traffic_flow(self, sim_time_s: float) -> int:
        self.destroy_traffic()

        spawn_points = list(self.map.get_spawn_points())
        if not spawn_points:
            self.recorder.record_event("traffic_spawn_failed", sim_time_s, {"reason": "no_spawn_points"})
            return 0

        ego_location = self.ego_vehicle.get_location()
        eligible_spawn_points = [
            transform
            for transform in spawn_points
            if transform.location.distance(ego_location) >= self.args.traffic_spawn_exclusion_radius_m
        ]
        if not eligible_spawn_points:
            eligible_spawn_points = spawn_points

        random.shuffle(eligible_spawn_points)
        blueprints = list(self.world.get_blueprint_library().filter("vehicle.*"))
        blueprints = [bp for bp in blueprints if not bp.id.endswith("isetta") and not bp.id.endswith("carlacola")]
        if not blueprints:
            self.recorder.record_event("traffic_spawn_failed", sim_time_s, {"reason": "no_vehicle_blueprints"})
            return 0

        target_count = min(self.args.traffic_vehicle_count, len(eligible_spawn_points))
        spawned_ids: list[int] = []
        for spawn_point in eligible_spawn_points:
            if len(spawned_ids) >= target_count:
                break
            blueprint = random.choice(blueprints)
            if blueprint.has_attribute("role_name"):
                blueprint.set_attribute("role_name", "traffic")
            if blueprint.has_attribute("color"):
                color_attr = blueprint.get_attribute("color")
                if color_attr.recommended_values:
                    blueprint.set_attribute("color", random.choice(color_attr.recommended_values))
            if blueprint.has_attribute("driver_id"):
                driver_attr = blueprint.get_attribute("driver_id")
                if driver_attr.recommended_values:
                    blueprint.set_attribute("driver_id", random.choice(driver_attr.recommended_values))

            vehicle = self.world.try_spawn_actor(blueprint, spawn_point)
            if vehicle is None:
                continue

            vehicle.set_autopilot(True, self.args.tm_port)
            self.traffic_manager.auto_lane_change(vehicle, True)
            self.traffic_manager.distance_to_leading_vehicle(vehicle, self.args.traffic_follow_distance_m)
            self.traffic_manager.vehicle_percentage_speed_difference(vehicle, self.args.traffic_speed_delta_percent)
            self.traffic_manager.random_left_lanechange_percentage(vehicle, self.args.traffic_lane_change_percent)
            self.traffic_manager.random_right_lanechange_percentage(vehicle, self.args.traffic_lane_change_percent)
            spawned_ids.append(int(vehicle.id))

        self.traffic_actor_ids = spawned_ids
        self._owned_actor_ids.update(spawned_ids)
        self.recorder.record_event(
            "traffic_spawned",
            sim_time_s,
            {
                "vehicle_count": len(spawned_ids),
                "tm_port": self.args.tm_port,
                "speed_delta_percent": self.args.traffic_speed_delta_percent,
            },
        )
        return len(spawned_ids)

    def spawn_static_obstacle_ahead(self, distance_m: float, sim_time_s: float) -> Optional[carla.Vehicle]:
        self.destroy_obstacle()

        ego_waypoint = self.map.get_waypoint(
            self.ego_vehicle.get_location(),
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        waypoint = ego_waypoint
        remaining = distance_m
        while remaining > 0.01:
            step = min(remaining, ROUTE_STEP_M)
            next_candidates = waypoint.next(step)
            if not next_candidates:
                break
            waypoint = next_candidates[0]
            remaining -= step

        spawn_transform = waypoint.transform
        spawn_transform.location.z += 0.3

        for blueprint in self._preferred_vehicle_blueprints():
            if blueprint.has_attribute("role_name"):
                blueprint.set_attribute("role_name", "static_obstacle")
            try_actor = self.world.try_spawn_actor(blueprint, spawn_transform)
            if try_actor is None:
                continue
            obstacle = try_actor
            obstacle.set_autopilot(False)
            obstacle.set_simulate_physics(False)
            self.obstacle_actor = obstacle
            self._owned_actor_ids.add(int(obstacle.id))
            self.recorder.record_event(
                "static_obstacle_spawned",
                sim_time_s,
                {
                    "actor_id": int(obstacle.id),
                    "x": round(float(spawn_transform.location.x), 3),
                    "y": round(float(spawn_transform.location.y), 3),
                    "distance_m": round(distance_m, 3),
                },
            )
            return obstacle

        self.recorder.record_event("static_obstacle_spawn_failed", sim_time_s, {"distance_m": distance_m})
        return None


class CarlaLoopApp:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.client = carla.Client(args.host, args.port)
        self.client.set_timeout(args.client_timeout_s)
        self.world = self.client.get_world()
        self.map = self.world.get_map()
        self.display: Optional[pygame.Surface] = None
        self.font: Optional[pygame.font.Font] = None
        self.ego_vehicle: Optional[carla.Vehicle] = None
        self.owns_ego = False
        self.camera: Optional[ChaseCamera] = None
        self.collision_sensor: Optional[CollisionSensor] = None
        self.agent: Optional[BasicAgent] = None
        self.drive_mode = args.initial_mode.upper()
        self.driver_state = DriverState()
        self.keyboard = KeyboardController()
        self.recorder = EventRecorder(Path(args.log_dir))
        self.gt_provider: Optional[GroundTruthProvider] = None
        self.external_scene_understanding = ExternalSceneUnderstanding(
            lookahead_m=args.hazard_lookahead_m,
            vehicle_prediction_horizon_s=args.semantic_prediction_horizon_s,
            pedestrian_prediction_horizon_s=args.semantic_prediction_horizon_s,
            cut_in_prediction_horizon_s=args.semantic_cut_in_horizon_s,
            prediction_step_s=args.semantic_prediction_step_s,
            pedestrian_prediction_step_s=args.semantic_prediction_step_s,
            comfortable_decel_mps2=args.semantic_comfortable_decel_mps2,
            emergency_decel_mps2=args.semantic_emergency_decel_mps2,
            reaction_time_s=args.semantic_reaction_time_s,
            safety_margin_m=args.semantic_safety_margin_m,
        )
        self.takeover_manager = TakeoverManager(self.recorder)
        self.auto_drive_manager: Optional[AutoDriveManager] = None
        self.scenario_manager: Optional[ScenarioManager] = None
        self.current_destination: Optional[carla.Location] = None
        self.last_print_time_s = -1.0
        self.last_sim_time_s = 0.0
        self.run_start_sim_time_s = 0.0
        self.last_hazard_onset_s: Optional[float] = None
        self.scripted_driver_ready_applied = False
        self.scripted_manual_takeover_applied = False
        self.max_duration_reached = False
        self.last_auto_drive_decision = AutoDriveDecision()
        self.ros_publisher: Optional[BasicAgentRosPublisher] = None
        if args.publish_ros:
            self.ros_publisher = BasicAgentRosPublisher(args.role_name, args.ros_topic_namespace)

    def _build_agent(self) -> BasicAgent:
        assert self.ego_vehicle is not None
        agent = BasicAgent(self.ego_vehicle, target_speed=self.args.target_speed_kmh)
        agent.follow_speed_limits(True)
        if self.auto_drive_manager is not None:
            self.auto_drive_manager.bind_agent(agent)
        return agent

    @staticmethod
    def _preferred_forward_waypoint(
        waypoint: carla.Waypoint,
        step_m: float = ROUTE_STEP_M,
    ) -> Optional[carla.Waypoint]:
        candidates = waypoint.next(step_m)
        if not candidates:
            return None
        current_yaw = waypoint.transform.rotation.yaw
        return min(
            candidates,
            key=lambda candidate: abs(normalize_angle_deg(candidate.transform.rotation.yaw - current_yaw)),
        )

    def _find_existing_ego(self) -> Optional[carla.Vehicle]:
        for actor in self.world.get_actors().filter("vehicle.*"):
            if actor.attributes.get("role_name", "") == self.args.role_name:
                return actor
        return None

    def _choose_vehicle_blueprint(self) -> carla.ActorBlueprint:
        library = self.world.get_blueprint_library()
        preferred_ids = [
            self.args.vehicle_blueprint,
            "vehicle.tesla.model3",
            "vehicle.audi.tt",
        ]
        for blueprint_id in preferred_ids:
            try:
                blueprint = library.find(blueprint_id)
                if blueprint is not None:
                    return blueprint
            except RuntimeError:
                continue
        blueprints = list(library.filter("vehicle.*"))
        if not blueprints:
            raise RuntimeError("No vehicle blueprint available in the current CARLA world.")
        return random.choice(blueprints)

    def _forward_lane_distance_from_waypoint(
        self,
        start_waypoint: carla.Waypoint,
        target_distance_m: float = 220.0,
    ) -> tuple[float, Optional[carla.Waypoint]]:
        traversed_m = 0.0
        previous_waypoint = start_waypoint
        current_waypoint = start_waypoint

        while traversed_m < target_distance_m:
            next_waypoint = self._preferred_forward_waypoint(current_waypoint)
            if next_waypoint is None:
                break
            traversed_m += float(previous_waypoint.transform.location.distance(next_waypoint.transform.location))
            previous_waypoint = next_waypoint
            current_waypoint = next_waypoint

        return traversed_m, (current_waypoint if traversed_m > 0.0 else None)

    def _score_spawn_point(self, spawn_transform: carla.Transform) -> tuple[float, float, float]:
        waypoint = self.map.get_waypoint(
            spawn_transform.location,
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        forward_distance_m, _ = self._forward_lane_distance_from_waypoint(
            waypoint,
            target_distance_m=max(self.args.min_destination_distance_m, 220.0),
        )
        junction_penalty = 1000.0 if waypoint.is_junction else 0.0
        abs_curvature_bias = abs(normalize_angle_deg(spawn_transform.rotation.yaw))
        return (forward_distance_m - junction_penalty, forward_distance_m, -abs_curvature_bias)

    def _spawn_ego(self) -> carla.Vehicle:
        existing = self._find_existing_ego()
        if existing is not None:
            return existing

        blueprint = self._choose_vehicle_blueprint()
        if blueprint.has_attribute("role_name"):
            blueprint.set_attribute("role_name", self.args.role_name)
        if blueprint.has_attribute("color"):
            color_attr = blueprint.get_attribute("color")
            if color_attr.recommended_values:
                blueprint.set_attribute("color", color_attr.recommended_values[0])

        spawn_points = self.map.get_spawn_points()
        if not spawn_points:
            raise RuntimeError("CARLA map does not provide spawn points.")

        ranked_spawn_points = sorted(
            spawn_points,
            key=self._score_spawn_point,
            reverse=True,
        )

        for spawn_point in ranked_spawn_points:
            vehicle = self.world.try_spawn_actor(blueprint, spawn_point)
            if vehicle is not None:
                self.owns_ego = True
                return vehicle

        raise RuntimeError("Unable to spawn ego vehicle from available CARLA spawn points.")

    def _estimate_route_length_m(self, destination: carla.Location) -> float:
        assert self.ego_vehicle is not None
        if self.agent is None:
            return float(self.ego_vehicle.get_location().distance(destination))

        start_waypoint = self.map.get_waypoint(
            self.ego_vehicle.get_location(),
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        end_waypoint = self.map.get_waypoint(
            destination,
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        route_trace = self.agent.trace_route(start_waypoint, end_waypoint)
        if not route_trace:
            return 0.0

        previous_location = start_waypoint.transform.location
        route_length_m = 0.0
        for waypoint, _ in route_trace:
            current_location = waypoint.transform.location
            route_length_m += float(previous_location.distance(current_location))
            previous_location = current_location
        route_length_m += float(previous_location.distance(end_waypoint.transform.location))
        return route_length_m

    def _pick_forward_lane_destination(self, target_distance_m: float) -> tuple[Optional[carla.Location], float]:
        assert self.ego_vehicle is not None
        start_waypoint = self.map.get_waypoint(
            self.ego_vehicle.get_location(),
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        route_length_m, destination_waypoint = self._forward_lane_distance_from_waypoint(
            start_waypoint,
            target_distance_m=target_distance_m,
        )
        if destination_waypoint is None or route_length_m < max(40.0, target_distance_m * 0.6):
            return None, route_length_m
        return destination_waypoint.transform.location, route_length_m

    def _pick_destination(self, min_distance_m: float = 120.0) -> tuple[carla.Location, float]:
        assert self.ego_vehicle is not None
        forward_destination, forward_route_length_m = self._pick_forward_lane_destination(min_distance_m)
        if forward_destination is not None:
            return forward_destination, forward_route_length_m

        current_location = self.ego_vehicle.get_location()
        spawn_points = self.map.get_spawn_points()
        candidates = [
            transform.location
            for transform in spawn_points
            if transform.location.distance(current_location) >= min_distance_m
        ]
        if not candidates:
            candidates = [transform.location for transform in spawn_points]
        if not candidates:
            raise RuntimeError("CARLA map does not provide reachable destination candidates.")

        if len(candidates) > self.args.destination_samples:
            candidates = random.sample(candidates, self.args.destination_samples)

        best_destination = candidates[0]
        best_route_length_m = -1.0
        best_straight_distance_m = -1.0
        for candidate in candidates:
            route_length_m = self._estimate_route_length_m(candidate)
            straight_distance_m = float(current_location.distance(candidate))
            if route_length_m > best_route_length_m or (
                math.isclose(route_length_m, best_route_length_m) and straight_distance_m > best_straight_distance_m
            ):
                best_destination = candidate
                best_route_length_m = route_length_m
                best_straight_distance_m = straight_distance_m

        return best_destination, max(best_route_length_m, best_straight_distance_m)

    def _set_new_destination(self, sim_time_s: float, reason: str) -> None:
        assert self.ego_vehicle is not None
        if self.agent is None:
            return
        destination, route_length_m = self._pick_destination(min_distance_m=self.args.min_destination_distance_m)
        current_location = self.ego_vehicle.get_location()
        self.agent.set_destination(destination, start_location=current_location)
        self.current_destination = destination
        self.recorder.record_event(
            "destination_updated",
            sim_time_s,
            {
                "reason": reason,
                "destination": {
                    "x": round(float(destination.x), 3),
                    "y": round(float(destination.y), 3),
                    "z": round(float(destination.z), 3),
                },
                "route_length_m": round(float(route_length_m), 3),
            },
        )

    def _route_points_from_plan_entries(
        self,
        plan_entries: Any,
        source: str,
        lookahead_m: float,
    ) -> tuple[list[dict[str, Any]], str]:
        assert self.ego_vehicle is not None
        ego_location = self.ego_vehicle.get_location()
        ego_yaw = float(self.ego_vehicle.get_transform().rotation.yaw)
        ego_heading = heading_unit_vector(ego_yaw)

        route_points: list[dict[str, Any]] = []
        previous_location = ego_location
        route_s = 0.0

        for waypoint, road_option in list(plan_entries):
            location = waypoint.transform.location
            rel_x = float(location.x - ego_location.x)
            rel_y = float(location.y - ego_location.y)
            if not route_points and rel_x * ego_heading[0] + rel_y * ego_heading[1] < -2.0:
                continue

            step_distance = float(previous_location.distance(location))
            if step_distance < 0.25:
                continue
            route_s += step_distance
            previous_location = location

            point = waypoint_to_route_point(waypoint, road_option)
            point["route_s_m"] = round(float(route_s), 3)
            route_points.append(point)
            if route_s >= lookahead_m or len(route_points) >= ROUTE_POINT_COUNT:
                break

        return route_points, source

    def _planned_route_points(self) -> tuple[Optional[list[dict[str, Any]]], str]:
        assert self.ego_vehicle is not None
        if self.agent is None:
            return None, "map_forward_sampling"

        lookahead_m = ROUTE_POINT_COUNT * ROUTE_STEP_M
        try:
            local_planner = self.agent.get_local_planner()
            plan_entries = local_planner.get_plan()
            route_points, source = self._route_points_from_plan_entries(
                plan_entries,
                source="basic_agent_local_planner_queue",
                lookahead_m=lookahead_m,
            )
            if route_points:
                return route_points, source
        except Exception as exc:
            self.recorder.record_event(
                "planned_route_queue_read_failed",
                self.last_sim_time_s,
                {"error": str(exc)},
            )

        if self.current_destination is None:
            return None, "map_forward_sampling"

        try:
            start_waypoint = self.map.get_waypoint(
                self.ego_vehicle.get_location(),
                project_to_road=True,
                lane_type=carla.LaneType.Driving,
            )
            end_waypoint = self.map.get_waypoint(
                self.current_destination,
                project_to_road=True,
                lane_type=carla.LaneType.Driving,
            )
            route_trace = self.agent.trace_route(start_waypoint, end_waypoint)
            route_points, source = self._route_points_from_plan_entries(
                route_trace,
                source="basic_agent_global_route_trace",
                lookahead_m=lookahead_m,
            )
            if route_points:
                return route_points, source
        except Exception as exc:
            self.recorder.record_event(
                "planned_route_trace_failed",
                self.last_sim_time_s,
                {"error": str(exc)},
            )

        return None, "map_forward_sampling"

    def _follow_spectator(self) -> None:
        assert self.ego_vehicle is not None
        spectator = self.world.get_spectator()
        ego_transform = self.ego_vehicle.get_transform()
        yaw_rad = math.radians(ego_transform.rotation.yaw)
        offset = carla.Location(
            x=-8.0 * math.cos(yaw_rad),
            y=-8.0 * math.sin(yaw_rad),
            z=4.0,
        )
        spectator_transform = carla.Transform(
            ego_transform.location + offset,
            carla.Rotation(pitch=-15.0, yaw=ego_transform.rotation.yaw),
        )
        spectator.set_transform(spectator_transform)

    def setup(self) -> None:
        if self.args.headless:
            os.environ.setdefault("SDL_VIDEODRIVER", "dummy")
        pygame.init()
        pygame.font.init()
        self.display = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT), pygame.HWSURFACE | pygame.DOUBLEBUF)
        if not self.args.headless:
            pygame.display.set_caption("CARLA BasicAgent L3 HMI Loop")
        self.font = pygame.font.SysFont("monospace", 18)

        self.ego_vehicle = self._spawn_ego()
        if not self.args.headless:
            self.camera = ChaseCamera(self.world, self.ego_vehicle)
        self.collision_sensor = CollisionSensor(self.world, self.ego_vehicle, self.recorder)
        self.agent = self._build_agent()
        self.auto_drive_manager = AutoDriveManager(
            self.world,
            self.ego_vehicle,
            self.map,
            self.agent,
            self.recorder,
            self.args,
        )
        self.gt_provider = GroundTruthProvider(self.world, self.ego_vehicle, self.args.role_name)
        self.scenario_manager = ScenarioManager(
            self.client,
            self.world,
            self.ego_vehicle,
            self.recorder,
            self.args,
        )
        self.driver_state.driver_available = self.args.driver_available
        self.driver_state.hands_on = self.args.hands_on
        self.driver_state.attention_on_road = self.args.attention_on_road

        initial_snapshot = self.world.get_snapshot()
        sim_time_s = float(initial_snapshot.timestamp.elapsed_seconds)
        self.run_start_sim_time_s = sim_time_s
        if self.args.spawn_traffic:
            self.scenario_manager.spawn_traffic_flow(sim_time_s)
        self._set_new_destination(sim_time_s, reason="startup")
        if self.args.spawn_static_obstacle:
            self.scenario_manager.spawn_static_obstacle_ahead(self.args.static_obstacle_distance_m, sim_time_s)

        self.recorder.record_event(
            "ego_ready",
            sim_time_s,
            {
                "actor_id": int(self.ego_vehicle.id),
                "role_name": self.args.role_name,
                "owns_ego": self.owns_ego,
                "drive_mode": self.drive_mode,
            },
        )

    def _apply_scripted_inputs(self, hazard: dict[str, Any], intent: ControlIntent) -> ControlIntent:
        if hazard["takeover_required"]:
            if self.last_hazard_onset_s is None:
                self.last_hazard_onset_s = self.last_sim_time_s
            hazard_age_s = self.last_sim_time_s - self.last_hazard_onset_s

            if (
                self.args.scripted_driver_ready_after_s is not None
                and not self.scripted_driver_ready_applied
                and hazard_age_s >= self.args.scripted_driver_ready_after_s
            ):
                self.driver_state.driver_available = True
                self.driver_state.hands_on = True
                self.driver_state.attention_on_road = True
                self.scripted_driver_ready_applied = True
                self.recorder.record_event(
                    "scripted_driver_ready",
                    self.last_sim_time_s,
                    {"hazard_age_s": round(hazard_age_s, 3)},
                )

            if (
                self.args.scripted_manual_takeover_after_s is not None
                and not self.scripted_manual_takeover_applied
                and hazard_age_s >= self.args.scripted_manual_takeover_after_s
            ):
                intent.scripted_toggle_manual_requested = True
                self.scripted_manual_takeover_applied = True
                self.recorder.record_event(
                    "scripted_manual_takeover_requested",
                    self.last_sim_time_s,
                    {"hazard_age_s": round(hazard_age_s, 3)},
                )
        else:
            self.last_hazard_onset_s = None
            self.scripted_driver_ready_applied = False
            self.scripted_manual_takeover_applied = False
        return intent

    def _draw_overlay(self, scene_state: dict[str, Any], hazard: dict[str, Any], tor_status: dict[str, Any]) -> None:
        assert self.display is not None
        assert self.font is not None

        lines = [
            "P:auto/manual  R:reroute  O:respawn obstacle  Y:reset MRM  ESC:quit",
            f"1:driver_available={self.driver_state.driver_available}  2:hands_on={self.driver_state.hands_on}  3:attention_on_road={self.driver_state.attention_on_road}",
            f"drive_mode={self.drive_mode}  tor_state={tor_status['state']}  reason={tor_status['reason_code']}",
            f"auto_action={self.last_auto_drive_decision.action} lead_id={self.last_auto_drive_decision.lead_vehicle_id} lead_dist={self.last_auto_drive_decision.lead_distance_m}",
            f"ego x={scene_state['ego_state']['x']:.2f} y={scene_state['ego_state']['y']:.2f} yaw={scene_state['ego_state']['yaw']:.1f} speed={scene_state['ego_state']['speed']:.2f}m/s",
            f"road_id={scene_state['road_info']['road_id']} lane_id={scene_state['road_info']['lane_id']} lane_width={scene_state['road_info']['lane_width']:.2f} junction={scene_state['road_info']['is_junction']}",
            f"traffic={len(self.scenario_manager.traffic_actor_ids) if self.scenario_manager else 0} objects={len(scene_state['objects_ground_truth'])} hazard={hazard['reason_code']} dist={hazard['hazard_distance_m']} ttc={hazard['hazard_ttc_s']} risk={hazard['risk_level']}",
        ]

        background = pygame.Surface((WINDOW_WIDTH, 164), pygame.SRCALPHA)
        background.fill((0, 0, 0, 140))
        self.display.blit(background, (0, 0))

        for index, line in enumerate(lines):
            text_surface = self.font.render(line, True, (255, 255, 255))
            self.display.blit(text_surface, (16, 12 + 22 * index))

    def _stdout_tick(
        self,
        scene_state: dict[str, Any],
        external_understanding: dict[str, Any],
        hazard: dict[str, Any],
        tor_status: dict[str, Any],
    ) -> None:
        auto_drive_dict = self._auto_drive_decision_dict()
        payload = {
            "sim_time_s": scene_state["sim_time_s"],
            "drive_mode": self.drive_mode,
            "tor_state": tor_status["state"],
            "auto_drive_action": auto_drive_dict["action"],
            "traffic_vehicle_count": len(self.scenario_manager.traffic_actor_ids) if self.scenario_manager else 0,
            "ego_state": scene_state["ego_state"],
            "road_info": scene_state["road_info"],
            "route_info": scene_state["route_info"],
            "objects_ground_truth": scene_state["objects_ground_truth"],
            "external_semantics": external_understanding["external_semantics"],
            "external_risk": external_understanding["external_risk"],
            "hazard": {
                "takeover_required": hazard["takeover_required"],
                "reason_code": hazard["reason_code"],
                "hazard_distance_m": hazard["hazard_distance_m"],
                "hazard_ttc_s": hazard["hazard_ttc_s"],
                "risk_level": hazard["risk_level"],
            },
            "driver_state": asdict(self.driver_state),
        }
        print(json.dumps(payload, ensure_ascii=True))

    def _auto_drive_decision_dict(self) -> dict[str, Any]:
        decision = asdict(self.last_auto_drive_decision)
        override_control = self.last_auto_drive_decision.override_control
        if override_control is None:
            decision["override_control"] = None
        else:
            decision["override_control"] = {
                "throttle": round(float(override_control.throttle), 3),
                "brake": round(float(override_control.brake), 3),
                "steer": round(float(override_control.steer), 3),
                "hand_brake": bool(override_control.hand_brake),
                "reverse": bool(override_control.reverse),
            }
        return decision

    def run(self) -> None:
        assert self.display is not None
        assert self.ego_vehicle is not None
        assert self.agent is not None
        assert self.auto_drive_manager is not None
        assert self.gt_provider is not None
        assert self.scenario_manager is not None

        clock = pygame.time.Clock()
        while True:
            clock.tick(self.args.ui_fps)
            snapshot = self.world.wait_for_tick(seconds=2.0)
            self.last_sim_time_s = float(snapshot.timestamp.elapsed_seconds)

            intent = self.keyboard.poll_events(self.driver_state)
            if intent.quit_requested:
                break

            planned_route_points, planned_route_source = self._planned_route_points()
            scene_state = self.gt_provider.get_scene_state(
                snapshot,
                planned_route_points=planned_route_points,
                planned_route_source=planned_route_source,
            )
            external_understanding = self.external_scene_understanding.understand(scene_state)
            external_risk = external_understanding["external_risk"]

            # Adapter for the legacy local HMI/TOR/recorder path.
            # ROS2 consumers should prefer /scene/external_risk and perform
            # final L3+DMS fusion outside this CARLA scene node.
            hazard = {
                "takeover_required": bool(external_risk["has_external_hazard"]),
                "reason_code": external_risk["primary_hazard_type"],
                "hazard_distance_m": external_risk["hazard_distance_m"],
                "hazard_ttc_s": external_risk["hazard_ttc_s"],
                "risk_level": external_risk["external_risk_level"],
                "matched_object": external_risk["matched_object"],
            }
            intent = self._apply_scripted_inputs(hazard, intent)

            run_elapsed_s = self.last_sim_time_s - self.run_start_sim_time_s
            if self.args.max_duration_s is not None and run_elapsed_s >= self.args.max_duration_s:
                self.max_duration_reached = True
                self.recorder.record_event(
                    "max_duration_reached",
                    self.last_sim_time_s,
                    {"run_elapsed_s": round(run_elapsed_s, 3)},
                )
                break

            if intent.toggle_auto_requested or intent.scripted_toggle_manual_requested:
                next_mode = "MANUAL" if self.drive_mode == "AUTO" else "AUTO"
                self.drive_mode = next_mode
                payload = {"drive_mode": self.drive_mode}
                if intent.scripted_toggle_manual_requested:
                    payload["reason"] = "scripted_manual_takeover"
                self.recorder.record_event("drive_mode_changed", self.last_sim_time_s, payload)
                if self.drive_mode == "AUTO":
                    self.agent = self._build_agent()
                    self._set_new_destination(self.last_sim_time_s, reason="manual_to_auto")

            if intent.reroute_requested:
                if self.drive_mode == "AUTO":
                    self.agent = self._build_agent()
                self._set_new_destination(self.last_sim_time_s, reason="manual_reroute")

            if intent.respawn_obstacle_requested:
                self.scenario_manager.spawn_static_obstacle_ahead(self.args.static_obstacle_distance_m, self.last_sim_time_s)

            if self.drive_mode == "AUTO" and self.takeover_manager.state in ("TOR_ACTIVE", "ESCALATED_TOR"):
                if self.keyboard.manual_override_requested():
                    self.drive_mode = "MANUAL"
                    self.recorder.record_event(
                        "drive_mode_changed",
                        self.last_sim_time_s,
                        {"drive_mode": self.drive_mode, "reason": "manual_override_input"},
                    )

            tor_status = self.takeover_manager.update(
                self.last_sim_time_s,
                self.drive_mode,
                self.driver_state,
                hazard,
                reset_mrm=intent.reset_mrm_requested,
            )

            if self.agent.done() and self.drive_mode == "AUTO":
                self._set_new_destination(self.last_sim_time_s, reason="agent_route_done")

            if tor_status["state"] == "MRM":
                self.last_auto_drive_decision = AutoDriveDecision(action="MRM")
                control = self.takeover_manager.build_mrm_control(scene_state["ego_state"]["speed"])
            elif self.drive_mode == "AUTO":
                self.last_auto_drive_decision = self.auto_drive_manager.decide(
                    self.last_sim_time_s,
                    scene_state,
                    hazard,
                )
                if self.last_auto_drive_decision.override_control is not None:
                    control = self.last_auto_drive_decision.override_control
                else:
                    control = self.agent.run_step()
            else:
                self.last_auto_drive_decision = AutoDriveDecision(action="MANUAL")
                control = self.keyboard.build_manual_control()
            self.ego_vehicle.apply_control(control)

            tick_record = {
                "sim_time_s": scene_state["sim_time_s"],
                "drive_mode": self.drive_mode,
                "tor_state": tor_status["state"],
                "driver_state": asdict(self.driver_state),
                "scene_state": scene_state,
                "external_understanding": external_understanding,
                "hazard": hazard,
                "auto_drive_decision": self._auto_drive_decision_dict(),
            }
            self.recorder.record_tick(tick_record)
            if self.ros_publisher is not None:
                self.ros_publisher.publish(
                    scene_state=scene_state,
                    external_understanding=external_understanding,
                )

            if self.args.print_interval_s <= 0.0:
                self._stdout_tick(scene_state, external_understanding, hazard, tor_status)
            elif self.last_print_time_s < 0.0 or (self.last_sim_time_s - self.last_print_time_s) >= self.args.print_interval_s:
                self._stdout_tick(scene_state, external_understanding, hazard, tor_status)
                self.last_print_time_s = self.last_sim_time_s

            self._follow_spectator()
            if self.camera is not None:
                self.camera.render(self.display)
                self._draw_overlay(scene_state, hazard, tor_status)
                pygame.display.flip()

    def cleanup(self) -> None:
        if self.scenario_manager is not None:
            self.scenario_manager.destroy_owned_actors()
        if self.camera is not None:
            self.camera.destroy()
        if self.collision_sensor is not None:
            self.collision_sensor.destroy()
        if self.ego_vehicle is not None and self.owns_ego and self.ego_vehicle.is_alive:
            self.ego_vehicle.destroy()
        if self.ros_publisher is not None:
            self.ros_publisher.close()
        pygame.quit()
        self.recorder.finalize(self.last_sim_time_s)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Minimal CARLA BasicAgent + manual takeover/HMI test loop with logging."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument("--client-timeout-s", type=float, default=10.0)
    parser.add_argument("--role-name", default="ego_vehicle")
    parser.add_argument("--vehicle-blueprint", default="vehicle.tesla.model3")
    parser.add_argument("--target-speed-kmh", type=float, default=25.0)
    parser.add_argument("--initial-mode", choices=["auto", "manual"], default="auto")
    parser.add_argument("--min-destination-distance-m", type=float, default=120.0)
    parser.add_argument("--destination-samples", type=int, default=25)
    parser.add_argument("--hazard-lookahead-m", type=float, default=50.0)
    parser.add_argument("--auto-follow-lookahead-m", type=float, default=45.0)
    parser.add_argument("--lane-change-trigger-distance-m", type=float, default=18.0)
    parser.add_argument("--lane-change-min-relative-speed-mps", type=float, default=1.5)
    parser.add_argument("--lane-change-lead-speed-threshold-mps", type=float, default=3.0)
    parser.add_argument("--lane-change-check-radius-m", type=float, default=30.0)
    parser.add_argument("--lane-change-clear-ahead-m", type=float, default=18.0)
    parser.add_argument("--lane-change-clear-behind-m", type=float, default=12.0)
    parser.add_argument("--lane-change-cooldown-s", type=float, default=6.0)
    parser.add_argument("--lane-change-active-window-s", type=float, default=5.0)
    parser.add_argument("--emergency-brake-distance-m", type=float, default=8.0)
    parser.add_argument("--emergency-brake-ttc-s", type=float, default=1.2)
    parser.add_argument("--semantic-prediction-horizon-s", type=float, default=3.0)
    parser.add_argument("--semantic-cut-in-horizon-s", type=float, default=2.5)
    parser.add_argument("--semantic-prediction-step-s", type=float, default=0.5)
    parser.add_argument("--semantic-comfortable-decel-mps2", type=float, default=3.5)
    parser.add_argument("--semantic-emergency-decel-mps2", type=float, default=6.0)
    parser.add_argument("--semantic-reaction-time-s", type=float, default=1.0)
    parser.add_argument("--semantic-safety-margin-m", type=float, default=4.0)
    parser.add_argument("--spawn-traffic", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--traffic-vehicle-count", type=int, default=24)
    parser.add_argument("--traffic-spawn-exclusion-radius-m", type=float, default=18.0)
    parser.add_argument("--traffic-follow-distance-m", type=float, default=3.0)
    parser.add_argument("--traffic-speed-delta-percent", type=float, default=10.0)
    parser.add_argument("--traffic-lane-change-percent", type=float, default=20.0)
    parser.add_argument("--tm-port", type=int, default=8000)
    parser.add_argument("--spawn-static-obstacle", action="store_true")
    parser.add_argument("--static-obstacle-distance-m", type=float, default=35.0)
    parser.add_argument("--ui-fps", type=int, default=20)
    parser.add_argument("--print-interval-s", type=float, default=0.0)
    parser.add_argument("--log-dir", default="/home/vci/sim/logs")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--publish-ros", action="store_true")
    parser.add_argument("--ros-topic-namespace", default="/carla")
    parser.add_argument("--max-duration-s", type=float, default=None)
    parser.add_argument("--driver-available", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--hands-on", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--attention-on-road", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--scripted-driver-ready-after-s", type=float, default=None)
    parser.add_argument("--scripted-manual-takeover-after-s", type=float, default=None)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    app = CarlaLoopApp(args)
    try:
        app.setup()
        app.run()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        app.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
