from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any, Optional

from geometry_msgs.msg import PoseStamped
from l3_interfaces.msg import GroundTruthObject
from l3_interfaces.msg import GroundTruthObjectArray
from l3_interfaces.msg import RoadInfo
from nav_msgs.msg import Odometry, Path
import rclpy
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from std_msgs.msg import Bool, String


CARLA_ROOT = Path("/home/vci/sim/carla-0.9.15")
CARLA_PYTHONAPI = CARLA_ROOT / "PythonAPI" / "carla"
CARLA_VENV_SITE = Path("/home/vci/sim/venvs/carla015/lib/python3.10/site-packages")

for path in (CARLA_VENV_SITE, CARLA_PYTHONAPI):
    if path.exists() and str(path) not in sys.path:
        sys.path.append(str(path))

import carla


ROUTE_POINT_COUNT = 20
ROUTE_STEP_M = 2.0


def normalize_angle_deg(angle_deg: float) -> float:
    while angle_deg > 180.0:
        angle_deg -= 360.0
    while angle_deg < -180.0:
        angle_deg += 360.0
    return angle_deg


def speed_mps(actor: carla.Actor) -> float:
    velocity = actor.get_velocity()
    return math.sqrt(velocity.x ** 2 + velocity.y ** 2 + velocity.z ** 2)


def quaternion_from_yaw_deg(yaw_deg: float) -> tuple[float, float, float, float]:
    half_yaw = math.radians(yaw_deg) / 2.0
    return (0.0, 0.0, math.sin(half_yaw), math.cos(half_yaw))


class GroundTruthProvider:
    def __init__(self, world: carla.World, ego_vehicle: carla.Vehicle) -> None:
        self.world = world
        self.map = world.get_map()
        self.ego_vehicle = ego_vehicle

    def bind_ego_vehicle(self, ego_vehicle: carla.Vehicle) -> None:
        self.ego_vehicle = ego_vehicle

    @staticmethod
    def _choose_next_waypoint(waypoint: carla.Waypoint, step_m: float) -> Optional[carla.Waypoint]:
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
            transform = waypoint.transform
            location = transform.location
            route_points.append(
                {
                    "x": round(float(location.x), 3),
                    "y": round(float(location.y), 3),
                    "yaw": round(float(transform.rotation.yaw), 3),
                    "road_id": int(waypoint.road_id),
                    "lane_id": int(waypoint.lane_id),
                    "is_junction": bool(waypoint.is_junction),
                }
            )
        return route_points

    def _objects_ground_truth(self, ego_location: carla.Location, max_distance_m: float) -> list[dict[str, Any]]:
        objects: list[dict[str, Any]] = []
        for actor in self.world.get_actors():
            if actor.id == self.ego_vehicle.id:
                continue
            if not (actor.type_id.startswith("vehicle.") or actor.type_id.startswith("walker.")):
                continue
            actor_location = actor.get_location()
            if actor_location.distance(ego_location) > max_distance_m:
                continue
            bbox_extent = actor.bounding_box.extent
            objects.append(
                {
                    "id": int(actor.id),
                    "role_name": actor.attributes.get("role_name", ""),
                    "object_type": actor.type_id,
                    "x": round(float(actor_location.x), 3),
                    "y": round(float(actor_location.y), 3),
                    "yaw": round(float(actor.get_transform().rotation.yaw), 3),
                    "speed": round(speed_mps(actor), 3),
                    "bbox": {
                        "extent_x": round(float(bbox_extent.x), 3),
                        "extent_y": round(float(bbox_extent.y), 3),
                        "extent_z": round(float(bbox_extent.z), 3),
                    },
                }
            )
        objects.sort(key=lambda entry: entry["id"])
        return objects

    def get_scene_state(self, snapshot: carla.WorldSnapshot) -> dict[str, Any]:
        ego_transform = self.ego_vehicle.get_transform()
        ego_location = ego_transform.location
        ego_waypoint = self.map.get_waypoint(
            ego_location,
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        route_points = self._forward_route_points(ego_waypoint, ROUTE_POINT_COUNT, ROUTE_STEP_M)

        return {
            "sim_time_s": round(float(snapshot.timestamp.elapsed_seconds), 3),
            "ego_state": {
                "x": round(float(ego_location.x), 3),
                "y": round(float(ego_location.y), 3),
                "yaw": round(float(ego_transform.rotation.yaw), 3),
                "speed": round(speed_mps(self.ego_vehicle), 3),
            },
            "road_info": {
                "road_id": int(ego_waypoint.road_id),
                "lane_id": int(ego_waypoint.lane_id),
                "lane_width": round(float(ego_waypoint.lane_width), 3),
                "is_junction": bool(ego_waypoint.is_junction),
            },
            "route_info": {
                "lookahead_m": round(ROUTE_POINT_COUNT * ROUTE_STEP_M, 3),
                "points": route_points,
            },
            "objects_ground_truth": self._objects_ground_truth(ego_location, max_distance_m=80.0),
        }


class CarlaSceneBridgeNode(Node):
    def __init__(self) -> None:
        super().__init__("carla_scene_bridge")
        self.declare_parameter("host", "127.0.0.1")
        self.declare_parameter("port", 3000)
        self.declare_parameter("timeout", 10.0)
        self.declare_parameter("role_name", "ego_vehicle")
        self.declare_parameter("publish_rate_hz", 10.0)

        self.host = str(self.get_parameter("host").value)
        self.port = int(self.get_parameter("port").value)
        self.timeout = float(self.get_parameter("timeout").value)
        self.role_name = str(self.get_parameter("role_name").value)
        self.publish_rate_hz = float(self.get_parameter("publish_rate_hz").value)

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.ego_vehicle: Optional[carla.Vehicle] = None
        self.gt_provider: Optional[GroundTruthProvider] = None
        self.last_warn_ns = 0
        self.ego_odometry_topic = f"/carla/{self.role_name}/odometry"
        self.ego_waypoints_topic = f"/carla/{self.role_name}/waypoints"

        self.scene_available_pub = self.create_publisher(Bool, "/carla/scene/available", 10)
        self.scene_state_pub = self.create_publisher(String, "/carla/scene/state", 10)
        self.road_info_pub = self.create_publisher(RoadInfo, "/carla/scene/road_info", 10)
        self.objects_pub = self.create_publisher(GroundTruthObjectArray, "/carla/scene/objects_ground_truth", 10)
        self.ego_odometry_pub = self.create_publisher(Odometry, self.ego_odometry_topic, 10)
        self.ego_waypoints_pub = self.create_publisher(Path, self.ego_waypoints_topic, 10)

        self.timer = self.create_timer(1.0 / max(self.publish_rate_hz, 1.0), self._tick)
        self.get_logger().info(
            f"carla_scene_bridge started: host={self.host} port={self.port} role_name={self.role_name}"
        )

    def _ensure_world(self) -> bool:
        try:
            if self.client is None:
                self.client = carla.Client(self.host, self.port)
                self.client.set_timeout(self.timeout)
            if self.world is None:
                self.world = self.client.get_world()
            return True
        except RuntimeError as exc:
            self.world = None
            self.ego_vehicle = None
            self.gt_provider = None
            now_ns = self.get_clock().now().nanoseconds
            if now_ns - self.last_warn_ns > int(5e9):
                self.last_warn_ns = now_ns
                self.get_logger().warning(f"waiting for CARLA server {self.host}:{self.port}: {exc}")
            return False

    def _find_ego_vehicle(self) -> Optional[carla.Vehicle]:
        assert self.world is not None
        for actor in self.world.get_actors().filter("vehicle.*"):
            if actor.attributes.get("role_name", "") == self.role_name:
                return actor
        return None

    def _warn_missing_ego(self) -> None:
        now_ns = self.get_clock().now().nanoseconds
        if now_ns - self.last_warn_ns > int(5e9):
            self.last_warn_ns = now_ns
            self.get_logger().warning(
                f"ego vehicle with role_name='{self.role_name}' not found in CARLA world; waiting..."
            )

    @staticmethod
    def _publish_json(publisher, payload: dict[str, Any]) -> None:
        msg = String()
        msg.data = json.dumps(payload, ensure_ascii=True)
        publisher.publish(msg)

    def _publish_odometry(self, ego_vehicle: carla.Vehicle) -> None:
        transform = ego_vehicle.get_transform()
        location = transform.location
        rotation = transform.rotation
        linear_velocity = ego_vehicle.get_velocity()
        angular_velocity = ego_vehicle.get_angular_velocity()
        qx, qy, qz, qw = quaternion_from_yaw_deg(float(rotation.yaw))

        msg = Odometry()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "map"
        msg.child_frame_id = f"{self.role_name}/base_link"
        msg.pose.pose.position.x = float(location.x)
        msg.pose.pose.position.y = float(location.y)
        msg.pose.pose.position.z = float(location.z)
        msg.pose.pose.orientation.x = qx
        msg.pose.pose.orientation.y = qy
        msg.pose.pose.orientation.z = qz
        msg.pose.pose.orientation.w = qw
        msg.twist.twist.linear.x = float(linear_velocity.x)
        msg.twist.twist.linear.y = float(linear_velocity.y)
        msg.twist.twist.linear.z = float(linear_velocity.z)
        msg.twist.twist.angular.x = float(angular_velocity.x)
        msg.twist.twist.angular.y = float(angular_velocity.y)
        msg.twist.twist.angular.z = float(angular_velocity.z)
        self.ego_odometry_pub.publish(msg)

    def _publish_waypoints(self, route_info: dict[str, Any], ego_vehicle: carla.Vehicle) -> None:
        ego_transform = ego_vehicle.get_transform()
        path_msg = Path()
        path_msg.header.stamp = self.get_clock().now().to_msg()
        path_msg.header.frame_id = "map"

        ego_pose = PoseStamped()
        ego_pose.header = path_msg.header
        ego_pose.pose.position.x = float(ego_transform.location.x)
        ego_pose.pose.position.y = float(ego_transform.location.y)
        ego_pose.pose.position.z = float(ego_transform.location.z)
        qx, qy, qz, qw = quaternion_from_yaw_deg(float(ego_transform.rotation.yaw))
        ego_pose.pose.orientation.x = qx
        ego_pose.pose.orientation.y = qy
        ego_pose.pose.orientation.z = qz
        ego_pose.pose.orientation.w = qw
        path_msg.poses.append(ego_pose)

        for point in route_info.get("points", []):
            pose = PoseStamped()
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

    def _publish_road_info(self, road_info: dict[str, Any]) -> None:
        msg = RoadInfo()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "map"
        msg.road_id = int(road_info["road_id"])
        msg.lane_id = int(road_info["lane_id"])
        msg.lane_width = float(road_info["lane_width"])
        msg.is_junction = bool(road_info["is_junction"])
        self.road_info_pub.publish(msg)

    def _publish_objects(self, objects: list[dict[str, Any]]) -> None:
        msg = GroundTruthObjectArray()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "map"
        for entry in objects:
            obj = GroundTruthObject()
            obj.id = int(entry["id"])
            obj.role_name = str(entry["role_name"])
            obj.object_type = str(entry["object_type"])
            obj.x = float(entry["x"])
            obj.y = float(entry["y"])
            obj.yaw_deg = float(entry["yaw"])
            obj.speed_mps = float(entry["speed"])
            obj.bbox_x = float(entry["bbox"]["extent_x"])
            obj.bbox_y = float(entry["bbox"]["extent_y"])
            obj.bbox_z = float(entry["bbox"]["extent_z"])
            msg.objects.append(obj)
        self.objects_pub.publish(msg)

    def _tick(self) -> None:
        try:
            if not self._ensure_world():
                self.scene_available_pub.publish(Bool(data=False))
                return

            if self.ego_vehicle is None or not self.ego_vehicle.is_alive:
                self.ego_vehicle = self._find_ego_vehicle()
                if self.ego_vehicle is not None:
                    if self.gt_provider is None:
                        self.gt_provider = GroundTruthProvider(self.world, self.ego_vehicle)
                    else:
                        self.gt_provider.bind_ego_vehicle(self.ego_vehicle)
                    self.get_logger().info(
                        f"attached to ego vehicle id={self.ego_vehicle.id} role_name={self.role_name}"
                    )

            if self.ego_vehicle is None or self.gt_provider is None:
                self.scene_available_pub.publish(Bool(data=False))
                self._warn_missing_ego()
                return

            snapshot = self.world.get_snapshot()
            scene_state = self.gt_provider.get_scene_state(snapshot)

            self.scene_available_pub.publish(Bool(data=True))
            self._publish_json(self.scene_state_pub, scene_state)
            self._publish_road_info(scene_state["road_info"])
            self._publish_objects(scene_state["objects_ground_truth"])
            self._publish_odometry(self.ego_vehicle)
            self._publish_waypoints(scene_state["route_info"], self.ego_vehicle)
        except RuntimeError as exc:
            self.scene_available_pub.publish(Bool(data=False))
            self.get_logger().warning(f"CARLA read failed: {exc}")


def main() -> None:
    rclpy.init()
    node = CarlaSceneBridgeNode()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
