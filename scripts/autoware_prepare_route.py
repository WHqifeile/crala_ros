#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import sys
import time
from typing import Iterable, Optional

import rclpy
from autoware_adapi_v1_msgs.msg import OperationModeState
from autoware_adapi_v1_msgs.srv import ChangeOperationMode
from autoware_adapi_v1_msgs.srv import ClearRoute
from autoware_adapi_v1_msgs.srv import InitializeLocalization
from autoware_adapi_v1_msgs.srv import SetRoutePoints
from geometry_msgs.msg import Pose
from geometry_msgs.msg import PoseWithCovarianceStamped
from nav_msgs.msg import Odometry
from autoware_vehicle_msgs.msg import VelocityReport
from rclpy.node import Node
from rclpy.parameter import Parameter
from rosgraph_msgs.msg import Clock


def quaternion_to_yaw(z: float, w: float) -> float:
    return math.atan2(2.0 * w * z, 1.0 - 2.0 * z * z)


def yaw_to_quaternion(yaw: float) -> tuple[float, float]:
    return math.sin(yaw / 2.0), math.cos(yaw / 2.0)


class AutowareRoutePreparer(Node):
    def __init__(
        self,
        goal_distances: list[float],
        lateral_offsets: list[float],
        carla_host: str,
        carla_port: int,
        carla_y_mode: str,
        enable_turns: bool,
        enable_lane_change: bool,
        service_timeout_sec: float,
        ready_timeout_sec: float,
        input_freshness_sec: float,
        required_samples: int,
        require_operation_mode_state: bool,
    ) -> None:
        super().__init__("autoware_route_preparer")
        self.set_parameters([Parameter("use_sim_time", value=True)])
        self.goal_distances = goal_distances
        self.lateral_offsets = lateral_offsets
        self.carla_host = carla_host
        self.carla_port = carla_port
        self.carla_y_mode = carla_y_mode
        self.enable_turns = enable_turns
        self.enable_lane_change = enable_lane_change
        self.service_timeout_sec = service_timeout_sec
        self.ready_timeout_sec = ready_timeout_sec
        self.input_freshness_sec = input_freshness_sec
        self.required_samples = max(1, required_samples)
        self.require_operation_mode_state = require_operation_mode_state

        self.gnss_pose: Optional[PoseWithCovarianceStamped] = None
        self.kinematic_state: Optional[Odometry] = None
        self.operation_mode: Optional[OperationModeState] = None
        self.velocity_report: Optional[VelocityReport] = None
        self.clock_updates = 0
        self.last_clock_value_ns: Optional[int] = None

        self.last_clock_receive_sec: Optional[float] = None
        self.last_gnss_receive_sec: Optional[float] = None
        self.last_kinematic_receive_sec: Optional[float] = None
        self.last_operation_mode_receive_sec: Optional[float] = None
        self.last_velocity_receive_sec: Optional[float] = None

        self.create_subscription(
            Clock,
            "/clock",
            self._on_clock,
            10,
        )

        self.create_subscription(
            PoseWithCovarianceStamped,
            "/sensing/gnss/pose_with_covariance",
            self._on_gnss_pose,
            10,
        )
        self.create_subscription(
            Odometry,
            "/localization/kinematic_state",
            self._on_kinematic_state,
            10,
        )
        self.create_subscription(
            OperationModeState,
            "/api/operation_mode/state",
            self._on_operation_mode,
            10,
        )
        self.create_subscription(
            VelocityReport,
            "/vehicle/status/velocity_status",
            self._on_velocity_report,
            10,
        )

        self.initialize_client = self.create_client(InitializeLocalization, "/api/localization/initialize")
        self.change_to_stop_client = self.create_client(ChangeOperationMode, "/api/operation_mode/change_to_stop")
        self.clear_route_client = self.create_client(ClearRoute, "/api/routing/clear_route")
        self.set_route_points_client = self.create_client(SetRoutePoints, "/api/routing/set_route_points")

    def _now_monotonic(self) -> float:
        return time.monotonic()

    def _mark_receive(self) -> float:
        return self._now_monotonic()

    def _is_fresh(self, stamp_sec: Optional[float]) -> bool:
        return stamp_sec is not None and (self._now_monotonic() - stamp_sec) <= self.input_freshness_sec

    def _on_clock(self, msg: Clock) -> None:
        stamp_ns = int(msg.clock.sec) * 1_000_000_000 + int(msg.clock.nanosec)
        if self.last_clock_value_ns is None or stamp_ns != self.last_clock_value_ns:
            self.clock_updates += 1
            self.last_clock_value_ns = stamp_ns
        self.last_clock_receive_sec = self._mark_receive()

    def _on_gnss_pose(self, msg: PoseWithCovarianceStamped) -> None:
        self.gnss_pose = msg
        self.last_gnss_receive_sec = self._mark_receive()

    def _on_kinematic_state(self, msg: Odometry) -> None:
        self.kinematic_state = msg
        self.last_kinematic_receive_sec = self._mark_receive()

    def _on_operation_mode(self, msg: OperationModeState) -> None:
        self.operation_mode = msg
        self.last_operation_mode_receive_sec = self._mark_receive()

    def _on_velocity_report(self, msg: VelocityReport) -> None:
        self.velocity_report = msg
        self.last_velocity_receive_sec = self._mark_receive()

    def wait_for_predicate(self, predicate, timeout_sec: float, label: str) -> bool:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            rclpy.spin_once(self, timeout_sec=0.2)
            if predicate():
                return True
        self.get_logger().error(f"Timed out waiting for {label}")
        return False

    def wait_for_services(self) -> bool:
        clients = (
            (self.initialize_client, "/api/localization/initialize"),
            (self.change_to_stop_client, "/api/operation_mode/change_to_stop"),
            (self.clear_route_client, "/api/routing/clear_route"),
            (self.set_route_points_client, "/api/routing/set_route_points"),
        )
        for client, name in clients:
            deadline = time.monotonic() + self.service_timeout_sec
            while time.monotonic() < deadline:
                rclpy.spin_once(self, timeout_sec=0.2)
                if client.service_is_ready() or client.wait_for_service(timeout_sec=0.2):
                    break
            else:
                self.get_logger().error(f"Service not ready: {name}")
                return False
        return True

    def call_service(self, client, request, label: str):
        future = client.call_async(request)
        deadline = time.monotonic() + self.service_timeout_sec
        while time.monotonic() < deadline:
            rclpy.spin_once(self, timeout_sec=0.2)
            if future.done():
                response = future.result()
                if response is None:
                    self.get_logger().error(f"{label} returned no response")
                    return None
                return response
        self.get_logger().error(f"{label} timed out")
        return None

    def current_speed_mps(self) -> Optional[float]:
        if self.velocity_report is None:
            return None
        return max(abs(self.velocity_report.longitudinal_velocity), abs(self.velocity_report.lateral_velocity))

    def is_vehicle_stopped(self, threshold_mps: float = 0.05) -> bool:
        speed = self.current_speed_mps()
        return speed is not None and speed <= threshold_mps

    def operation_mode_label(self) -> str:
        if self.operation_mode is None:
            return "unknown"
        mode_map = {
            OperationModeState.UNKNOWN: "UNKNOWN",
            OperationModeState.STOP: "STOP",
            OperationModeState.AUTONOMOUS: "AUTONOMOUS",
            OperationModeState.LOCAL: "LOCAL",
            OperationModeState.REMOTE: "REMOTE",
        }
        return mode_map.get(self.operation_mode.mode, f"UNKNOWN({self.operation_mode.mode})")

    def ensure_vehicle_stopped(self, timeout_sec: float = 20.0, threshold_mps: float = 0.05) -> bool:
        if not self.wait_for_predicate(lambda: self.velocity_report is not None, timeout_sec, "vehicle velocity status"):
            return False

        change_requested = False
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            rclpy.spin_once(self, timeout_sec=0.2)
            speed = self.current_speed_mps()
            if speed is not None and speed <= threshold_mps:
                self.get_logger().info(
                    f"Vehicle is stopped enough for localization (speed={speed:.3f} m/s, mode={self.operation_mode_label()})"
                )
                return True

            if not change_requested:
                self.get_logger().info(
                    "Vehicle is still moving before localization init; requesting STOP mode and waiting for standstill"
                )
                response = self.call_service(
                    self.change_to_stop_client,
                    ChangeOperationMode.Request(),
                    "change_to_stop",
                )
                if response is None:
                    return False
                if response.status.success:
                    change_requested = True
                else:
                    self.get_logger().warning(
                        "change_to_stop returned "
                        f"code={response.status.code} message={response.status.message}"
                    )
                    change_requested = True
            time.sleep(0.2)

        speed_text = "unknown" if self.current_speed_mps() is None else f"{self.current_speed_mps():.3f}"
        self.get_logger().error(
            f"Vehicle did not reach standstill before localization init (speed={speed_text} m/s, mode={self.operation_mode_label()})"
        )
        return False

    def best_start_pose(self) -> Optional[PoseWithCovarianceStamped]:
        if self.kinematic_state is not None and self._is_fresh(self.last_kinematic_receive_sec):
            pose = PoseWithCovarianceStamped()
            pose.header = self.kinematic_state.header
            pose.pose.pose = self.kinematic_state.pose.pose
            pose.pose.covariance = [0.0] * 36
            pose.pose.covariance[0] = 0.25
            pose.pose.covariance[7] = 0.25
            pose.pose.covariance[14] = 0.25
            pose.pose.covariance[21] = 1.0
            pose.pose.covariance[28] = 1.0
            pose.pose.covariance[35] = 1.0
            return pose

        if self.gnss_pose is not None and self._is_fresh(self.last_gnss_receive_sec):
            return self.gnss_pose
        return None

    def runtime_inputs_ready(self) -> bool:
        return (
            self.clock_updates >= self.required_samples
            and self._is_fresh(self.last_clock_receive_sec)
            and self.best_start_pose() is not None
            and self._is_fresh(self.last_velocity_receive_sec)
        )

    def initialize_localization(self, pose: PoseWithCovarianceStamped) -> bool:
        last_not_stopped_failure = False
        for attempt in range(1, 4):
            # Some stacks reject initialize_localization unless STOP mode is explicitly requested first.
            stop_response = self.call_service(
                self.change_to_stop_client,
                ChangeOperationMode.Request(),
                "change_to_stop",
            )
            if stop_response is None:
                return False
            if not stop_response.status.success:
                self.get_logger().warning(
                    "change_to_stop returned "
                    f"code={stop_response.status.code} message={stop_response.status.message}"
                )

            if not self.ensure_vehicle_stopped():
                return False

            request = InitializeLocalization.Request()
            request.pose = [pose]
            response = self.call_service(self.initialize_client, request, "initialize_localization")
            if response is None:
                return False
            if response.status.success:
                self.get_logger().info("Localization initialized from current GNSS/kinematic pose")
                return True

            self.get_logger().warning(
                "initialize_localization failed: "
                f"code={response.status.code} message={response.status.message} "
                f"(attempt {attempt}/3)"
            )
            last_not_stopped_failure = "not stopped" in response.status.message.lower()
            if not last_not_stopped_failure:
                return False
            time.sleep(1.0)

        if last_not_stopped_failure and self.best_start_pose() is not None:
            self.get_logger().warning(
                "Localization initialize kept returning 'vehicle is not stopped' while fresh pose/velocity data "
                "was already available; reusing the current localization state."
            )
            return True

        self.get_logger().error("Localization initialization failed after repeated standstill retries")
        return False

    def clear_route(self) -> None:
        request = ClearRoute.Request()
        response = self.call_service(self.clear_route_client, request, "clear_route")
        if response is None:
            return
        if response.status.success:
            self.get_logger().info("Cleared previous route")
        else:
            self.get_logger().warning(
                f"clear_route returned code={response.status.code} message={response.status.message}"
            )

    def build_goal_candidates(self, pose: PoseWithCovarianceStamped) -> Iterable[Pose]:
        yaw = quaternion_to_yaw(pose.pose.pose.orientation.z, pose.pose.pose.orientation.w)
        for direction in (1.0, -1.0):
            goal_yaw = yaw if direction > 0.0 else yaw + math.pi
            qz, qw = yaw_to_quaternion(goal_yaw)
            lateral_yaw = goal_yaw + math.pi / 2.0
            for distance in self.goal_distances:
                travel = direction * abs(distance)
                for lateral in self.lateral_offsets:
                    goal = Pose()
                    goal.position.x = (
                        pose.pose.pose.position.x
                        + travel * math.cos(goal_yaw)
                        + lateral * math.cos(lateral_yaw)
                    )
                    goal.position.y = (
                        pose.pose.pose.position.y
                        + travel * math.sin(goal_yaw)
                        + lateral * math.sin(lateral_yaw)
                    )
                    goal.position.z = pose.pose.pose.position.z
                    goal.orientation.x = 0.0
                    goal.orientation.y = 0.0
                    goal.orientation.z = qz
                    goal.orientation.w = qw
                    yield goal

    def _carla_location_from_ros(self, x: float, y: float, z: float, invert_y: bool):
        import carla

        carla_y = -y if invert_y else y
        return carla.Location(x=x, y=carla_y, z=z)

    def _ros_xy_from_carla(self, x: float, y: float, invert_y: bool) -> tuple[float, float]:
        return x, -y if invert_y else y

    def _ros_yaw_from_carla_deg(self, yaw_deg: float, invert_y: bool) -> float:
        yaw = math.radians(yaw_deg)
        return -yaw if invert_y else yaw

    def _pick_start_waypoint(self, carla_map, start_pose: PoseWithCovarianceStamped):
        import carla

        start_x = start_pose.pose.pose.position.x
        start_y = start_pose.pose.pose.position.y
        start_z = start_pose.pose.pose.position.z

        candidates = []
        if self.carla_y_mode == "invert":
            modes = (True,)
        elif self.carla_y_mode == "identity":
            modes = (False,)
        else:
            modes = (False, True)

        for invert_y in modes:
            location = self._carla_location_from_ros(start_x, start_y, start_z, invert_y)
            waypoint = carla_map.get_waypoint(
                location,
                project_to_road=True,
                lane_type=carla.LaneType.Driving,
            )
            if waypoint is None:
                continue

            ros_x, ros_y = self._ros_xy_from_carla(
                waypoint.transform.location.x,
                waypoint.transform.location.y,
                invert_y,
            )
            err = math.hypot(ros_x - start_x, ros_y - start_y)
            candidates.append((err, invert_y, waypoint))

        if not candidates:
            return None, None

        candidates.sort(key=lambda item: item[0])
        best_err, best_invert_y, best_waypoint = candidates[0]
        if best_err > 12.0:
            self.get_logger().warning(f"Closest CARLA waypoint is far from start pose ({best_err:.1f}m), fallback to straight goal candidates")
            return None, None
        return best_waypoint, best_invert_y

    def _select_forward_waypoint(self, current_waypoint, next_waypoints):
        best = next_waypoints[0]
        current_yaw = math.radians(current_waypoint.transform.rotation.yaw)
        best_score = 999.0
        for waypoint in next_waypoints:
            yaw = math.radians(waypoint.transform.rotation.yaw)
            yaw_delta = abs(math.atan2(math.sin(yaw - current_yaw), math.cos(yaw - current_yaw)))
            lane_penalty = 0.0 if waypoint.road_id == current_waypoint.road_id else 0.3
            score = yaw_delta + lane_penalty
            if score < best_score:
                best_score = score
                best = waypoint
        return best

    def build_goal_candidates_on_road(self, start_pose: PoseWithCovarianceStamped) -> list[Pose]:
        try:
            import carla
        except Exception as exc:
            self.get_logger().warning(f"CARLA Python API not available in route preparer: {exc}")
            return []

        try:
            client = carla.Client(self.carla_host, self.carla_port)
            client.set_timeout(2.0)
            world = client.get_world()
            carla_map = world.get_map()
        except Exception as exc:
            self.get_logger().warning(f"Failed to connect to CARLA map for road-aligned route goal: {exc}")
            return []

        start_waypoint, invert_y = self._pick_start_waypoint(carla_map, start_pose)
        if start_waypoint is None:
            return []

        goals: list[Pose] = []
        step_m = 5.0
        for target_distance in self.goal_distances:
            waypoint = start_waypoint
            traveled = 0.0
            while traveled < target_distance:
                step = min(step_m, target_distance - traveled)
                next_waypoints = waypoint.next(step)
                if not next_waypoints:
                    waypoint = None
                    break
                next_waypoint = self._select_forward_waypoint(waypoint, next_waypoints)
                traveled += waypoint.transform.location.distance(next_waypoint.transform.location)
                waypoint = next_waypoint

            if waypoint is None:
                continue

            ros_x, ros_y = self._ros_xy_from_carla(
                waypoint.transform.location.x,
                waypoint.transform.location.y,
                invert_y,
            )
            ros_yaw = self._ros_yaw_from_carla_deg(waypoint.transform.rotation.yaw, invert_y)
            qz, qw = yaw_to_quaternion(ros_yaw)

            goal = Pose()
            goal.position.x = ros_x
            goal.position.y = ros_y
            goal.position.z = waypoint.transform.location.z
            goal.orientation.x = 0.0
            goal.orientation.y = 0.0
            goal.orientation.z = qz
            goal.orientation.w = qw
            goals.append(goal)

        if goals:
            self.get_logger().info(f"Built {len(goals)} road-aligned route goal candidates from CARLA waypoints")
        return goals

    def _pose_from_waypoint(self, waypoint, invert_y: bool) -> Pose:
        ros_x, ros_y = self._ros_xy_from_carla(
            waypoint.transform.location.x,
            waypoint.transform.location.y,
            invert_y,
        )
        ros_yaw = self._ros_yaw_from_carla_deg(waypoint.transform.rotation.yaw, invert_y)
        qz, qw = yaw_to_quaternion(ros_yaw)

        pose = Pose()
        pose.position.x = ros_x
        pose.position.y = ros_y
        pose.position.z = waypoint.transform.location.z
        pose.orientation.x = 0.0
        pose.orientation.y = 0.0
        pose.orientation.z = qz
        pose.orientation.w = qw
        return pose

    def build_route_points_on_road(self, start_pose: PoseWithCovarianceStamped) -> tuple[Optional[Pose], list[Pose]]:
        try:
            import carla
        except Exception:
            return None, []

        try:
            client = carla.Client(self.carla_host, self.carla_port)
            client.set_timeout(2.0)
            world = client.get_world()
            carla_map = world.get_map()
        except Exception:
            return None, []

        start_waypoint, invert_y = self._pick_start_waypoint(carla_map, start_pose)
        if start_waypoint is None:
            return None, []

        target_distance = max(self.goal_distances) if self.goal_distances else 120.0
        step_m = 5.0
        traveled = 0.0
        turned = False
        lane_changed = False
        waypoints = [start_waypoint]
        current = start_waypoint

        while traveled < target_distance:
            next_waypoints = current.next(step_m)
            if not next_waypoints:
                break

            chosen = self._select_forward_waypoint(current, next_waypoints)

            if self.enable_turns and not turned and len(next_waypoints) > 1 and traveled > 25.0:
                turn_candidate = max(
                    next_waypoints,
                    key=lambda wp: abs(
                        math.atan2(
                            math.sin(math.radians(wp.transform.rotation.yaw - current.transform.rotation.yaw)),
                            math.cos(math.radians(wp.transform.rotation.yaw - current.transform.rotation.yaw)),
                        )
                    ),
                )
                turn_delta = abs(
                    math.atan2(
                        math.sin(math.radians(turn_candidate.transform.rotation.yaw - current.transform.rotation.yaw)),
                        math.cos(math.radians(turn_candidate.transform.rotation.yaw - current.transform.rotation.yaw)),
                    )
                )
                if turn_delta > 0.35:
                    chosen = turn_candidate
                    turned = True

            if self.enable_lane_change and not lane_changed and traveled > 45.0:
                left_lane = current.get_left_lane()
                right_lane = current.get_right_lane()
                candidate_lane = None
                for lane in (left_lane, right_lane):
                    if lane is None:
                        continue
                    if lane.lane_type != carla.LaneType.Driving:
                        continue
                    if lane.lane_id == current.lane_id:
                        continue
                    candidate_lane = lane
                    break
                if candidate_lane is not None:
                    current = candidate_lane
                    lane_changed = True
                    waypoints.append(current)
                    continue

            traveled += current.transform.location.distance(chosen.transform.location)
            current = chosen
            waypoints.append(current)

        if len(waypoints) < 2:
            return None, []

        sampling_interval_m = 25.0
        sampled_waypoints = [waypoints[0]]
        accumulated = 0.0
        for idx in range(1, len(waypoints)):
            accumulated += waypoints[idx - 1].transform.location.distance(waypoints[idx].transform.location)
            if accumulated >= sampling_interval_m:
                sampled_waypoints.append(waypoints[idx])
                accumulated = 0.0
        if sampled_waypoints[-1] is not waypoints[-1]:
            sampled_waypoints.append(waypoints[-1])

        route_poses = [self._pose_from_waypoint(wp, invert_y) for wp in sampled_waypoints]
        goal_pose = route_poses[-1]
        intermediate_waypoints = route_poses[1:-1]

        self.get_logger().info(
            f"Built road route with {len(intermediate_waypoints)} waypoints "
            f"(turn_used={turned}, lane_change_used={lane_changed})"
        )
        return goal_pose, intermediate_waypoints

    def set_route(self, start_pose: PoseWithCovarianceStamped) -> bool:
        def route_already_set(response) -> bool:
            return (
                response is not None
                and (not response.status.success)
                and "already set" in response.status.message.lower()
            )

        request = SetRoutePoints.Request()
        request.header.frame_id = "map"
        request.header.stamp = self.get_clock().now().to_msg()
        request.option.allow_goal_modification = True

        road_goal, road_waypoints = self.build_route_points_on_road(start_pose)
        if road_goal is not None:
            request.goal = road_goal
            request.waypoints = road_waypoints
            response = self.call_service(self.set_route_points_client, request, "set_route_points")
            if response is not None and response.status.success:
                self.get_logger().info(
                    f"Route set from CARLA road graph: goal=({road_goal.position.x:.2f}, {road_goal.position.y:.2f}, {road_goal.position.z:.2f})"
                )
                return True
            if route_already_set(response):
                self.get_logger().info("Route already set from previous request; reusing current route")
                return True
            if response is not None:
                self.get_logger().warning(
                    "CARLA graph route failed, fallback to simple candidates: "
                    f"code={response.status.code} message={response.status.message}"
                )

        route_candidates = self.build_goal_candidates_on_road(start_pose)
        fallback_candidates = list(self.build_goal_candidates(start_pose))
        if route_candidates:
            route_candidates.extend(fallback_candidates)
        else:
            route_candidates = fallback_candidates

        for goal in route_candidates:
            request.goal = goal
            request.waypoints = []
            response = self.call_service(self.set_route_points_client, request, "set_route_points")
            if response is None:
                continue
            if response.status.success:
                self.get_logger().info(
                    f"Route set: goal=({goal.position.x:.2f}, {goal.position.y:.2f}, {goal.position.z:.2f})"
                )
                return True
            if route_already_set(response):
                self.get_logger().info("Route already set from previous request; reusing current route")
                return True
            self.get_logger().warning(
                "set_route_points failed for candidate "
                f"({goal.position.x:.2f}, {goal.position.y:.2f}): "
                f"code={response.status.code} message={response.status.message}"
            )
        return False

    def wait_for_localization_ready(self) -> bool:
        if not self.wait_for_predicate(
            lambda: self._is_fresh(self.last_kinematic_receive_sec),
            self.ready_timeout_sec,
            "fresh localization output",
        ):
            return False
        self.get_logger().info("Localization output is updating after initialization")
        return True

    def wait_for_route_outputs(self) -> bool:
        def ready() -> bool:
            return self.operation_mode is not None and self._is_fresh(self.last_operation_mode_receive_sec)

        if not self.wait_for_predicate(ready, self.ready_timeout_sec, "operation mode state"):
            if self.require_operation_mode_state:
                return False
            self.get_logger().warning(
                "Operation mode state did not update in time; continuing because route set/localization are ready."
            )
            return True
        self.get_logger().info(
            "Autoware operation mode topic is alive; route setup complete enough for L3 handoff"
        )
        return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize Autoware localization and set a simple forward route.")
    parser.add_argument(
        "--goal-distances",
        default="15,25,35,50,80,120",
        help="Comma-separated forward goal distances in meters.",
    )
    parser.add_argument(
        "--lateral-offsets",
        default="0.0",
        help="Comma-separated lateral offsets in meters for route goal candidates (default: 0.0).",
    )
    parser.add_argument("--carla-host", default="127.0.0.1")
    parser.add_argument("--carla-port", type=int, default=3000)
    parser.add_argument(
        "--carla-y-mode",
        choices=("auto", "invert", "identity"),
        default="invert",
        help="ROS<->CARLA Y-axis mapping mode used for CARLA-road route building.",
    )
    parser.add_argument(
        "--enable-turns",
        action="store_true",
        help="Prefer turn branches at intersections when building CARLA road route.",
    )
    parser.add_argument(
        "--enable-lane-change",
        action="store_true",
        help="Try one lane change on multi-lane roads when building CARLA road route.",
    )
    parser.add_argument("--service-timeout-sec", type=float, default=30.0)
    parser.add_argument("--ready-timeout-sec", type=float, default=20.0)
    parser.add_argument("--pose-timeout-sec", type=float, default=60.0)
    parser.add_argument("--input-freshness-sec", type=float, default=2.0)
    parser.add_argument("--required-samples", type=int, default=3)
    parser.add_argument(
        "--skip-localization-init",
        action="store_true",
        help="Skip /api/localization/initialize and reuse the current localization state.",
    )
    parser.add_argument(
        "--require-operation-mode-state",
        action="store_true",
        help="Fail if /api/operation_mode/state does not update after route setup.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    goal_distances = [float(value.strip()) for value in args.goal_distances.split(",") if value.strip()]
    lateral_offsets = [float(value.strip()) for value in args.lateral_offsets.split(",") if value.strip()]

    rclpy.init()
    node = AutowareRoutePreparer(
        goal_distances=goal_distances,
        lateral_offsets=lateral_offsets,
        carla_host=args.carla_host,
        carla_port=args.carla_port,
        carla_y_mode=args.carla_y_mode,
        enable_turns=args.enable_turns,
        enable_lane_change=args.enable_lane_change,
        service_timeout_sec=args.service_timeout_sec,
        ready_timeout_sec=args.ready_timeout_sec,
        input_freshness_sec=args.input_freshness_sec,
        required_samples=args.required_samples,
        require_operation_mode_state=args.require_operation_mode_state,
    )
    try:
        if not node.wait_for_services():
            return 1

        if not node.wait_for_predicate(
            node.runtime_inputs_ready,
            args.pose_timeout_sec,
            "stable /clock, pose, and vehicle status inputs",
        ):
            return 1

        start_pose = node.best_start_pose()
        assert start_pose is not None
        node.get_logger().info(
            "Using start pose "
            f"({start_pose.pose.pose.position.x:.2f}, {start_pose.pose.pose.position.y:.2f}, {start_pose.pose.pose.position.z:.2f})"
        )

        if args.skip_localization_init:
            node.get_logger().info("Skipping localization initialization and reusing current localization state")
        else:
            if not node.initialize_localization(start_pose):
                return 1
            if not node.wait_for_localization_ready():
                return 1

        node.clear_route()

        if not node.set_route(start_pose):
            node.get_logger().error("Failed to set any route candidate")
            return 1

        if not node.wait_for_route_outputs():
            return 1

        return 0
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    sys.exit(main())
