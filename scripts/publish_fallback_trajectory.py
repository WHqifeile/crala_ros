#!/usr/bin/env python3
import math

import rclpy
from autoware_perception_msgs.msg import PredictedObjects
from autoware_planning_msgs.msg import Trajectory, TrajectoryPoint
from autoware_vehicle_msgs.msg import HazardLightsCommand, TurnIndicatorsCommand
from nav_msgs.msg import Odometry
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from rclpy.parameter import Parameter


class FallbackTrajectoryPublisher(Node):
    def __init__(self) -> None:
        super().__init__('fallback_trajectory_publisher')
        self.set_parameters([Parameter("use_sim_time", value=True)])
        self._odom = None

        self.create_subscription(
            Odometry,
            '/localization/kinematic_state',
            self._on_odom,
            10,
        )
        self._pub_main = self.create_publisher(
            Trajectory,
            '/planning/scenario_planning/trajectory',
            10,
        )
        self._pub_legacy = self.create_publisher(
            Trajectory,
            '/planning/trajectory',
            10,
        )
        self._pub_objects = self.create_publisher(
            PredictedObjects,
            '/perception/object_recognition/objects',
            10,
        )
        self._pub_turn = self.create_publisher(
            TurnIndicatorsCommand,
            '/planning/turn_indicators_cmd',
            10,
        )
        self._pub_hazard = self.create_publisher(
            HazardLightsCommand,
            '/planning/hazard_lights_cmd',
            10,
        )
        self.create_timer(0.1, self._on_timer)
        self.get_logger().info('Publishing fallback trajectory/object/signal topics')

    def _on_odom(self, msg: Odometry) -> None:
        self._odom = msg

    @staticmethod
    def _yaw_from_quat(x: float, y: float, z: float, w: float) -> float:
        siny_cosp = 2.0 * (w * z + x * y)
        cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
        return math.atan2(siny_cosp, cosy_cosp)

    def _build_trajectory(self) -> Trajectory | None:
        if self._odom is None:
            return None

        pose = self._odom.pose.pose
        q = pose.orientation
        yaw = self._yaw_from_quat(q.x, q.y, q.z, q.w)

        traj = Trajectory()
        traj.header.stamp = self.get_clock().now().to_msg()
        traj.header.frame_id = 'map'

        speed = 3.0
        dt = 0.2
        step = speed * dt
        num_points = 50

        for i in range(num_points):
            p = TrajectoryPoint()
            p.time_from_start.sec = int(i * dt)
            p.time_from_start.nanosec = int((i * dt - int(i * dt)) * 1e9)

            dist = i * step
            p.pose.position.x = pose.position.x + dist * math.cos(yaw)
            p.pose.position.y = pose.position.y + dist * math.sin(yaw)
            p.pose.position.z = pose.position.z
            p.pose.orientation = pose.orientation

            p.longitudinal_velocity_mps = float(speed if i < num_points - 1 else 0.0)
            p.lateral_velocity_mps = 0.0
            p.acceleration_mps2 = 0.0
            p.heading_rate_rps = 0.0
            p.front_wheel_angle_rad = 0.0
            p.rear_wheel_angle_rad = 0.0
            traj.points.append(p)

        return traj

    def _on_timer(self) -> None:
        traj = self._build_trajectory()
        if traj is None:
            return

        now = self.get_clock().now().to_msg()

        objects = PredictedObjects()
        objects.header.stamp = now
        objects.header.frame_id = 'map'

        turn = TurnIndicatorsCommand()
        turn.stamp = now
        turn.command = TurnIndicatorsCommand.DISABLE

        hazard = HazardLightsCommand()
        hazard.stamp = now
        hazard.command = HazardLightsCommand.DISABLE

        self._pub_main.publish(traj)
        self._pub_legacy.publish(traj)
        self._pub_objects.publish(objects)
        self._pub_turn.publish(turn)
        self._pub_hazard.publish(hazard)


def main() -> None:
    rclpy.init()
    node = FallbackTrajectoryPublisher()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
