#!/usr/bin/env python3
import rclpy
from autoware_perception_msgs.msg import TrafficLightGroupArray
from autoware_perception_msgs.msg import PredictedObjects
from geometry_msgs.msg import Pose
from nav_msgs.msg import OccupancyGrid
from rclpy.executors import ExternalShutdownException
from rclpy.parameter import Parameter
from sensor_msgs.msg import PointCloud2
from sensor_msgs.msg import PointField
from std_msgs.msg import Header
from rclpy.node import Node


class EmptyObjectsPublisher(Node):
    def __init__(self) -> None:
        super().__init__("empty_objects_publisher")
        self.set_parameters([Parameter("use_sim_time", value=True)])
        self._pub_objects = self.create_publisher(
            PredictedObjects,
            "/perception/object_recognition/objects",
            10,
        )
        self._pub_occupancy = self.create_publisher(
            OccupancyGrid,
            "/perception/occupancy_grid_map/map",
            10,
        )
        self._pub_parking_costmap = self.create_publisher(
            OccupancyGrid,
            "/planning/scenario_planning/parking/costmap_generator/occupancy_grid",
            10,
        )
        self._pub_obstacle_pc = self.create_publisher(
            PointCloud2,
            "/perception/obstacle_segmentation/pointcloud",
            10,
        )
        self._pub_traffic = self.create_publisher(
            TrafficLightGroupArray,
            "/perception/traffic_light_recognition/traffic_signals",
            10,
        )
        self.create_timer(0.1, self._on_timer)
        self.get_logger().info(
            "Publishing empty perception stubs: objects, occupancy grid, obstacle pointcloud, traffic lights"
        )

    def _build_empty_grid(self, stamp) -> OccupancyGrid:
        grid = OccupancyGrid()
        grid.header = Header()
        grid.header.stamp = stamp
        grid.header.frame_id = "map"
        grid.info.map_load_time = stamp
        grid.info.resolution = 1.0
        grid.info.width = 1
        grid.info.height = 1
        grid.info.origin = Pose()
        grid.data = [-1]
        return grid

    def _on_timer(self) -> None:
        stamp = self.get_clock().now().to_msg()

        msg = PredictedObjects()
        msg.header.stamp = stamp
        msg.header.frame_id = "map"
        self._pub_objects.publish(msg)

        grid = self._build_empty_grid(stamp)
        self._pub_occupancy.publish(grid)
        self._pub_parking_costmap.publish(grid)

        traffic = TrafficLightGroupArray()
        traffic.stamp = stamp
        self._pub_traffic.publish(traffic)

        pointcloud = PointCloud2()
        pointcloud.header.stamp = stamp
        # Keep frame/fields aligned with AEB expectations even for an empty cloud.
        pointcloud.header.frame_id = "base_link"
        pointcloud.height = 1
        pointcloud.width = 0
        pointcloud.fields = [
            PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
            PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
            PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
        ]
        pointcloud.is_bigendian = False
        pointcloud.is_dense = True
        pointcloud.point_step = 12
        pointcloud.row_step = 0
        pointcloud.data = b""
        self._pub_obstacle_pc.publish(pointcloud)


def main() -> None:
    rclpy.init()
    node = EmptyObjectsPublisher()
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
