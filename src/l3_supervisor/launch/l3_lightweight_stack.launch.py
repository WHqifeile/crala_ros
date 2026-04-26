import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    default_params = os.path.join(
        get_package_share_directory("l3_supervisor"),
        "config",
        "l3_lightweight.params.yaml",
    )

    return LaunchDescription(
        [
            DeclareLaunchArgument("host", default_value="127.0.0.1"),
            DeclareLaunchArgument("port", default_value="3000"),
            DeclareLaunchArgument("role_name", default_value="ego_vehicle"),
            DeclareLaunchArgument("publish_rate_hz", default_value="10.0"),
            DeclareLaunchArgument("params_file", default_value=default_params),
            DeclareLaunchArgument("require_driver_monitoring", default_value="false"),
            Node(
                package="carla_scene_bridge",
                executable="carla_scene_bridge_node",
                name="carla_scene_bridge",
                output="screen",
                parameters=[
                    {
                        "host": LaunchConfiguration("host"),
                        "port": LaunchConfiguration("port"),
                        "role_name": LaunchConfiguration("role_name"),
                        "publish_rate_hz": LaunchConfiguration("publish_rate_hz"),
                    }
                ],
            ),
            Node(
                package="carla_scene_bridge",
                executable="hazard_assessor_node",
                name="hazard_assessor",
                output="screen",
                parameters=[
                    {
                        "role_name": LaunchConfiguration("role_name"),
                        "publish_rate_hz": LaunchConfiguration("publish_rate_hz"),
                    }
                ],
            ),
            Node(
                package="l3_supervisor",
                executable="l3_supervisor_node",
                name="l3_supervisor",
                output="screen",
                parameters=[
                    LaunchConfiguration("params_file"),
                    {
                        "role_name": LaunchConfiguration("role_name"),
                        "require_driver_monitoring": LaunchConfiguration("require_driver_monitoring"),
                    },
                ],
            ),
        ]
    )
