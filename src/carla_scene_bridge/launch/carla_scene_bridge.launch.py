from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument("host", default_value="127.0.0.1"),
            DeclareLaunchArgument("port", default_value="3000"),
            DeclareLaunchArgument("role_name", default_value="ego_vehicle"),
            DeclareLaunchArgument("publish_rate_hz", default_value="10.0"),
            DeclareLaunchArgument("hazard_lookahead_m", default_value="50.0"),
            Node(
                package="carla_scene_bridge",
                executable="carla_scene_bridge_node",
                name="carla_scene_bridge",
                output="screen",
                parameters=[
                    {"host": LaunchConfiguration("host")},
                    {"port": LaunchConfiguration("port")},
                    {"role_name": LaunchConfiguration("role_name")},
                    {"publish_rate_hz": LaunchConfiguration("publish_rate_hz")},
                ],
            ),
            Node(
                package="carla_scene_bridge",
                executable="hazard_assessor_node",
                name="hazard_assessor",
                output="screen",
                parameters=[
                    {"role_name": LaunchConfiguration("role_name")},
                    {"publish_rate_hz": LaunchConfiguration("publish_rate_hz")},
                    {"hazard_lookahead_m": LaunchConfiguration("hazard_lookahead_m")},
                ],
            ),
        ]
    )
