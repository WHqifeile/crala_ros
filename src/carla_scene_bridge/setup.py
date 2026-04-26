from glob import glob
from setuptools import setup


package_name = "carla_scene_bridge"


setup(
    name=package_name,
    version="0.1.0",
    packages=[package_name],
    package_dir={"": "src"},
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
        ("share/" + package_name + "/launch", glob("launch/*.py")),
    ],
    install_requires=[],
    zip_safe=True,
    maintainer="vci",
    maintainer_email="vci@local",
    description="Bridge CARLA scene state, road info, route info, objects, and hazard semantics into ROS 2 topics.",
    license="Apache-2.0",
    entry_points={
        "console_scripts": [
            "carla_scene_bridge_node = carla_scene_bridge.carla_scene_bridge_node:main",
            "hazard_assessor_node = carla_scene_bridge.hazard_assessor_node:main",
        ],
    },
)
