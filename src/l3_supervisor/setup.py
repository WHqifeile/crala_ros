from setuptools import setup
from glob import glob


package_name = "l3_supervisor"


setup(
    name=package_name,
    version="0.1.0",
    packages=[package_name],
    package_dir={"": "src"},
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
        ("share/" + package_name + "/launch", glob("launch/*.py")),
        ("share/" + package_name + "/config", glob("config/*.yaml")),
    ],
    install_requires=[],
    zip_safe=True,
    maintainer="vci",
    maintainer_email="vci@local",
    description="L3 takeover and minimal-risk supervisor for CARLA and DMS ROS 2 topics.",
    license="Apache-2.0",
    entry_points={
        "console_scripts": [
            "l3_supervisor_node = l3_supervisor.l3_supervisor_node:main",
        ],
    },
)
