from setuptools import setup


package_name = "external_dms_pose"


setup(
    name=package_name,
    version="0.1.0",
    packages=[package_name],
    package_dir={"": "src"},
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=[],
    zip_safe=True,
    maintainer="vci",
    maintainer_email="vci@local",
    description="External DMS camera bridge with head pose publishing for ROS 2.",
    license="Apache-2.0",
    entry_points={
        "console_scripts": [
            "external_dms_pose_node = external_dms_pose.external_dms_pose_node:main",
        ],
    },
)
