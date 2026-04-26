#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

import rclpy
from rclpy.node import Node
from rosidl_runtime_py.utilities import get_message


class TopicProbe(Node):
    def __init__(self, topic: str, count: int, listed_only: bool, print_once: bool) -> None:
        super().__init__("ros_topic_probe")
        self.topic = topic
        self.count_target = count
        self.listed_only = listed_only
        self.print_once = print_once
        self.subscription = None
        self.message_cls = None
        self.count = 0

    def _topic_type(self) -> Optional[str]:
        for name, types in self.get_topic_names_and_types():
            if name == self.topic and types:
                return types[0]
        return None

    def _ensure_subscription(self) -> bool:
        if self.listed_only:
            return self._topic_type() is not None

        if self.subscription is not None:
            return True

        topic_type = self._topic_type()
        if topic_type is None:
            return False

        self.message_cls = get_message(topic_type)
        self.subscription = self.create_subscription(self.message_cls, self.topic, self._on_message, 10)
        return True

    def _on_message(self, msg) -> None:
        self.count += 1
        if self.print_once and self.count == 1:
            print(msg)

    def wait(self, timeout_sec: float) -> bool:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if self._ensure_subscription():
                if self.listed_only:
                    return True
                rclpy.spin_once(self, timeout_sec=0.2)
                if self.count >= self.count_target:
                    return True
            else:
                rclpy.spin_once(self, timeout_sec=0.2)
            time.sleep(0.05)
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait until a ROS topic appears or publishes messages.")
    parser.add_argument("topic")
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--listed-only", action="store_true")
    parser.add_argument("--print-once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rclpy.init()
    node = TopicProbe(
        topic=args.topic,
        count=max(1, args.count),
        listed_only=args.listed_only,
        print_once=args.print_once,
    )
    try:
        return 0 if node.wait(args.timeout_sec) else 1
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    sys.exit(main())
