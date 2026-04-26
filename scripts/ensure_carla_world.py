#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait for CARLA and ensure the desired world is loaded.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument("--map", dest="map_name", default="Town01")
    parser.add_argument("--timeout-sec", type=float, default=90.0)
    parser.add_argument("--tick-timeout-sec", type=float, default=20.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        import carla
    except Exception as exc:  # pragma: no cover
        print(f"[carla-world] failed to import carla python API: {exc}", file=sys.stderr)
        return 1

    deadline = time.monotonic() + args.timeout_sec
    last_error = "uninitialized"

    while time.monotonic() < deadline:
        try:
            client = carla.Client(args.host, args.port)
            client.set_timeout(min(20.0, args.tick_timeout_sec))

            world = client.get_world()
            current_map = world.get_map().name
            print(f"[carla-world] connected: current_map={current_map}")

            if current_map != args.map_name:
                print(f"[carla-world] loading map={args.map_name}")
                world = client.load_world(args.map_name)

            start = time.monotonic()
            while time.monotonic() - start < args.tick_timeout_sec:
                world.wait_for_tick(seconds=1.0)
                current_map = world.get_map().name
                if current_map == args.map_name:
                    print(f"[carla-world] ready: map={current_map}")
                    return 0

            last_error = f"timed out waiting for tick on map={args.map_name}"
        except Exception as exc:
            last_error = str(exc)

        time.sleep(2.0)

    print(f"[carla-world] failed: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
