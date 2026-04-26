#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Use this file with: source /home/vci/sim/carla-humble-0.9.15-ws/scripts/env_carla915.sh"
  exit 1
fi

if [[ -f /home/vci/sim/venvs/carla015/bin/activate ]]; then
  # shellcheck disable=SC1091
  source /home/vci/sim/venvs/carla015/bin/activate
fi

export PYTHONPATH="/home/vci/sim/venvs/carla015/lib/python3.10/site-packages:${PYTHONPATH:-}"
