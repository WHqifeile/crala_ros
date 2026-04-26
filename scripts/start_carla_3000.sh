#!/usr/bin/env bash
set -euo pipefail

QUALITY_LEVEL="${CARLA_QUALITY_LEVEL:-Epic}"
RENDER_OFFSCREEN="${CARLA_RENDER_OFFSCREEN:-true}"

cd /home/vci/sim/carla-0.9.15
if [[ "${RENDER_OFFSCREEN}" == "true" ]]; then
  exec ./CarlaUE4.sh -RenderOffScreen -quality-level="${QUALITY_LEVEL}" -carla-rpc-port=3000
fi

exec env \
  DISPLAY=:0 \
  XAUTHORITY=/run/user/1000/gdm/Xauthority \
  SDL_VIDEODRIVER=x11 \
  __GLX_VENDOR_LIBRARY_NAME=nvidia \
  ./CarlaUE4.sh -prefernvidia -quality-level="${QUALITY_LEVEL}" -carla-rpc-port=3000
