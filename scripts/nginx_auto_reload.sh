#!/usr/bin/env sh
set -eu

# Reload nginx periodically so renewed certificates are picked up without
# requiring docker exec from another container.
INTERVAL="${NGINX_RELOAD_INTERVAL_SECONDS:-21600}"

while true; do
  sleep "$INTERVAL"
  nginx -s reload || true
done
