#!/usr/bin/env sh
set -eu

TOKEN="${DUCKDNS_TOKEN:-}"
SUBDOMAINS="${DUCKDNS_SUBDOMAINS:-}"
INTERVAL="${DUCKDNS_UPDATE_INTERVAL_SECONDS:-300}"

if [ -z "$TOKEN" ] || [ -z "$SUBDOMAINS" ] || [ "$TOKEN" = "change-me" ] || [ "$SUBDOMAINS" = "your-subdomain" ]; then
  echo "[duckdns-auto] DUCKDNS_TOKEN or DUCKDNS_SUBDOMAINS is not set; sleeping."
  exec sleep infinity
fi

echo "[duckdns-auto] Starting dynamic DNS updates for: $SUBDOMAINS"
while true; do
  IFS=','
  for subdomain in $SUBDOMAINS; do
    response=$(curl -fsS "https://www.duckdns.org/update?domains=${subdomain}&token=${TOKEN}&ip=" || true)
    if [ "$response" = "OK" ]; then
      echo "[duckdns-auto] Updated ${subdomain}.duckdns.org"
    else
      echo "[duckdns-auto] Update failed for ${subdomain}.duckdns.org (response: ${response})"
    fi
  done
  unset IFS

  sleep "$INTERVAL"
done
