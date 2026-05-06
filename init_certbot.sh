#!/usr/bin/env bash
# Bootstrap HTTPS automation variables and start automation services.
# Usage: ./init_certbot.sh <domain> <email>
set -euo pipefail

DOMAIN="${1:?Usage: $0 <domain> <email>}"
EMAIL="${2:?Usage: $0 <domain> <email>}"
ENV_FILE=".env"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required"
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required"
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  touch "$ENV_FILE"
fi

upsert_env() {
  key="$1"
  value="$2"
  if grep -q "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
  else
    printf "%s=%s\n" "$key" "$value" >> "$ENV_FILE"
  fi
}

# If the domain is a duckdns host, infer subdomain for duckdns-auto.
DUCK_SUBDOMAIN=""
if [[ "$DOMAIN" =~ ^([a-zA-Z0-9-]+)\.duckdns\.org$ ]]; then
  DUCK_SUBDOMAIN="${BASH_REMATCH[1]}"
fi

upsert_env "CERTBOT_DOMAIN" "$DOMAIN"
upsert_env "CERTBOT_EMAIL" "$EMAIL"

if [ -n "$DUCK_SUBDOMAIN" ] && ! grep -q '^DUCKDNS_SUBDOMAINS=' "$ENV_FILE"; then
  upsert_env "DUCKDNS_SUBDOMAINS" "$DUCK_SUBDOMAIN"
fi

# Ensure automation toggles have sane defaults.
if ! grep -q '^CERTBOT_RENEW_INTERVAL_SECONDS=' "$ENV_FILE"; then
  upsert_env "CERTBOT_RENEW_INTERVAL_SECONDS" "43200"
fi
if ! grep -q '^NGINX_AUTO_RELOAD_ENABLE=' "$ENV_FILE"; then
  upsert_env "NGINX_AUTO_RELOAD_ENABLE" "1"
fi
if ! grep -q '^NGINX_RELOAD_INTERVAL_SECONDS=' "$ENV_FILE"; then
  upsert_env "NGINX_RELOAD_INTERVAL_SECONDS" "21600"
fi
if ! grep -q '^DUCKDNS_UPDATE_INTERVAL_SECONDS=' "$ENV_FILE"; then
  upsert_env "DUCKDNS_UPDATE_INTERVAL_SECONDS" "300"
fi

echo "Starting HTTPS automation services..."
docker compose up -d nginx certbot-auto duckdns-auto

echo ""
echo "Current service status:"
docker compose ps nginx certbot-auto duckdns-auto

echo ""
echo "Recent certbot logs:"
docker logs --tail 20 certbot-auto || true

echo ""
echo "Recent duckdns logs:"
docker logs --tail 20 duckdns-auto || true

echo ""
echo "Bootstrap complete for https://${DOMAIN}"
