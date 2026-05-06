#!/usr/bin/env sh
set -eu

DOMAIN="${CERTBOT_DOMAIN:-}"
EMAIL="${CERTBOT_EMAIL:-}"
WEBROOT="/var/www/certbot"
SLEEP_SECS="${CERTBOT_RENEW_INTERVAL_SECONDS:-43200}"
LE_DIR="/etc/letsencrypt"
RENEW_WINDOW_SECONDS="${CERTBOT_RENEW_WINDOW_SECONDS:-2592000}"

if [ -z "$DOMAIN" ] || [ -z "$EMAIL" ] || [ "$DOMAIN" = "your-subdomain.duckdns.org" ] || [ "$EMAIL" = "you@example.com" ]; then
  echo "[certbot-auto] CERTBOT_DOMAIN or CERTBOT_EMAIL is not set; sleeping."
  exec sleep infinity
fi

cleanup_broken_state() {
  live_dir="$LE_DIR/live/$DOMAIN"
  fullchain="$live_dir/fullchain.pem"
  archive_dir="$LE_DIR/archive/$DOMAIN"
  renewal_conf="$LE_DIR/renewal/$DOMAIN.conf"

  # If nginx bootstrap created plain files in live/$DOMAIN, remove them so
  # certbot can recreate proper symlinks in the canonical cert path.
  if [ -f "$fullchain" ] && [ ! -L "$fullchain" ]; then
    echo "[certbot-auto] Removing bootstrap cert files for $DOMAIN"
    rm -rf "$live_dir" "$archive_dir" "$renewal_conf"
  fi

  # Remove any renewal config that references non-existent host paths.
  if [ -f "$renewal_conf" ] && grep -q '/var/lib/docker/volumes/' "$renewal_conf"; then
    echo "[certbot-auto] Removing incompatible renewal config for $DOMAIN"
    rm -rf "$live_dir" "$archive_dir" "$renewal_conf"
  fi

  # If archive data exists without a renewal config, certbot cannot proceed.
  # Reset to a clean state and re-issue in-place.
  if [ -d "$archive_dir" ] && [ ! -f "$renewal_conf" ]; then
    echo "[certbot-auto] Resetting inconsistent archive state for $DOMAIN"
    rm -rf "$live_dir" "$archive_dir"
  fi
}

issue_or_renew() {
  cleanup_broken_state
  echo "[certbot-auto] Requesting/refreshing cert for $DOMAIN"
  certbot certonly \
    --webroot \
    --webroot-path "$WEBROOT" \
    --non-interactive \
    --agree-tos \
    --no-eff-email \
    --email "$EMAIL" \
    --keep-until-expiring \
    --cert-name "$DOMAIN" \
    -d "$DOMAIN"
}

cert_is_fresh() {
  fullchain="$LE_DIR/live/$DOMAIN/fullchain.pem"
  [ -f "$fullchain" ] && openssl x509 -checkend "$RENEW_WINDOW_SECONDS" -noout -in "$fullchain" >/dev/null 2>&1
}

if cert_is_fresh; then
  echo "[certbot-auto] Certificate is still valid beyond renewal window; skipping initial request."
elif issue_or_renew; then
  echo "[certbot-auto] Initial certificate check completed."
else
  echo "[certbot-auto] Initial certificate check failed; will retry on next interval."
fi

echo "[certbot-auto] Starting renewal loop."
while true; do
  sleep "$SLEEP_SECS"
  if cert_is_fresh; then
    echo "[certbot-auto] Certificate still fresh; skipping this cycle."
  else
    issue_or_renew || true
  fi
done
