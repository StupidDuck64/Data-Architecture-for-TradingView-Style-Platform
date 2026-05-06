#!/bin/sh
set -e

DOMAIN="${CERTBOT_DOMAIN:-localhost}"

# Substitute the domain placeholder in the nginx config
sed -i "s/\${CERTBOT_DOMAIN}/$DOMAIN/g" /etc/nginx/conf.d/default.conf

# If certificates don't exist yet, use a self-signed placeholder so nginx can start.
# Certbot will replace these once it runs successfully.
CERT_DIR="/etc/letsencrypt/live/$DOMAIN"
if [ ! -f "$CERT_DIR/fullchain.pem" ]; then
    echo "==> No TLS certificate found for $DOMAIN — generating temporary self-signed cert..."
    mkdir -p "$CERT_DIR"
    openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
        -keyout "$CERT_DIR/privkey.pem" \
        -out "$CERT_DIR/fullchain.pem" \
        -subj "/CN=$DOMAIN" 2>/dev/null
    echo "==> Temporary self-signed cert created. Run certbot to obtain a real certificate."
fi

if [ "${NGINX_AUTO_RELOAD_ENABLE:-1}" = "1" ]; then
    /usr/local/bin/nginx_auto_reload.sh &
fi

exec "$@"
