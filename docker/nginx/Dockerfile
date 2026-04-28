# ─────────────────────────────────────────────────────────────────────────────
# Multi-stage build: Node.js (React build) → Nginx (serve + reverse proxy)
# ─────────────────────────────────────────────────────────────────────────────

# Stage 1: Build the React frontend
FROM node:20-alpine AS build

WORKDIR /app

# Install dependencies first (layer caching)
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --legacy-peer-deps 2>/dev/null || npm install --legacy-peer-deps --include=dev

# Copy source and build
COPY frontend/ ./
RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:1.27-alpine

# Required by entrypoint.sh for temporary self-signed certificate generation
RUN apk add --no-cache openssl

# Remove default config
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY docker/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built React app from Stage 1
COPY --from=build /app/dist /usr/share/nginx/html

# Create ACME challenge directory
RUN mkdir -p /var/www/certbot

# Copy entrypoint that templates the domain into nginx config
COPY docker/nginx/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy periodic nginx reload helper so renewed certs are picked up automatically
COPY scripts/nginx_auto_reload.sh /usr/local/bin/nginx_auto_reload.sh
RUN chmod +x /usr/local/bin/nginx_auto_reload.sh

EXPOSE 80 443

ENTRYPOINT ["/entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]
