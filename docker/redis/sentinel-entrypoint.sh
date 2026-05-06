#!/bin/sh
set -e

# Wait for redis-master to be resolvable
echo "Waiting for redis-master to be resolvable..."
until getent hosts redis-master > /dev/null 2>&1; do
    echo "redis-master not yet resolvable, waiting..."
    sleep 2
done

# Get IP address of redis-master
MASTER_IP=$(getent hosts redis-master | awk '{ print $1 }')
echo "redis-master resolved to IP: $MASTER_IP"

# Create sentinel config with IP address
cat > /tmp/sentinel.conf <<EOF
port 26379

# Monitor master using IP address instead of hostname
sentinel monitor mymaster $MASTER_IP 6379 2

# Master is considered down after 5 seconds of no response
sentinel down-after-milliseconds mymaster 5000

# Only 1 replica can sync at a time during failover
sentinel parallel-syncs mymaster 1

# Failover timeout - 10 seconds
sentinel failover-timeout mymaster 10000

# Disable script reconfiguration for security
sentinel deny-scripts-reconfig yes

# Log level
loglevel notice
EOF

echo "Starting sentinel with config:"
cat /tmp/sentinel.conf

# Start Redis Sentinel
exec redis-sentinel /tmp/sentinel.conf

