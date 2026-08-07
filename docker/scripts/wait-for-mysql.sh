#!/bin/bash
# Wait for a MySQL instance to be ready
set -e

HOST="${1:-localhost}"
PORT="${2:-3306}"
USER="${3:-root}"
PASSWORD="${4:-rootpassword}"
TIMEOUT="${5:-60}"

echo "Waiting for MySQL at $HOST:$PORT (timeout: ${TIMEOUT}s)..."

for i in $(seq 1 $TIMEOUT); do
    if mysqladmin ping -h"$HOST" -P"$PORT" -u"$USER" -p"$PASSWORD" --silent 2>/dev/null; then
        echo "MySQL at $HOST:$PORT is ready!"
        exit 0
    fi
    sleep 1
done

echo "ERROR: MySQL at $HOST:$PORT did not become ready within ${TIMEOUT}s"
exit 1
