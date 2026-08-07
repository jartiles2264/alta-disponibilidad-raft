#!/bin/bash
# Configure a MySQL node as replica of the primary
# Called by sidecar or manually for initial setup
set -e

PRIMARY_HOST="${1:?Usage: setup-replication.sh PRIMARY_HOST}"
MYSQL_HOST="${2:-localhost}"
MYSQL_PORT="${3:-3306}"
REPL_USER="${MYSQL_REPL_USER:-replica_user}"
REPL_PASSWORD="${MYSQL_REPL_PASSWORD:-replicapass123}"
ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-rootpassword}"

echo "Setting up replication: $MYSQL_HOST -> $PRIMARY_HOST"

mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -uroot -p"$ROOT_PASSWORD" <<EOF
STOP REPLICA;
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='$PRIMARY_HOST',
  SOURCE_PORT=$MYSQL_PORT,
  SOURCE_USER='$REPL_USER',
  SOURCE_PASSWORD='$REPL_PASSWORD',
  SOURCE_AUTO_POSITION=1,
  GET_SOURCE_PUBLIC_KEY=1;
START REPLICA;
EOF

echo "Replication configured successfully."
