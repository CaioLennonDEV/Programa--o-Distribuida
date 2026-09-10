#!/bin/sh
# Garante permissão de replicação no Primary a cada start
# (volumes antigos podem ter sido criados sem a linha em pg_hba.conf).
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
HBA="$PGDATA/pg_hba.conf"

if [ -f "$HBA" ]; then
  if ! grep -qE '^[[:space:]]*host[[:space:]]+replication' "$HBA"; then
    echo "[PostgreSQL Primary] Adicionando regra de replicação em pg_hba.conf..."
    echo "host replication all 0.0.0.0/0 trust" >> "$HBA"
  fi
fi

exec docker-entrypoint.sh postgres \
  -c wal_level=replica \
  -c max_wal_senders=10 \
  -c max_replication_slots=10 \
  -c hot_standby=on
