#!/bin/sh
# Garante scripts de init sem CRLF e permissao de replicacao no Primary.
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
HBA="$PGDATA/pg_hba.conf"

# Copia scripts montados do host removendo \r (Windows CRLF)
mkdir -p /docker-entrypoint-initdb.d
if [ -d /infra-scripts ]; then
  for f in /infra-scripts/*; do
    [ -f "$f" ] || continue
    base=$(basename "$f")
    tr -d '\r' < "$f" > "/docker-entrypoint-initdb.d/$base"
    case "$base" in
      *.sh) chmod +x "/docker-entrypoint-initdb.d/$base" ;;
    esac
  done
fi

# Volumes antigos: garante regra de replicacao mesmo sem rerodar o init
if [ -f "$HBA" ]; then
  if ! grep -qE '^[[:space:]]*host[[:space:]]+replication' "$HBA"; then
    echo "[PostgreSQL Primary] Adicionando regra de replicacao em pg_hba.conf..."
    echo "host replication all 0.0.0.0/0 trust" >> "$HBA"
  fi
fi

exec docker-entrypoint.sh postgres \
  -c wal_level=replica \
  -c max_wal_senders=10 \
  -c max_replication_slots=10 \
  -c hot_standby=on
