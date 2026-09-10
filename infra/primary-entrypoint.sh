#!/bin/sh
# Primary entrypoint: sanitiza scripts (CRLF), aplica pg_hba com replicacao e sobe o Postgres.
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

# 1) Copia scripts de init montados do host removendo \r (Windows CRLF)
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

# 2) pg_hba.conf autoritativo com regra de replication (nao depende so do init)
mkdir -p /etc/postgresql
if [ -f /infra-scripts/pg_hba.conf ]; then
  tr -d '\r' < /infra-scripts/pg_hba.conf > /etc/postgresql/pg_hba.conf
else
  cat > /etc/postgresql/pg_hba.conf <<'EOF'
local   all             all                     trust
host    all             all     0.0.0.0/0       trust
host    all             all     ::/0            trust
host    replication     all     0.0.0.0/0       trust
host    replication     all     ::/0            trust
host    replication     all     all             trust
EOF
fi

# Tambem espelha no PGDATA quando o volume ja existe (init nao roda de novo)
if [ -f "$PGDATA/pg_hba.conf" ]; then
  cp /etc/postgresql/pg_hba.conf "$PGDATA/pg_hba.conf"
fi

echo "[PostgreSQL Primary] pg_hba.conf com replicacao aplicado."

exec docker-entrypoint.sh postgres \
  -c wal_level=replica \
  -c max_wal_senders=10 \
  -c max_replication_slots=10 \
  -c hot_standby=on \
  -c hba_file=/etc/postgresql/pg_hba.conf \
  -c listen_addresses='*'
