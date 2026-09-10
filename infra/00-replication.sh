#!/bin/sh
set -e

echo "[PostgreSQL Primary] Configurando permissões de replicação em pg_hba.conf..."
echo "host replication all 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"
pg_ctl -D "$PGDATA" reload || true
echo "[PostgreSQL Primary] Permissão de replicação habilitada."
