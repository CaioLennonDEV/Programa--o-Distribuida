#!/bin/sh
set -e

echo "[PostgreSQL Primary] Configurando permissoes de replicacao em pg_hba.conf..."
cat >> "$PGDATA/pg_hba.conf" <<'EOF'
host replication all 0.0.0.0/0 trust
host replication all ::/0 trust
host replication all all trust
EOF
pg_ctl -D "$PGDATA" reload || true
echo "[PostgreSQL Primary] Permissao de replicacao habilitada."
