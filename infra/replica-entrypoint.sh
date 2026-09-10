#!/bin/sh
# Entrypoint da Replica: espera o Primary, clona via pg_basebackup (com retry) e sobe em Standby.
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

clear_data_dir() {
  # Remove residuos de tentativas anteriores (inclui arquivos ocultos).
  if [ -d "$PGDATA" ]; then
    find "$PGDATA" -mindepth 1 -delete 2>/dev/null || rm -rf "${PGDATA:?}"/*
  fi
}

echo "[Replica] Aguardando postgres-primary ficar pronto..."
until pg_isready -h postgres-primary -p 5432 -U agro -d agrosense >/dev/null 2>&1; do
  sleep 2
done

# Pequena folga para WAL / pg_hba de replicacao estabilizarem apos o healthy.
sleep 5

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "[Replica] Diretorio vazio/incompleto. Clonando Primary via pg_basebackup..."
  clear_data_dir

  ATTEMPT=1
  MAX_ATTEMPTS=30
  until PGPASSWORD=agro123 pg_basebackup \
      -h postgres-primary \
      -p 5432 \
      -U agro \
      -D "$PGDATA" \
      -Fp -Xs -P -R; do
    echo "[Replica] pg_basebackup falhou (tentativa ${ATTEMPT}/${MAX_ATTEMPTS}). Nova tentativa em 3s..."
    clear_data_dir
    ATTEMPT=$((ATTEMPT + 1))
    if [ "$ATTEMPT" -gt "$MAX_ATTEMPTS" ]; then
      echo "[Replica] FALHA: nao foi possivel clonar o Primary apos ${MAX_ATTEMPTS} tentativas."
      exit 1
    fi
    sleep 3
  done

  chown -R postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  echo "[Replica] Clone concluido com sucesso."
else
  echo "[Replica] Dados existentes encontrados. Pulando pg_basebackup."
fi

echo "[Replica] Iniciando PostgreSQL Replica em modo Standby..."
exec su-exec postgres postgres -D "$PGDATA" -c hot_standby=on -c listen_addresses='*'
