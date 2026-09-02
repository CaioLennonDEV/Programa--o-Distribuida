-- AgroSense Mesh – Inicialização do Banco de Dados

-- Tabela principal de logs de irrigação (gerada pelo Worker Líder)
CREATE TABLE IF NOT EXISTS irrigation_log (
  id           SERIAL PRIMARY KEY,
  sensor_id    TEXT        NOT NULL,
  zone         TEXT        NOT NULL,
  moisture     REAL        NOT NULL,
  temperature  REAL        NOT NULL,
  lamport_time BIGINT      NOT NULL,
  event_ts     BIGINT      NOT NULL,
  worker_id    INTEGER     NOT NULL,
  activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices para performance de leitura no dashboard
CREATE INDEX IF NOT EXISTS idx_irr_lamport   ON irrigation_log (lamport_time DESC);
CREATE INDEX IF NOT EXISTS idx_irr_zone      ON irrigation_log (zone);
CREATE INDEX IF NOT EXISTS idx_irr_sensor    ON irrigation_log (sensor_id);
CREATE INDEX IF NOT EXISTS idx_irr_activated ON irrigation_log (activated_at DESC);

-- Tabela de auditoria de eleição de líder
CREATE TABLE IF NOT EXISTS leader_election_log (
  id          SERIAL PRIMARY KEY,
  worker_id   INTEGER     NOT NULL,
  event       TEXT        NOT NULL,  -- 'ELECTED' | 'RESIGNED' | 'FAILED'
  lamport_time BIGINT     NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- View para o dashboard: últimos 20 logs com temperatura e umidade
CREATE OR REPLACE VIEW recent_irrigation AS
  SELECT
    sensor_id,
    zone,
    ROUND(moisture::numeric, 1)    AS moisture_pct,
    ROUND(temperature::numeric, 1) AS temp_celsius,
    lamport_time,
    worker_id,
    activated_at
  FROM irrigation_log
  ORDER BY lamport_time DESC
  LIMIT 20;
