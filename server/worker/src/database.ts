import { Pool, PoolClient } from 'pg';

const pool = new Pool({
  host:     process.env.DB_HOST     ?? 'postgres-primary',
  port:     Number(process.env.DB_PORT ?? 5432),
  database: process.env.DB_NAME     ?? 'agrosense',
  user:     process.env.DB_USER     ?? 'agro',
  password: process.env.DB_PASSWORD ?? 'agro123',
});

export interface IrrigationLog {
  message_id:   string;
  sensor_id:    string;
  zone:         string;
  moisture:     number;
  temperature:  number;
  lamport_time: number;
  event_ts:     number;
  worker_id:    number;
}

/**
 * Garante que as tabelas necessárias existem no banco de dados.
 */
export async function ensureSchema(): Promise<void> {
  const client: PoolClient = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS irrigation_log (
        id           SERIAL PRIMARY KEY,
        message_id   UUID        NOT NULL,
        sensor_id    TEXT        NOT NULL,
        zone         TEXT        NOT NULL,
        moisture     REAL        NOT NULL,
        temperature  REAL        NOT NULL,
        lamport_time BIGINT      NOT NULL,
        event_ts     BIGINT      NOT NULL,
        worker_id    INTEGER     NOT NULL,
        activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_irrigation_message_id UNIQUE (message_id)
      );

      CREATE TABLE IF NOT EXISTS leader_election_log (
        id           SERIAL PRIMARY KEY,
        worker_id    INTEGER     NOT NULL,
        event        TEXT        NOT NULL,
        lamport_time BIGINT      NOT NULL,
        recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    console.log('[DB] Schema garantido.');
  } finally {
    client.release();
  }
}

/**
 * Persiste um registro de ativação de irrigação no banco primário.
 * Apenas o Worker Líder chama esta função durante a consolidação.
 * Utiliza ON CONFLICT para garantir idempotência caso o RabbitMQ reentregue uma mensagem.
 */
export async function persistIrrigationLog(log: IrrigationLog): Promise<boolean> {
  const client: PoolClient = await pool.connect();
  try {
    const res = await client.query(
      `INSERT INTO irrigation_log
         (message_id, sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (message_id) DO NOTHING`,
      [log.message_id, log.sensor_id, log.zone, log.moisture, log.temperature,
       log.lamport_time, log.event_ts, log.worker_id],
    );
    const inserted = (res.rowCount ?? 0) > 0;
    if (inserted) {
      console.log(`[DB] Log de irrigação persistido: msgId=${log.message_id} sensor=${log.sensor_id} zone=${log.zone} L=${log.lamport_time}`);
    } else {
      console.log(`[DB] Log duplicado ignorado (idempotência): msgId=${log.message_id}`);
    }
    return inserted;
  } catch (err) {
    console.error('[DB] Erro ao persistir log:', err);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Registra eventos de eleição de liderança para auditoria.
 */
export async function recordElectionLog(
  workerId: number,
  event: 'ELECTED' | 'RESIGNED' | 'FAILED',
  lamportTime: number,
): Promise<void> {
  let client: PoolClient | null = null;
  try {
    client = await pool.connect();
    await client.query(
      `INSERT INTO leader_election_log (worker_id, event, lamport_time)
       VALUES ($1, $2, $3)`,
      [workerId, event, lamportTime],
    );
    console.log(`[DB] Eleição registrada: worker=${workerId} event=${event} L=${lamportTime}`);
  } catch (err) {
    console.error('[DB] Erro ao registrar eleição:', err);
  } finally {
    client?.release();
  }
}

/**
 * Busca os últimos N logs (para verificação ou dashboard).
 */
export async function fetchRecentLogs(limit = 50): Promise<IrrigationLog[]> {
  const client: PoolClient = await pool.connect();
  try {
    const result = await client.query(
      `SELECT message_id, sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id
         FROM irrigation_log
        ORDER BY lamport_time DESC
        LIMIT $1`,
      [limit],
    );
    return result.rows as IrrigationLog[];
  } finally {
    client.release();
  }
}

