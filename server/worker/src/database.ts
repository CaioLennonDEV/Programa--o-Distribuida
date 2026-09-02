import { Pool, PoolClient } from 'pg';

const pool = new Pool({
  host:     process.env.DB_HOST     ?? 'postgres-primary',
  port:     Number(process.env.DB_PORT ?? 5432),
  database: process.env.DB_NAME     ?? 'agrosense',
  user:     process.env.DB_USER     ?? 'agro',
  password: process.env.DB_PASSWORD ?? 'agro123',
});

export interface IrrigationLog {
  sensor_id:    string;
  zone:         string;
  moisture:     number;
  temperature:  number;
  lamport_time: number;
  event_ts:     number;
  worker_id:    number;
}

/**
 * Garante que a tabela de logs existe no banco.
 */
export async function ensureSchema(): Promise<void> {
  const client: PoolClient = await pool.connect();
  try {
    await client.query(`
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
    `);
    console.log('[DB] Schema garantido.');
  } finally {
    client.release();
  }
}

/**
 * Persiste um registro de ativação de irrigação no banco primário.
 * Apenas o Worker Líder chama esta função.
 */
export async function persistIrrigationLog(log: IrrigationLog): Promise<void> {
  const client: PoolClient = await pool.connect();
  try {
    await client.query(
      `INSERT INTO irrigation_log
         (sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [log.sensor_id, log.zone, log.moisture, log.temperature,
       log.lamport_time, log.event_ts, log.worker_id],
    );
    console.log(`[DB] Log de irrigação persistido: sensor=${log.sensor_id} zone=${log.zone} L=${log.lamport_time}`);
  } catch (err) {
    console.error('[DB] Erro ao persistir log:', err);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Busca os últimos N logs (para o dashboard).
 */
export async function fetchRecentLogs(limit = 50): Promise<IrrigationLog[]> {
  const client: PoolClient = await pool.connect();
  try {
    const result = await client.query(
      `SELECT sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id
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
