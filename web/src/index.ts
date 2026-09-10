import Fastify from 'fastify';
import fastifyStatic  from '@fastify/static';
import fastifyWs      from '@fastify/websocket';
import path           from 'path';
import { Pool }       from 'pg';
import WebSocket      from 'ws';

const PORT  = Number(process.env.WEB_PORT ?? 3000);
const app   = Fastify({ logger: false });

// ─── Banco de dados (somente leitura – aponta para a réplica PostgreSQL) ──────
const pool = new Pool({
  host:     process.env.DB_HOST     ?? 'postgres-replica',
  port:     Number(process.env.DB_PORT ?? 5432),
  database: process.env.DB_NAME     ?? 'agrosense',
  user:     process.env.DB_USER     ?? 'agro',
  password: process.env.DB_PASSWORD ?? 'agro123',
});

// ─── Estado em memória ────────────────────────────────────────────────────────
interface WorkerStatus {
  node_id:    string;
  status:     string;
  lamport:    number;
  updated_at: number;
}

const workerStatuses = new Map<string, WorkerStatus>();
const wsClients      = new Set<WebSocket>();

// ─── Broadcast para todos os clientes WS conectados ──────────────────────────
function broadcast(event: string, data: unknown): void {
  const payload = JSON.stringify({ event, data, ts: Date.now() });
  for (const ws of wsClients) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(payload);
      } catch {
        // cliente desconectado
      }
    }
  }
}

// ─── Polling de logs de irrigação para o dashboard ───────────────────────────
async function pollIrrigationLogs(): Promise<void> {
  try {
    const result = await pool.query(
      `SELECT message_id, sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id, activated_at
         FROM irrigation_log
        ORDER BY lamport_time DESC
        LIMIT 20`
    );
    broadcast('irrigation-logs', result.rows);
  } catch {
    // Réplica pode estar em processo de sincronização inicial
  }
}

// ─── Bootstrap ───────────────────────────────────────────────────────────────
async function bootstrap(): Promise<void> {
  // ── Plugins Fastify ─────────────────────────────────────────────────────────
  await app.register(fastifyStatic, {
    root:   path.join(__dirname, '../src/public'),
    prefix: '/',
  });

  await app.register(fastifyWs);

  // ── Rota WebSocket principal ─────────────────────────────────────────────────
  app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection as unknown as WebSocket;
    wsClients.add(ws);

    console.log(`[Web] Cliente WS conectado. Total: ${wsClients.size}`);

    // Envia estado inicial dos workers
    ws.send(JSON.stringify({
      event: 'worker-status',
      data:  Array.from(workerStatuses.values()),
      ts:    Date.now(),
    }));

    ws.on('close', () => {
      wsClients.delete(ws);
      console.log(`[Web] Cliente WS desconectado. Total: ${wsClients.size}`);
    });

    ws.on('error', (err: Error) => console.error('[Web] WS erro:', err.message));
  });

  // ── REST API ─────────────────────────────────────────────────────────────────

  /** Atualiza status de um worker (chamado pelos próprios workers via HTTP) */
  app.post('/api/worker-status', async (req, reply) => {
    const body = req.body as WorkerStatus;
    if (!body?.node_id) {
      return reply.status(400).send({ error: 'node_id obrigatório' });
    }

    workerStatuses.set(body.node_id, { ...body, updated_at: Date.now() });
    broadcast('worker-status', Array.from(workerStatuses.values()));
    return reply.send({ ok: true });
  });

  /** Retorna logs de irrigação recentes (lendo da réplica) */
  app.get('/api/irrigation-logs', async (_req, reply) => {
    try {
      const result = await pool.query(
        `SELECT message_id, sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id, activated_at
           FROM irrigation_log
          ORDER BY lamport_time DESC
          LIMIT 50`
      );
      return reply.send(result.rows);
    } catch {
      return reply.status(503).send({ error: 'Réplica do banco temporariamente indisponível' });
    }
  });

  /** Health check */
  app.get('/health', async (_req, reply) => reply.send({ status: 'ok', ts: Date.now() }));

  // ── Polling periódico da réplica ─────────────────────────────────────────────
  setInterval(pollIrrigationLogs, 2500);

  setInterval(() => {
    const now = Date.now();
    let changed = false;
    for (const [id, status] of workerStatuses) {
      if (now - status.updated_at > 12000 && status.status !== 'DOWN') {
        workerStatuses.set(id, { ...status, status: 'DOWN', updated_at: now });
        changed = true;
      }
    }
    if (changed) broadcast('worker-status', Array.from(workerStatuses.values()));
  }, 4000);

  // ── Inicia servidor ──────────────────────────────────────────────────────────
  await app.listen({ port: PORT, host: '0.0.0.0' });
  console.log(`[Web] Dashboard AgroSense disponível em http://0.0.0.0:${PORT}`);
}

bootstrap().catch((err: Error) => {
  console.error('[Web] Erro fatal:', err.message);
  process.exit(1);
});

