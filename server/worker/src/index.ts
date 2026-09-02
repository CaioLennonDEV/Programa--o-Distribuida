import amqp, { ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { LamportClock }       from './lamport';
import { BullyElection }      from './election';
import { ensureSchema, persistIrrigationLog } from './database';

// ─── Variáveis de ambiente ────────────────────────────────────────────────────
const NODE_ID      = Number(process.env.NODE_ID ?? 1);
const ALL_NODES    = (process.env.ALL_NODES ?? '1,2,3').split(',').map(Number);
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const QUEUE_NAME   = 'agro_telemetry_queue';

// ─── Tipos ────────────────────────────────────────────────────────────────────
interface TelemetryEnriched {
  sensor_id:       string;
  zone:            string;
  moisture:        number;
  temperature:     number;
  timestamp:       number;
  lamport_clock:   number;
  gateway_lamport: number;
  received_at:     number;
}

// ─── Estado Global ────────────────────────────────────────────────────────────
const clock    = new LamportClock();
const election = new BullyElection(NODE_ID, ALL_NODES);

/** Buffer de mensagens ordenadas por tempo de Lamport (validação causal) */
const causalBuffer: Array<{ lamport: number; payload: TelemetryEnriched }> = [];

/** Threshold de umidade para ativar irrigação (%) */
const MOISTURE_THRESHOLD = 40;

// ─── Processamento de Mensagem ────────────────────────────────────────────────

async function processTelemetry(payload: TelemetryEnriched): Promise<void> {
  // 1. Atualiza relógio de Lamport local
  clock.receive(payload.gateway_lamport);
  const localTime = clock.tick();

  console.log(
    `[Worker-${NODE_ID}] Processando sensor=${payload.sensor_id} ` +
    `moisture=${payload.moisture}% L_local=${localTime} isLeader=${election.isLeader}`
  );

  // 2. Insere no buffer causal ordenado por lamport
  causalBuffer.push({ lamport: localTime, payload });
  causalBuffer.sort((a, b) => a.lamport - b.lamport);

  // Mantém o buffer nos últimos 1000 eventos
  if (causalBuffer.length > 1000) causalBuffer.shift();

  // 3. Apenas o líder valida e persiste logs de irrigação
  if (!election.isLeader) return;

  // Verifica necessidade de irrigação
  if (payload.moisture < MOISTURE_THRESHOLD) {
    console.log(
      `[Worker-${NODE_ID}] ★ LÍDER – Ativando irrigação na zona ${payload.zone} ` +
      `(moisture=${payload.moisture}% < ${MOISTURE_THRESHOLD}%) L=${localTime}`
    );

    await persistIrrigationLog({
      sensor_id:    payload.sensor_id,
      zone:         payload.zone,
      moisture:     payload.moisture,
      temperature:  payload.temperature,
      lamport_time: localTime,
      event_ts:     payload.timestamp,
      worker_id:    NODE_ID,
    });
  }
}

// ─── Consumer RabbitMQ ────────────────────────────────────────────────────────

async function startConsumer(): Promise<void> {
  const MAX_RETRIES = 10;
  const DELAY_MS    = 3000;

  let connection: ChannelModel | null = null;
  let channel:    Channel      | null = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      connection = await amqp.connect(RABBITMQ_URL);
      channel    = await connection.createChannel();

      // Fila durável – deve existir antes de consumir
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      // Competing Consumers: cada worker processa 1 mensagem por vez
      await channel.prefetch(1);

      console.log(`[Worker-${NODE_ID}] Aguardando mensagens em ${QUEUE_NAME}...`);

      await channel.consume(QUEUE_NAME, async (msg: ConsumeMessage | null) => {
        if (!msg) return;

        try {
          const payload: TelemetryEnriched = JSON.parse(msg.content.toString());
          await processTelemetry(payload);

          // ACK explícito após processamento bem-sucedido
          channel!.ack(msg);
        } catch (err) {
          console.error(`[Worker-${NODE_ID}] Erro ao processar mensagem:`, err);
          // NACK com requeue=true para retentativa
          channel!.nack(msg, false, true);
        }
      }, { noAck: false });

      return; // conexão estabelecida com sucesso

    } catch (err) {
      console.warn(`[Worker-${NODE_ID}] Tentativa ${attempt}/${MAX_RETRIES} falhou. Aguardando ${DELAY_MS}ms...`);
      await new Promise(res => setTimeout(res, DELAY_MS));
    }
  }

  throw new Error(`[Worker-${NODE_ID}] Não foi possível conectar ao RabbitMQ.`);
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log(`\n══════════════════════════════════════`);
  console.log(` AgroSense Worker NODE_ID=${NODE_ID}`);
  console.log(`══════════════════════════════════════\n`);

  // 1. Garante schema do banco
  try {
    await ensureSchema();
  } catch (err) {
    console.warn(`[Worker-${NODE_ID}] Aviso – banco ainda não disponível:`, err);
  }

  // 2. Inicia eleição de líder
  await election.start();

  election.on('leader-changed', (leaderId: number) => {
    console.log(`[Worker-${NODE_ID}] Evento leader-changed → líder atual: ${leaderId}`);
  });

  // 3. Inicia consumer RabbitMQ
  startConsumer(); // Removido await para não bloquear o código abaixo

  // 4. Reporta status para o Dashboard periodicamente
  const WEB_URL = process.env.WEB_URL ?? 'http://web:3000';
  setInterval(() => {
    fetch(`${WEB_URL}/api/worker-status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        node_id: `worker-${NODE_ID}`,
        status: election.isLeader ? 'LEADER' : 'ACTIVE',
        lamport: clock.value
      })
    }).catch(() => {});
  }, 1500);
}

main().catch(err => {
  console.error(`[Worker-${NODE_ID}] Erro fatal:`, err);
  process.exit(1);
});
