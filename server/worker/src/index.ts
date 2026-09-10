import amqp, { ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { LamportClock }       from './lamport';
import { BullyElection }      from './election';
import { ensureSchema, persistIrrigationLog, recordElectionLog } from './database';

// ─── Variáveis de Ambiente e Constantes ───────────────────────────────────────
const NODE_ID          = Number(process.env.NODE_ID ?? 1);
const ALL_NODES        = (process.env.ALL_NODES ?? '1,2,3').split(',').map(Number);
const RABBITMQ_URL     = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const TELEMETRY_QUEUE  = 'agro_telemetry_queue';
const PROCESSED_QUEUE  = 'agro_processed_results';
const DLX_EXCHANGE     = 'agro_telemetry_dlx';
const DLQ_QUEUE        = 'agro_telemetry_dlq';

/** Threshold de umidade para ativação de irrigação (%) */
export const MOISTURE_THRESHOLD = 40;

// ─── Interfaces de Dados ──────────────────────────────────────────────────────
export interface TelemetryEnriched {
  message_id:      string;
  sensor_id:       string;
  zone:            string;
  moisture:        number;
  temperature:     number;
  timestamp:       number;
  lamport_clock:   number;
  gateway_lamport: number;
  received_at:     number;
}

export interface ProcessedTelemetry {
  message_id:       string;
  sensor_id:        string;
  zone:             string;
  moisture:         number;
  temperature:      number;
  timestamp:        number;
  original_lamport: number;
  gateway_lamport:  number;
  worker_id:        number;
  worker_lamport:   number;
  processed_at:     number;
}

// ─── Estado Global do Nó ──────────────────────────────────────────────────────
const clock    = new LamportClock();
const election = new BullyElection(NODE_ID, ALL_NODES);

/** Buffer causal mantido pelo líder para ordenação determinística de eventos */
const consolidationBuffer: ProcessedTelemetry[] = [];

let amqpConnection:   ChannelModel | null = null;
let telemetryChannel: Channel      | null = null;
let publisherChannel: Channel      | null = null;
let leaderChannel:    Channel      | null = null;
let leaderConsumerTag: string      | null = null;

// ─── Ordenação Causal Determinística ──────────────────────────────────────────
export function sortCausalEvents(a: ProcessedTelemetry, b: ProcessedTelemetry): number {
  // 1. Critério primário: Relógio Lógico de Lamport do Worker que processou
  if (a.worker_lamport !== b.worker_lamport) {
    return a.worker_lamport - b.worker_lamport;
  }
  // 2. Desempate determinístico secundário: ID do Worker (menor ID primeiro)
  if (a.worker_id !== b.worker_id) {
    return a.worker_id - b.worker_id;
  }
  // 3. Desempate determinístico terciário: message_id UUID
  return a.message_id.localeCompare(b.message_id);
}

// ─── Processamento Local (Executado por TODOS os Workers 1, 2 e 3) ────────────
async function processTelemetryLocal(payload: TelemetryEnriched): Promise<ProcessedTelemetry> {
  // 1. Atualiza relógio de Lamport local: L = max(L_local, L_gw) + 1
  clock.receive(payload.gateway_lamport);
  const workerLamport = clock.tick();

  const processed: ProcessedTelemetry = {
    message_id:       payload.message_id,
    sensor_id:        payload.sensor_id,
    zone:             payload.zone,
    moisture:         payload.moisture,
    temperature:      payload.temperature,
    timestamp:        payload.timestamp,
    original_lamport: payload.lamport_clock,
    gateway_lamport:  payload.gateway_lamport,
    worker_id:        NODE_ID,
    worker_lamport:   workerLamport,
    processed_at:     Date.now(),
  };

  console.log(
    `[Worker-${NODE_ID}] ✓ Leitura processada: sensor=${processed.sensor_id} ` +
    `zone=${processed.zone} moisture=${processed.moisture}% | ` +
    `L_gw=${processed.gateway_lamport} → L_worker=${workerLamport} | msgId=${processed.message_id.slice(0, 8)}...`
  );

  return processed;
}

// ─── Publicação de Resultados Processados ─────────────────────────────────────
async function publishProcessedResult(result: ProcessedTelemetry): Promise<void> {
  if (!publisherChannel) {
    throw new Error(`[Worker-${NODE_ID}] Canal de publicação de resultados indisponível.`);
  }

  const buffer = Buffer.from(JSON.stringify(result));
  publisherChannel.sendToQueue(PROCESSED_QUEUE, buffer, {
    persistent:  true,
    messageId:   result.message_id,
    contentType: 'application/json',
    timestamp:   Math.floor(Date.now() / 1000),
  });
}

// ─── Consolidação pelo Líder (Executado EXCLUSIVAMENTE pelo Líder) ────────────
async function startLeaderConsolidation(): Promise<void> {
  if (!election.isLeader) return;
  if (leaderConsumerTag) return; // já está consumindo

  if (!amqpConnection) return;

  try {
    leaderChannel = await amqpConnection.createChannel();
    await leaderChannel.assertQueue(PROCESSED_QUEUE, { durable: true });
    await leaderChannel.prefetch(5);

    console.log(`[Leader/Worker-${NODE_ID}] ★ Iniciando consumo da fila de consolidação ${PROCESSED_QUEUE}...`);

    const consumeResult = await leaderChannel.consume(
      PROCESSED_QUEUE,
      async (msg: ConsumeMessage | null) => {
        if (!msg) return;

        // Se durante o consumo deixamos de ser líder, rejeitamos com requeue
        if (!election.isLeader) {
          leaderChannel?.nack(msg, false, true);
          return;
        }

        try {
          const item: ProcessedTelemetry = JSON.parse(msg.content.toString());

          // 1. Sincroniza relógio de Lamport do líder com o worker que processou
          clock.receive(item.worker_lamport);
          const leaderLamport = clock.tick();

          // 2. Insere no buffer causal e reordena deterministicamente
          consolidationBuffer.push(item);
          consolidationBuffer.sort(sortCausalEvents);
          if (consolidationBuffer.length > 500) consolidationBuffer.shift();

          // 3. Avalia regra de negócio de irrigação (moisture < 40%)
          if (item.moisture < MOISTURE_THRESHOLD) {
            console.log(
              `[Leader/Worker-${NODE_ID}] 💧 DECISÃO DE IRRIGAÇÃO: Zona ${item.zone} ` +
              `(umidade=${item.moisture}% < ${MOISTURE_THRESHOLD}%) | ` +
              `sensor=${item.sensor_id} L_worker=${item.worker_lamport} L_leader=${leaderLamport} msgId=${item.message_id.slice(0, 8)}...`
            );

            await persistIrrigationLog({
              message_id:   item.message_id,
              sensor_id:    item.sensor_id,
              zone:         item.zone,
              moisture:     item.moisture,
              temperature:  item.temperature,
              lamport_time: item.worker_lamport,
              event_ts:     item.timestamp,
              worker_id:    item.worker_id,
            });
          } else {
            console.log(
              `[Leader/Worker-${NODE_ID}] 🌿 Consolidação normal: Zona ${item.zone} umidade=${item.moisture}% (OK) L_worker=${item.worker_lamport}`
            );
          }

          leaderChannel?.ack(msg);
        } catch (err) {
          console.error(`[Leader/Worker-${NODE_ID}] Erro ao consolidar resultado:`, err);
          leaderChannel?.nack(msg, false, true);
        }
      },
      { noAck: false }
    );

    leaderConsumerTag = consumeResult.consumerTag;
  } catch (err) {
    console.error(`[Leader/Worker-${NODE_ID}] Erro ao iniciar consolidação:`, err);
  }
}

async function stopLeaderConsolidation(): Promise<void> {
  if (leaderConsumerTag && leaderChannel) {
    try {
      console.log(`[Worker-${NODE_ID}] Parando consumo da consolidação (não sou mais líder)...`);
      await leaderChannel.cancel(leaderConsumerTag);
      await leaderChannel.close();
    } catch {
      // canal já fechado
    } finally {
      leaderConsumerTag = null;
      leaderChannel     = null;
    }
  }
}

// ─── Consumer Principal (Competing Consumers) ─────────────────────────────────
async function startConsumer(): Promise<void> {
  const MAX_RETRIES = 15;
  const BASE_DELAY  = 2000;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(`[Worker-${NODE_ID}] Conectando ao RabbitMQ em ${RABBITMQ_URL} (tentativa ${attempt}/${MAX_RETRIES})...`);
      amqpConnection = await amqp.connect(RABBITMQ_URL);

      amqpConnection.on('error', (err) => {
        console.error(`[Worker-${NODE_ID}] Erro na conexão RabbitMQ:`, err.message);
      });

      amqpConnection.on('close', () => {
        console.warn(`[Worker-${NODE_ID}] Conexão RabbitMQ encerrada. Reconectando em 3s...`);
        amqpConnection   = null;
        telemetryChannel = null;
        publisherChannel = null;
        leaderChannel    = null;
        leaderConsumerTag = null;
        setTimeout(() => startConsumer().catch(e => console.error('[Worker] Erro na reconexão:', e)), 3000);
      });

      telemetryChannel = await amqpConnection.createChannel();
      publisherChannel = await amqpConnection.createChannel();

      // Configura Dead Letter Exchange e Queues
      await telemetryChannel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
      await telemetryChannel.assertQueue(DLQ_QUEUE, { durable: true });
      await telemetryChannel.bindQueue(DLQ_QUEUE, DLX_EXCHANGE, 'dead-letter');

      await telemetryChannel.assertQueue(TELEMETRY_QUEUE, {
        durable: true,
        deadLetterExchange: DLX_EXCHANGE,
        deadLetterRoutingKey: 'dead-letter',
      });

      await publisherChannel.assertQueue(PROCESSED_QUEUE, { durable: true });

      // Competing Consumers: prefetch(1) garante distribuição uniforme entre workers
      await telemetryChannel.prefetch(1);

      console.log(`[Worker-${NODE_ID}] Pronto para consumir ${TELEMETRY_QUEUE} (Competing Consumers ativo)...`);

      await telemetryChannel.consume(
        TELEMETRY_QUEUE,
        async (msg: ConsumeMessage | null) => {
          if (!msg) return;

          let payload: TelemetryEnriched;
          try {
            payload = JSON.parse(msg.content.toString());
            if (!payload || !payload.sensor_id || !payload.message_id) {
              throw new Error('Payload inválido ou incompleto');
            }
          } catch (parseErr) {
            console.error(`[Worker-${NODE_ID}] Mensagem malformada descartada para DLQ:`, parseErr);
            // Rejeita sem requeue → vai automaticamente para a Dead Letter Queue (DLQ)
            telemetryChannel?.nack(msg, false, false);
            return;
          }

          try {
            // 1. Processa localmente (todos os nós realizam esta etapa)
            const processedResult = await processTelemetryLocal(payload);

            // 2. Publica o resultado processado na fila de resultados
            await publishProcessedResult(processedResult);

            // 3. Confirma a mensagem original com ACK após publicação bem-sucedida
            telemetryChannel?.ack(msg);
          } catch (procErr) {
            console.error(`[Worker-${NODE_ID}] Erro transitório no processamento da mensagem:`, procErr);
            // Erro transitório → devolve para a fila (requeue=true)
            telemetryChannel?.nack(msg, false, true);
          }
        },
        { noAck: false }
      );

      // Se este nó já foi eleito líder antes da conexão RabbitMQ estar pronta
      if (election.isLeader) {
        await startLeaderConsolidation();
      }

      return;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const delay = Math.min(BASE_DELAY * attempt, 10000);
      console.warn(`[Worker-${NODE_ID}] Falha ao iniciar consumer (${msg}). Aguardando ${delay}ms...`);
      await new Promise(res => setTimeout(res, delay));
    }
  }

  throw new Error(`[Worker-${NODE_ID}] Falha crítica: não foi possível conectar ao RabbitMQ.`);
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  console.log(`\n══════════════════════════════════════════════════════`);
  console.log(` AgroSense Worker Node NODE_ID=${NODE_ID}`);
  console.log(` Cluster Nodes: [${ALL_NODES.join(', ')}]`);
  console.log(`══════════════════════════════════════════════════════\n`);

  // 1. Garante schema do banco de dados no Postgres Primary
  try {
    await ensureSchema();
  } catch (err) {
    console.warn(`[Worker-${NODE_ID}] Aviso – banco de dados ainda inicializando:`, err);
  }

  // 2. Inicia o Algoritmo do Valentão (Bully Election)
  await election.start();

  election.on('leader-changed', async (newLeaderId: number) => {
    console.log(`[Worker-${NODE_ID}] Transição de liderança → Novo líder: worker-${newLeaderId}`);

    if (newLeaderId === NODE_ID) {
      clock.tick();
      await recordElectionLog(NODE_ID, 'ELECTED', clock.value);
      await startLeaderConsolidation();
    } else {
      await stopLeaderConsolidation();
    }
  });

  // 3. Inicia o Consumer do RabbitMQ com tratamento adequado de erros
  startConsumer().catch((err) => {
    console.error(`[Worker-${NODE_ID}] Erro fatal no Consumer RabbitMQ:`, err);
    process.exit(1);
  });

  // 4. Reporta status periódico para o Web Dashboard
  const WEB_URL = process.env.WEB_URL ?? 'http://web:3000';
  setInterval(() => {
    fetch(`${WEB_URL}/api/worker-status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        node_id: `worker-${NODE_ID}`,
        status: election.isLeader ? 'LEADER' : 'ACTIVE',
        lamport: clock.value,
      }),
    }).catch(() => {
      // dashboard pode estar iniciando
    });
  }, 1500);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`[Worker-${NODE_ID}] Erro fatal no bootstrap:`, err);
    process.exit(1);
  });
}


