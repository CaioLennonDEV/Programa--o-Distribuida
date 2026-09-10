import path from 'path';
import { randomUUID } from 'crypto';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { LamportClock } from './lamport';
import { RabbitMQPublisher } from './rabbitmq';

// ─── Tipos derivados do .proto ───────────────────────────────────────────────
export interface TelemetryPacket {
  sensor_id:     string;
  zone:          string;
  moisture:      number;
  temperature:   number;
  timestamp:     number;
  lamport_clock: number;
}

export interface TelemetryAck {
  queued:          boolean;
  message_id:      string;
  gateway_lamport: number;
  info:            string;
}

export interface WatchRequest {
  client_id: string;
}

export interface WorkerStatusEvent {
  node_id:      string;
  status:       string;
  lamport_time: number;
  event_ts:     number;
}

// ─── Bootstrap ───────────────────────────────────────────────────────────────
const PROTO_PATH = process.env.PROTO_PATH
  ?? path.resolve(__dirname, '../agro_telemetry.proto');
const GRPC_PORT  = process.env.GRPC_PORT ?? '50051';

async function main(): Promise<void> {
  const clock     = new LamportClock();
  const publisher = new RabbitMQPublisher();

  await publisher.connect();

  // Carrega o pacote proto em tempo de execução
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const protoDescriptor = grpc.loadPackageDefinition(packageDef);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const agrosensePkg = (protoDescriptor as any).agrosense;

  // ─── Implementação dos handlers gRPC ──────────────────────────────────────
  const handlers = {
    /**
     * SendTelemetry – recebe telemetria do sensor, sincroniza o relógio de Lamport,
     * enriquece o pacote com message_id e gateway_lamport, publica no RabbitMQ com
     * confirmação de broker e responde o ACK sem bloquear a linha de processamento.
     */
    SendTelemetry: async (
      call: grpc.ServerUnaryCall<TelemetryPacket, TelemetryAck>,
      callback: grpc.sendUnaryData<TelemetryAck>,
    ): Promise<void> => {
      try {
        const packet = call.request;

        // 1. Sincronização causal de Lamport: L = max(L_local, L_msg) + 1
        clock.receive(Number(packet.lamport_clock));
        const gatewayLamport = clock.tick();

        // 2. Criação do UUID determinístico ponta a ponta
        const messageId = randomUUID();

        const enriched = {
          message_id:      messageId,
          sensor_id:       packet.sensor_id,
          zone:            packet.zone,
          moisture:        Number(packet.moisture),
          temperature:     Number(packet.temperature),
          timestamp:       Number(packet.timestamp),
          lamport_clock:   Number(packet.lamport_clock),
          gateway_lamport: gatewayLamport,
          received_at:     Date.now(),
        };

        // 3. Publicação com Publisher Confirms
        const { success } = await publisher.publish(enriched);

        const ack: TelemetryAck = {
          queued:          success,
          message_id:      messageId,
          gateway_lamport: gatewayLamport,
          info:            success
            ? `Enfileirado em agro_telemetry_queue [L=${gatewayLamport}]`
            : 'Falha ao enfileirar no broker RabbitMQ',
        };

        callback(null, ack);
      } catch (err) {
        console.error('[Gateway] Erro no handler SendTelemetry:', err);
        callback({
          code: grpc.status.INTERNAL,
          message: err instanceof Error ? err.message : 'Erro interno no Gateway',
        }, null);
      }
    },

    /**
     * WatchWorkerStatus – server-side streaming
     */
    WatchWorkerStatus: (
      call: grpc.ServerWritableStream<WatchRequest, WorkerStatusEvent>,
    ): void => {
      console.log(`[Gateway] WatchWorkerStatus: cliente "${call.request?.client_id ?? 'desconhecido'}" conectado`);
      
      const interval = setInterval(() => {
        if (call.cancelled) {
          clearInterval(interval);
          return;
        }
        try {
          call.write({
            node_id:      'gateway',
            status:       'ACTIVE',
            lamport_time: clock.value,
            event_ts:     Date.now(),
          });
        } catch {
          clearInterval(interval);
        }
      }, 5000);

      call.on('cancelled', () => {
        clearInterval(interval);
      });
    },
  };

  // ─── Cria e inicia o servidor gRPC ────────────────────────────────────────
  const server = new grpc.Server();
  server.addService(agrosensePkg.AgroTelemetryService.service, handlers);

  server.bindAsync(
    `0.0.0.0:${GRPC_PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('[Gateway] Erro fatal ao iniciar gRPC:', err);
        process.exit(1);
      }
      console.log(`[Gateway] gRPC escutando na porta ${port} | Lamport inicial=${clock.value}`);
    },
  );
}

if (require.main === module) {
  main().catch(err => {
    console.error('[Gateway] Erro fatal:', err);
    process.exit(1);
  });
}


