import path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { LamportClock } from './lamport';
import { RabbitMQPublisher } from './rabbitmq';

// ─── Tipos derivados do .proto ───────────────────────────────────────────────
interface TelemetryPacket {
  sensor_id: string;
  zone: string;
  moisture: number;
  temperature: number;
  timestamp: number;
  lamport_clock: number;
}

interface TelemetryAck {
  queued: boolean;
  message_id: string;
  gateway_lamport: number;
  info: string;
}

// ─── Bootstrap ───────────────────────────────────────────────────────────────
// Em produção: __dirname = /app/dist → proto em /app/agro_telemetry.proto (../)
// Em dev (ts-node): __dirname = /app/src → proto em /app/agro_telemetry.proto (../)
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
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const proto = grpc.loadPackageDefinition(packageDef) as any;

  // ─── Implementação dos handlers gRPC ──────────────────────────────────────
  const handlers = {
    /**
     * SendTelemetry – recebe telemetria, atualiza relógio de Lamport,
     * publica na fila e devolve ACK imediato.
     */
    SendTelemetry: async (
      call: grpc.ServerUnaryCall<TelemetryPacket, TelemetryAck>,
      callback: grpc.sendUnaryData<TelemetryAck>,
    ): Promise<void> => {
      const packet = call.request;

      // Sincroniza relógio de Lamport: L = max(L_local, L_msg) + 1
      clock.receive(packet.lamport_clock);
      const gatewayLamport = clock.tick();

      const enriched = {
        ...packet,
        gateway_lamport: gatewayLamport,
        received_at: Date.now(),
      };

      const { messageId, success } = await publisher.publish(enriched);

      const ack: TelemetryAck = {
        queued:          success,
        message_id:      messageId,
        gateway_lamport: gatewayLamport,
        info:            success
          ? `Enfileirado em agro_telemetry_queue [L=${gatewayLamport}]`
          : 'Falha ao enfileirar – tente novamente',
      };

      callback(null, ack);
    },

    /**
     * WatchWorkerStatus – server-side streaming: emite um evento inicial
     * e mantém o stream aberto para que o dashboard web possa observar.
     * (Workers enviam eventos para o DB; o web backend os lê e emite via WS.)
     */
    WatchWorkerStatus: (
      call: grpc.ServerWritableStream<{ client_id: string }, unknown>,
    ): void => {
      console.log(`[Gateway] WatchWorkerStatus: cliente "${call.request.client_id}" conectado`);
      // Mantém o stream aberto — eventos são enviados por eventos externos
      // Exemplo de keep-alive simples (workers atualizam via DB/broadcast)
      const interval = setInterval(() => {
        if (call.cancelled) { clearInterval(interval); return; }
        try {
          call.write({ node_id: 'gateway', status: 'ACTIVE', lamport_time: clock.value, event_ts: Date.now() });
        } catch {
          clearInterval(interval);
        }
      }, 5000);

      call.on('cancelled', () => clearInterval(interval));
    },
  };

  // ─── Cria e inicia o servidor gRPC ────────────────────────────────────────
  const server = new grpc.Server();
  server.addService(proto.agrosense.AgroTelemetryService.service, handlers);

  server.bindAsync(
    `0.0.0.0:${GRPC_PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) { console.error('[Gateway] Erro ao iniciar:', err); process.exit(1); }
      console.log(`[Gateway] gRPC escutando na porta ${port} | Lamport=${clock.value}`);
    },
  );
}

main().catch(err => { console.error('[Gateway] Fatal:', err); process.exit(1); });
