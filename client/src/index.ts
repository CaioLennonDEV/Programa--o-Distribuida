import path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// ─── Configuração ────────────────────────────────────────────────────────────
const PROTO_PATH = process.env.PROTO_PATH
  ?? path.resolve(__dirname, '../agro_telemetry.proto');
const GATEWAY_ADDR = process.env.GATEWAY_ADDR ?? 'localhost:50051';
const INTERVAL_MS  = Number(process.env.INTERVAL_MS ?? 2000);

// ─── Tipos ────────────────────────────────────────────────────────────────────
interface TelemetryPacket {
  sensor_id:     string;
  zone:          string;
  moisture:      number;
  temperature:   number;
  timestamp:     number;
  lamport_clock: number;
}

interface TelemetryAck {
  queued:          boolean;
  message_id:      string;
  gateway_lamport: number;
  info:            string;
}

interface AgroTelemetryClient extends grpc.Client {
  SendTelemetry(
    argument: TelemetryPacket,
    callback: (error: grpc.ServiceError | null, response: TelemetryAck) => void
  ): grpc.ClientUnaryCall;
}

// ─── Relógio de Lamport local do simulador ────────────────────────────────────
let localLamport = 0;

function tickLamport(): number { return ++localLamport; }
function receiveLamport(remote: number): void {
  localLamport = Math.max(localLamport, remote) + 1;
}

// ─── Sensores simulados ───────────────────────────────────────────────────────
const SENSORS: Array<{ id: string; zone: string }> = [
  { id: 'sensor-A-01', zone: 'A' },
  { id: 'sensor-B-01', zone: 'B' },
  { id: 'sensor-C-01', zone: 'C' },
  { id: 'sensor-D-01', zone: 'D' },
  { id: 'sensor-E-01', zone: 'E' },
];

// ─── Gerador de leitura aleatória ─────────────────────────────────────────────
function randomReading(sensor: { id: string; zone: string }): TelemetryPacket {
  return {
    sensor_id:     sensor.id,
    zone:          sensor.zone,
    moisture:      parseFloat((Math.random() * 100).toFixed(2)),
    temperature:   parseFloat((15 + Math.random() * 25).toFixed(2)),
    timestamp:     Date.now(),
    lamport_clock: tickLamport(),
  };
}

// ─── Função de envio tipada ──────────────────────────────────────────────────
function sendTelemetry(
  client: AgroTelemetryClient,
  packet: TelemetryPacket,
): Promise<TelemetryAck> {
  return new Promise((resolve, reject) => {
    client.SendTelemetry(packet, (err, response) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(response);
    });
  });
}

// ─── Bootstrap ───────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const protoDescriptor = grpc.loadPackageDefinition(packageDef);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const AgroService = (protoDescriptor as any).agrosense.AgroTelemetryService;

  const grpcClient = new AgroService(
    GATEWAY_ADDR,
    grpc.credentials.createInsecure(),
  ) as AgroTelemetryClient;

  console.log(`\n══════════════════════════════════════════════`);
  console.log(` AgroSense Sensor Simulator`);
  console.log(` Gateway: ${GATEWAY_ADDR}`);
  console.log(` Sensores: ${SENSORS.length} | Intervalo: ${INTERVAL_MS}ms`);
  console.log(`══════════════════════════════════════════════\n`);

  // Aguarda o gateway iniciar
  await new Promise(res => setTimeout(res, 3000));

  // Loop de envio concorrente: todos os 5 sensores enviam simultaneamente
  setInterval(async () => {
    const sends = SENSORS.map(async (sensor) => {
      const packet = randomReading(sensor);

      try {
        const ack = await sendTelemetry(grpcClient, packet);
        receiveLamport(Number(ack.gateway_lamport));

        console.log(
          `[Client] ✓ ${sensor.id} | zone=${sensor.zone} | ` +
          `moisture=${packet.moisture}% temp=${packet.temperature}°C | ` +
          `L_local=${packet.lamport_clock} → L_gw=${ack.gateway_lamport} | ` +
          `msgId=${ack.message_id.slice(0, 8)}...`
        );
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[Client] ✗ ${sensor.id} erro: ${msg}`);
      }
    });

    await Promise.allSettled(sends);
  }, INTERVAL_MS);
}

if (require.main === module) {
  main().catch(err => {
    console.error('[Client] Erro fatal:', err);
    process.exit(1);
  });
}


