import amqp, { ChannelModel, ConfirmChannel } from 'amqplib';

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
export const TELEMETRY_QUEUE = 'agro_telemetry_queue';
export const PROCESSED_QUEUE = 'agro_processed_results';
export const DLX_EXCHANGE    = 'agro_telemetry_dlx';
export const DLQ_QUEUE       = 'agro_telemetry_dlq';

export interface PublishResult {
  messageId: string;
  success:   boolean;
}

export class RabbitMQPublisher {
  private connection:     ChannelModel   | null = null;
  private confirmChannel: ConfirmChannel | null = null;
  private isConnecting:   boolean               = false;
  private isClosing:      boolean               = false;

  async connect(): Promise<void> {
    if (this.confirmChannel && this.connection) return;
    if (this.isConnecting) return;
    this.isConnecting = true;

    const MAX_RETRIES = 15;
    const BASE_DELAY  = 2000;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        console.log(`[RabbitMQ/Gateway] Conectando a ${RABBITMQ_URL} (tentativa ${attempt}/${MAX_RETRIES})...`);
        this.connection = await amqp.connect(RABBITMQ_URL);

        this.connection.on('error', (err) => {
          console.error('[RabbitMQ/Gateway] Erro de conexão:', err.message);
        });

        this.connection.on('close', () => {
          if (!this.isClosing) {
            console.warn('[RabbitMQ/Gateway] Conexão encerrada inesperadamente. Iniciando reconexão...');
            this.confirmChannel = null;
            this.connection = null;
            setTimeout(() => this.connect().catch(() => {}), 3000);
          }
        });

        this.confirmChannel = await this.connection.createConfirmChannel();

        // 1. Configura Dead Letter Exchange e Dead Letter Queue
        await this.confirmChannel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        await this.confirmChannel.assertQueue(DLQ_QUEUE, { durable: true });
        await this.confirmChannel.bindQueue(DLQ_QUEUE, DLX_EXCHANGE, 'dead-letter');

        // 2. Configura fila principal com redirecionamento para DLX em caso de rejeição
        await this.confirmChannel.assertQueue(TELEMETRY_QUEUE, {
          durable: true,
          deadLetterExchange: DLX_EXCHANGE,
          deadLetterRoutingKey: 'dead-letter',
        });

        // 3. Garante que a fila de resultados processados também existe
        await this.confirmChannel.assertQueue(PROCESSED_QUEUE, { durable: true });

        console.log(`[RabbitMQ/Gateway] Conectado e filas assertadas: ${TELEMETRY_QUEUE}, ${PROCESSED_QUEUE}, ${DLQ_QUEUE}`);
        this.isConnecting = false;
        return;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const delay = Math.min(BASE_DELAY * attempt, 10000);
        console.warn(`[RabbitMQ/Gateway] Falha na conexão (${msg}). Aguardando ${delay}ms...`);
        await new Promise(res => setTimeout(res, delay));
      }
    }

    this.isConnecting = false;
    throw new Error('[RabbitMQ/Gateway] Falha fatal: Não foi possível conectar ao RabbitMQ após múltiplas tentativas.');
  }

  /**
   * Publica mensagem no RabbitMQ utilizando ConfirmChannel.
   * A promise resolve somente após o ACK do broker RabbitMQ.
   */
  async publish(payload: Record<string, unknown> & { message_id: string }): Promise<PublishResult> {
    const messageId = payload.message_id;

    if (!this.confirmChannel) {
      console.warn('[RabbitMQ/Gateway] Canal não disponível ao publicar. Tentando reconectar...');
      try {
        await this.connect();
      } catch (e) {
        return { messageId, success: false };
      }
    }

    if (!this.confirmChannel) {
      return { messageId, success: false };
    }

    return new Promise<PublishResult>((resolve) => {
      try {
        const buffer = Buffer.from(JSON.stringify(payload));

        this.confirmChannel!.sendToQueue(
          TELEMETRY_QUEUE,
          buffer,
          {
            persistent:  true,
            messageId,
            contentType: 'application/json',
            timestamp:   Math.floor(Date.now() / 1000),
          },
          (err) => {
            if (err) {
              console.error(`[RabbitMQ/Gateway] NACK do broker para msgId=${messageId}:`, err);
              resolve({ messageId, success: false });
            } else {
              console.log(`[RabbitMQ/Gateway] Broker CONFIRM msgId=${messageId} sensor=${payload['sensor_id']} L_gw=${payload['gateway_lamport']}`);
              resolve({ messageId, success: true });
            }
          }
        );
      } catch (err) {
        console.error('[RabbitMQ/Gateway] Exceção ao publicar:', err);
        resolve({ messageId, success: false });
      }
    });
  }

  async close(): Promise<void> {
    this.isClosing = true;
    try {
      await this.confirmChannel?.close();
      await this.connection?.close();
    } catch {
      // fechamento limpo
    }
  }
}

